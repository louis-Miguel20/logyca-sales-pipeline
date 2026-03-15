import structlog
import csv
from decimal import Decimal, InvalidOperation
from datetime import datetime
from typing import List, Tuple
from config.settings import get_settings
from services.blob_service import BlobService
from repositories.sales_repository import SalesRepository

logger = structlog.get_logger()
settings = get_settings()

# Headers requeridos para validación rápida
HEADERS_ESPERADOS = ['date', 'product_id', 'quantity', 'price']

class CsvProcessor:
    """
    Procesador de alto rendimiento para archivos CSV.
    
    Implementa el patrón 'Streaming Processing' para manejar archivos más grandes
    que la memoria RAM disponible.
    """
    def __init__(self):
        self.blob_service = BlobService()
        self.sales_repo = SalesRepository()

    async def process_csv_stream(self, blob_name: str, job_id: str) -> int:
        """
        Procesa un archivo CSV desde Blob Storage byte a byte (Streaming).
        
        Flujo de trabajo:
        1. Descarga chunks (trozos) del archivo desde Azure.
        2. Reconstruye líneas de texto a partir de los chunks.
        3. Parsea y valida cada línea.
        4. Acumula registros en un buffer (batch).
        5. Realiza inserción masiva (Bulk Insert) cuando el buffer se llena.
        
        Args:
            blob_name (str): Nombre del archivo en Azure Blob Storage.
            job_id (str): ID del trabajo para trazabilidad en logs.
            
        Returns:
            int: Número total de filas procesadas e insertadas.
        """
        # Contextualizamos el logger con el Job ID para rastrear todo el flujo
        log = logger.bind(job_id=job_id, blob_name=blob_name)
        log.info("csv_processing_started")

        total_processed = 0
        batch: List[Tuple] = []
        buffer = ""
        is_header = True
        
        try:
            # Iteramos sobre el stream de descarga de Azure (evita cargar todo en RAM)
            async for chunk in self.blob_service.download_file_stream(blob_name):
                # Decodificamos el chunk binario a texto, manejando caracteres rotos en bordes
                text_chunk = chunk.decode('utf-8', errors='replace')
                buffer += text_chunk
                
                # Procesamos todas las líneas completas que tengamos en el buffer
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    
                    if not line:
                        continue
                        
                    # Validación estricta de headers en la primera línea
                    if is_header:
                        headers = [h.strip().lower() for h in line.split(',')]
                        if headers != HEADERS_ESPERADOS:
                            raise ValueError(f"Headers inválidos: {headers}. Esperados: {HEADERS_ESPERADOS}")
                        is_header = False
                        continue
                    
                    # Parseamos la línea a tipos nativos de Python
                    row_data = self._parse_line(line, job_id)
                    if row_data:
                        batch.append(row_data)
                        
                    # Si el lote alcanza el tamaño configurado (ej. 1000), insertamos en BD
                    if len(batch) >= settings.batch_size:
                        await self._insert_batch(batch, job_id)
                        total_processed += len(batch)
                        batch = [] # Limpiamos el buffer

            # Procesar cualquier remanente en el buffer (última línea sin salto de línea final)
            if buffer.strip() and not is_header:
                row_data = self._parse_line(buffer.strip(), job_id)
                if row_data:
                    batch.append(row_data)

            # Insertar el último lote parcial que quedó pendiente
            if batch:
                await self._insert_batch(batch, job_id)
                total_processed += len(batch)

            log.info("csv_processing_completed", total_rows=total_processed)
            return total_processed

        except Exception as e:
            log.error("csv_processing_failed", error=str(e))
            raise

    def _parse_line(self, line: str, job_id: str) -> Tuple:
        """
        Helper puro para transformar una línea de texto en una tupla de datos tipados.
        Calcula el total (quantity * price) al vuelo.
        
        Maneja errores de formato individualmente para no detener todo el proceso
        por una sola línea corrupta (Fault Tolerance).
        """
        try:
            # Usamos csv.reader para manejar correctamente comillas, comas escapadas, etc.
            row = next(csv.reader([line]))
            if len(row) != 4:
                raise ValueError(f"Número incorrecto de columnas: {len(row)}")

            # Conversión de tipos segura
            date_val = datetime.strptime(row[0].strip(), '%Y-%m-%d').date()
            product_id = int(row[1].strip())
            quantity = int(row[2].strip())
            price = Decimal(row[3].strip())
            
            # Regla de negocio: Calcular total
            total = quantity * price
            
            return (date_val, product_id, quantity, price, total)
        except (ValueError, IndexError, InvalidOperation) as e:
            # Logueamos como warning pero retornamos None para saltar esta línea
            logger.warning("row_parse_error", line=line, error=str(e), job_id=job_id)
            return None

    async def _insert_batch(self, batch: List[Tuple], job_id: str):
        """
        Wrapper para la llamada al repositorio que maneja la inserción.
        Separa la lógica de acumulación (Buffer) de la lógica de persistencia (DB).
        """
        count = await self.sales_repo.bulk_insert_sales(batch)
        logger.info("batch_inserted", rows=count, job_id=job_id)
