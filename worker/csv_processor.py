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

HEADERS_ESPERADOS = ['date', 'product_id', 'quantity', 'price']

class CsvProcessor:
    def __init__(self):
        self.blob_service = BlobService()
        self.sales_repo = SalesRepository()

    async def process_csv_stream(self, blob_name: str, job_id: str) -> int:
        """
        Procesa un archivo CSV desde Blob Storage en modo streaming.
        Lee chunks, parsea líneas y hace bulk insert en lotes.
        """
        log = logger.bind(job_id=job_id, blob_name=blob_name)
        log.info("csv_processing_started")

        total_processed = 0
        batch: List[Tuple] = []
        buffer = ""
        is_header = True
        
        try:
            async for chunk in self.blob_service.download_file_stream(blob_name):
                # Decodificar chunk y agregarlo al buffer
                text_chunk = chunk.decode('utf-8', errors='replace')
                buffer += text_chunk
                
                # Procesar líneas completas en el buffer
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    line = line.strip()
                    
                    if not line:
                        continue
                        
                    # Validar headers en la primera línea
                    if is_header:
                        headers = [h.strip().lower() for h in line.split(',')]
                        if headers != HEADERS_ESPERADOS:
                            raise ValueError(f"Headers inválidos: {headers}. Esperados: {HEADERS_ESPERADOS}")
                        is_header = False
                        continue
                    
                    # Parsear línea de datos
                    row_data = self._parse_line(line, job_id)
                    if row_data:
                        batch.append(row_data)
                        
                    # Insertar lote si alcanzamos el tamaño configurado
                    if len(batch) >= settings.batch_size:
                        await self._insert_batch(batch, job_id)
                        total_processed += len(batch)
                        batch = []

            # Procesar cualquier remanente en el buffer (última línea sin \n)
            if buffer.strip() and not is_header:
                row_data = self._parse_line(buffer.strip(), job_id)
                if row_data:
                    batch.append(row_data)

            # Insertar el último lote parcial
            if batch:
                await self._insert_batch(batch, job_id)
                total_processed += len(batch)

            log.info("csv_processing_completed", total_rows=total_processed)
            return total_processed

        except Exception as e:
            log.error("csv_processing_failed", error=str(e))
            raise

    def _parse_line(self, line: str, job_id: str) -> Tuple:
        """Helper para parsear una línea CSV a los tipos correctos."""
        try:
            # Usamos csv.reader para manejar correctamente comillas, escapes, etc.
            row = next(csv.reader([line]))
            if len(row) != 4:
                raise ValueError(f"Número incorrecto de columnas: {len(row)}")

            date_val = datetime.strptime(row[0].strip(), '%Y-%m-%d').date()
            product_id = int(row[1].strip())
            quantity = int(row[2].strip())
            price = Decimal(row[3].strip())
            total = quantity * price
            
            return (date_val, product_id, quantity, price, total)
        except (ValueError, IndexError, InvalidOperation) as e:
            logger.warning("row_parse_error", line=line, error=str(e), job_id=job_id)
            return None

    async def _insert_batch(self, batch: List[Tuple], job_id: str):
        """Helper para insertar un lote y actualizar métricas."""
        count = await self.sales_repo.bulk_insert_sales(batch)
        logger.info("batch_inserted", rows=count, job_id=job_id)
