import structlog
from typing import AsyncIterator
from azure.storage.blob.aio import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from tenacity import retry, stop_after_attempt, wait_exponential
from config.settings import get_settings

logger = structlog.get_logger()
settings = get_settings()

class BlobService:
    """
    Servicio wrapper para Azure Blob Storage.
    Maneja la subida y descarga de archivos con reintentos automáticos (Resiliencia).
    """
    def __init__(self):
        self.connection_string = settings.azure_storage_connection_string
        self.container_name = settings.azure_blob_container_name

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=16),
        reraise=True
    )
    async def upload_file(self, file_content: bytes, blob_name: str) -> str:
        """
        Sube un archivo binario a Azure Blob Storage.
        
        Incluye patrón 'Retry' (Reintento) usando la librería Tenacity:
        - Si falla por error de red, reintenta hasta 3 veces.
        - Espera exponencialmente entre reintentos (4s, 8s, 16s).
        
        Args:
            file_content (bytes): Contenido crudo del archivo.
            blob_name (str): Nombre único del archivo destino.
            
        Returns:
            str: Nombre del blob subido.
        """
        log = logger.bind(blob_name=blob_name, size=len(file_content))
        log.info("blob_upload_started")

        try:
            # Usamos el cliente asíncrono (aio) para no bloquear el event loop
            async with BlobServiceClient.from_connection_string(self.connection_string) as client:
                container_client = client.get_container_client(self.container_name)
                
                # Verificamos/Creamos el contenedor (Operación Idempotente)
                if not await container_client.exists():
                    await container_client.create_container()

                blob_client = container_client.get_blob_client(blob_name)
                # overwrite=True permite reemplazar archivos si colisionan (aunque usamos UUIDs)
                await blob_client.upload_blob(file_content, overwrite=True)
                
                log.info("blob_upload_completed")
                return blob_name
        except Exception as e:
            log.error("blob_upload_failed", error=str(e))
            raise

    async def download_file_stream(self, blob_name: str) -> AsyncIterator[bytes]:
        """
        Generador asíncrono que descarga un archivo por 'chunks' (pedazos).
        
        CRÍTICO PARA PERFORMANCE:
        Evita cargar archivos grandes (ej. 1GB) completos en memoria RAM.
        Permite procesar streams infinitos con memoria constante.
        
        Yields:
            bytes: Un fragmento del archivo.
        """
        log = logger.bind(blob_name=blob_name)
        try:
            async with BlobServiceClient.from_connection_string(self.connection_string) as client:
                container_client = client.get_container_client(self.container_name)
                blob_client = container_client.get_blob_client(blob_name)

                if not await blob_client.exists():
                    raise ResourceNotFoundError(f"Blob {blob_name} no encontrado")

                # download_blob retorna un StorageStreamDownloader
                stream = await blob_client.download_blob()
                # .chunks() nos da un iterador asíncrono sobre los datos
                async for chunk in stream.chunks():
                    yield chunk
        except ResourceNotFoundError:
            log.warning("blob_not_found")
            raise
        except Exception as e:
            log.error("blob_download_failed", error=str(e))
            raise
