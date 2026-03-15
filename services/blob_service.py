import structlog
from typing import AsyncIterator
from azure.storage.blob.aio import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError
from tenacity import retry, stop_after_attempt, wait_exponential
from config.settings import get_settings

logger = structlog.get_logger()
settings = get_settings()

class BlobService:
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
        Sube un archivo a Azure Blob Storage con reintentos automáticos.
        """
        log = logger.bind(blob_name=blob_name, size=len(file_content))
        log.info("blob_upload_started")

        try:
            async with BlobServiceClient.from_connection_string(self.connection_string) as client:
                container_client = client.get_container_client(self.container_name)
                # Asegurar que el contenedor existe (idempotente)
                if not await container_client.exists():
                    await container_client.create_container()

                blob_client = container_client.get_blob_client(blob_name)
                await blob_client.upload_blob(file_content, overwrite=True)
                
                log.info("blob_upload_completed")
                return blob_name
        except Exception as e:
            log.error("blob_upload_failed", error=str(e))
            raise

    async def download_file_stream(self, blob_name: str) -> AsyncIterator[bytes]:
        """
        Descarga un archivo como stream de bytes para no cargar todo en memoria.
        """
        log = logger.bind(blob_name=blob_name)
        try:
            async with BlobServiceClient.from_connection_string(self.connection_string) as client:
                container_client = client.get_container_client(self.container_name)
                blob_client = container_client.get_blob_client(blob_name)

                if not await blob_client.exists():
                    raise ResourceNotFoundError(f"Blob {blob_name} no encontrado")

                stream = await blob_client.download_blob()
                async for chunk in stream.chunks():
                    yield chunk
        except ResourceNotFoundError:
            log.warning("blob_not_found")
            raise
        except Exception as e:
            log.error("blob_download_failed", error=str(e))
            raise
