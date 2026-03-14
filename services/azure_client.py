from contextlib import asynccontextmanager
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.queue.aio import QueueClient
from azure.core.exceptions import ResourceExistsError
from config.settings import get_settings

settings = get_settings()

async def init_azure_resources():
    """
    Inicializa los recursos de Azure (Contenedor y Cola) de forma idempotente.
    """
    # Inicializar Blob Container
    blob_service = BlobServiceClient.from_connection_string(settings.azure_storage_connection_string)
    async with blob_service:
        container = blob_service.get_container_client(settings.azure_blob_container_name)
        try:
            await container.create_container()
        except ResourceExistsError:
            pass

    # Inicializar Queue
    queue = QueueClient.from_connection_string(
        conn_str=settings.azure_storage_connection_string,
        queue_name=settings.azure_queue_name
    )
    async with queue:
        try:
            await queue.create_queue()
        except ResourceExistsError:
            pass

@asynccontextmanager
async def get_blob_uploader(file_name: str):
    """
    Context manager para obtener un cliente de blob para subir archivos.
    """
    blob_service = BlobServiceClient.from_connection_string(settings.azure_storage_connection_string)
    async with blob_service:
        container = blob_service.get_container_client(settings.azure_blob_container_name)
        blob = container.get_blob_client(file_name)
        yield blob

@asynccontextmanager
async def get_queue_producer():
    """
    Context manager para obtener un cliente de cola para enviar mensajes.
    """
    queue = QueueClient.from_connection_string(
        conn_str=settings.azure_storage_connection_string,
        queue_name=settings.azure_queue_name
    )
    async with queue:
        yield queue
