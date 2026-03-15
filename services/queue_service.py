import base64
import json
import structlog
from datetime import datetime
from azure.storage.queue.aio import QueueServiceClient
from azure.core.exceptions import ResourceNotFoundError
from tenacity import retry, stop_after_attempt, wait_exponential
from config.settings import get_settings

logger = structlog.get_logger()
settings = get_settings()

class QueueService:
    def __init__(self):
        self.connection_string = settings.azure_storage_connection_string
        self.queue_name = settings.azure_queue_name

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=16),
        reraise=True
    )
    async def send_message(self, job_id: str, blob_name: str) -> None:
        """
        Envía un mensaje a la cola de Azure codificado en Base64.
        """
        log = logger.bind(job_id=job_id, blob_name=blob_name)
        
        payload = json.dumps({
            "job_id": job_id,
            "blob_name": blob_name,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        # Codificar en Base64 como requiere Azure Queue Storage por defecto
        encoded_message = base64.b64encode(payload.encode('utf-8')).decode('utf-8')

        try:
            async with QueueServiceClient.from_connection_string(self.connection_string) as client:
                queue_client = client.get_queue_client(self.queue_name)
                # Asegurar que la cola existe
                try:
                    await queue_client.create_queue()
                except Exception:
                    pass  # Ignorar si ya existe

                await queue_client.send_message(encoded_message)
                log.info("queue_message_sent")
        except Exception as e:
            log.error("queue_send_failed", error=str(e))
            raise

    async def receive_messages(self, max_messages: int = 5) -> list:
        """
        Recibe mensajes de la cola. Retorna objetos QueueMessage.
        """
        try:
            async with QueueServiceClient.from_connection_string(self.connection_string) as client:
                queue_client = client.get_queue_client(self.queue_name)
                # visibility_timeout=60 asegura que otros workers no tomen el mensaje por 60s
                messages = []
                async for msg in queue_client.receive_messages(max_messages=max_messages, visibility_timeout=60):
                    messages.append(msg)
                return messages
        except Exception as e:
            logger.error("queue_receive_failed", error=str(e))
            return []

    async def delete_message(self, message_id: str, pop_receipt: str) -> None:
        """
        Elimina un mensaje de la cola tras procesarlo exitosamente.
        """
        try:
            async with QueueServiceClient.from_connection_string(self.connection_string) as client:
                queue_client = client.get_queue_client(self.queue_name)
                await queue_client.delete_message(message_id, pop_receipt)
        except ResourceNotFoundError:
            logger.warning("queue_message_not_found_for_deletion", message_id=message_id)
        except Exception as e:
            logger.error("queue_delete_failed", message_id=message_id, error=str(e))
            raise

    def decode_message(self, content: str) -> dict:
        """
        Decodifica el mensaje Base64 -> JSON -> Dict.
        """
        try:
            decoded_bytes = base64.b64decode(content.encode('utf-8'))
            decoded_str = decoded_bytes.decode('utf-8')
            return json.loads(decoded_str)
        except Exception as e:
            logger.error("message_decode_failed", error=str(e))
            raise ValueError("Invalid message format")
