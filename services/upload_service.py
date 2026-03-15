import structlog
from uuid import uuid4
from fastapi import UploadFile
from services.blob_service import BlobService
from services.queue_service import QueueService
from repositories.job_repository import JobRepository

logger = structlog.get_logger()

class UploadService:
    """
    Orquestador del proceso de carga (Facade Pattern).
    Coordina la interacción entre DB, Blob Storage y Queue Storage.
    """
    def __init__(self):
        self.blob_service = BlobService()
        self.queue_service = QueueService()
        self.job_repo = JobRepository()

    async def process_upload(self, file: UploadFile) -> str:
        """
        Ejecuta el flujo transaccional de recepción de archivo.
        
        Pasos:
        1. Persistencia de estado: Crea el Job en base de datos.
        2. Almacenamiento: Sube el físico a Blob Storage.
        3. Mensajería: Notifica al Worker vía Queue Storage.
        
        Args:
            file (UploadFile): Archivo recibido de FastAPI.
            
        Returns:
            str: Job ID generado.
        """
        # Generar nombre único (UUID) para evitar colisiones de nombres de archivo
        blob_name = f"{uuid4()}_{file.filename}"
        
        # Leemos el contenido en memoria. 
        # NOTA: Para archivos MUY grandes (>100MB) en producción, sería mejor
        # hacer streaming directo del request al blob, pero FastAPI carga en SpooledTemporaryFile
        # por lo que leerlo aquí es seguro para tamaños moderados.
        content = await file.read()
        file_size = len(content)
        
        log = logger.bind(filename=file.filename, blob_name=blob_name, size=file_size)
        log.info("upload_started")

        try:
            # 1. Crear Job (Estado inicial: PENDING)
            job_id = await self.job_repo.create_job(file.filename)
            
            # 2. Subir a Blob Storage
            await self.blob_service.upload_file(content, blob_name)
            
            # 3. Enviar a Queue (El trigger para el Worker)
            await self.queue_service.send_message(job_id, blob_name)
            
            log.info("upload_completed", job_id=job_id)
            return job_id
            
        except Exception as e:
            # Si algo falla, el Job quedará en PENDING (o no se creará)
            # Idealmente aquí podríamos marcar el Job como FAILED si ya se creó.
            log.error("upload_failed", error=str(e))
            raise
