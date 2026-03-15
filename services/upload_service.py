import structlog
from uuid import uuid4
from fastapi import UploadFile
from services.blob_service import BlobService
from services.queue_service import QueueService
from repositories.job_repository import JobRepository

logger = structlog.get_logger()

class UploadService:
    def __init__(self):
        self.blob_service = BlobService()
        self.queue_service = QueueService()
        self.job_repo = JobRepository()

    async def process_upload(self, file: UploadFile) -> str:
        """
        Maneja el flujo completo de subida:
        1. Crea Job en DB
        2. Sube archivo a Blob Storage
        3. Encola mensaje para procesamiento
        """
        # Generar nombre único para el blob
        blob_name = f"{uuid4()}_{file.filename}"
        
        # Leer contenido (en memoria para subida inicial)
        content = await file.read()
        file_size = len(content)
        
        log = logger.bind(filename=file.filename, blob_name=blob_name, size=file_size)
        log.info("upload_started")

        try:
            # 1. Crear Job (PENDING)
            job_id = await self.job_repo.create_job(file.filename)
            
            # 2. Subir a Blob Storage
            await self.blob_service.upload_file(content, blob_name)
            
            # 3. Enviar a Queue
            await self.queue_service.send_message(job_id, blob_name)
            
            log.info("upload_completed", job_id=job_id)
            return job_id
            
        except Exception as e:
            log.error("upload_failed", error=str(e))
            raise
