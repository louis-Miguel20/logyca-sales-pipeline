import structlog
from fastapi import APIRouter, UploadFile, File, HTTPException, status
from fastapi.responses import JSONResponse
from api.schemas.job import UploadResponse, JobStatus
from services.azure_client import get_blob_uploader, get_queue_producer
from db.connection import get_pool

router = APIRouter()
logger = structlog.get_logger()

@router.post(
    "/upload", 
    response_model=UploadResponse, 
    status_code=status.HTTP_202_ACCEPTED
)
async def upload_csv(file: UploadFile = File(...)):
    """
    Recibe un archivo CSV, lo sube a Azure Blob Storage y encola una tarea de procesamiento.
    """
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="El archivo debe ser un CSV (.csv)"
        )
    
    try:
        # 1. Crear registro en DB con estado PENDING
        pool = await get_pool()
        async with pool.acquire() as conn:
            job_id = await conn.fetchval(
                """
                INSERT INTO jobs (file_name, status) 
                VALUES ($1, $2) 
                RETURNING id
                """,
                file.filename,
                JobStatus.PENDING.value
            )
        
        # 2. Subir archivo a Azure Blob Storage
        async with get_blob_uploader(file.filename) as blob_client:
            # Leer el archivo en chunks o completo (para simplificar, leemos completo aquí, 
            # pero en producción idealmente sería stream)
            content = await file.read()
            await blob_client.upload_blob(content, overwrite=True)
            
        # 3. Enviar mensaje a la cola para procesamiento
        async with get_queue_producer() as queue_client:
            # Enviamos el job_id y filename como mensaje
            import json
            message = json.dumps({"job_id": str(job_id), "file_name": file.filename})
            await queue_client.send_message(message)
            
        await logger.info("upload_success", job_id=str(job_id), filename=file.filename)
        
        return JSONResponse(
            content=UploadResponse(
                job_id=str(job_id),
                message="Archivo recibido y en proceso",
                status="accepted"
            ).model_dump(),
            status_code=status.HTTP_202_ACCEPTED
        )
        
    except Exception as e:
        await logger.error("upload_failed", filename=file.filename, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error procesando la carga: {str(e)}"
        )
