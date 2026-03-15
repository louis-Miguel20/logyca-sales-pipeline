import structlog
from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from fastapi.responses import JSONResponse
from api.schemas.job import UploadResponse, JobStatus
from services.upload_service import UploadService
from db.connection import get_pool

router = APIRouter()
logger = structlog.get_logger()

# Dependencia para inyectar el servicio
def get_upload_service():
    return UploadService()

@router.post(
    "/upload", 
    response_model=UploadResponse, 
    status_code=status.HTTP_202_ACCEPTED
)
async def upload_csv(
    file: UploadFile = File(...),
    upload_service: UploadService = Depends(get_upload_service)
):
    """
    Recibe un archivo CSV, lo sube a Azure Blob Storage y encola una tarea de procesamiento.
    """
    if not file.filename.lower().endswith('.csv'):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="El archivo debe ser un CSV (.csv)"
        )
    
    try:
        job_id = await upload_service.process_upload(file)
        
        return JSONResponse(
            content=UploadResponse(
                job_id=str(job_id),
                message="Archivo recibido y en proceso",
                status="accepted"
            ).model_dump(),
            status_code=status.HTTP_202_ACCEPTED
        )
        
    except Exception as e:
        logger.error("upload_failed", filename=file.filename, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error procesando la carga: {str(e)}"
        )
