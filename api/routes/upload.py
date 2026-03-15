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
    """
    Dependency Injection provider for UploadService.
    Ensures a fresh instance is created for the request context.
    """
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
    Endpoint principal para la carga de archivos CSV.
    
    Este endpoint implementa el patrón "Fire-and-Forget" (Disparar y Olvidar) para evitar bloqueos:
    1. Recibe el archivo.
    2. Lo sube a Azure Blob Storage.
    3. Encola un mensaje en Azure Queue Storage.
    4. Retorna inmediatamente un Job ID al cliente.
    
    Args:
        file (UploadFile): El archivo CSV a procesar. Debe tener extensión .csv.
        upload_service (UploadService): Servicio de lógica de negocio inyectado.

    Returns:
        JSONResponse: Objeto JSON con el job_id y estado 'accepted'.

    Raises:
        HTTPException 400: Si el archivo no es un CSV.
        HTTPException 500: Si ocurre un error interno en la subida o encolado.
    """
    # Validación temprana de la extensión del archivo para ahorrar recursos
    if not file.filename.lower().endswith('.csv'):
        logger.warning("invalid_file_extension", filename=file.filename)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, 
            detail="El archivo debe ser un CSV (.csv)"
        )
    
    try:
        # Delegamos la lógica compleja al servicio para mantener el controlador limpio
        job_id = await upload_service.process_upload(file)
        
        # Retornamos 202 Accepted indicando que el proceso ha iniciado pero no terminado
        return JSONResponse(
            content=UploadResponse(
                job_id=str(job_id),
                message="Archivo recibido y en proceso",
                status="accepted"
            ).model_dump(),
            status_code=status.HTTP_202_ACCEPTED
        )
        
    except Exception as e:
        # Logueamos el error con contexto estructurado para facilitar la depuración
        logger.error("upload_endpoint_failed", filename=file.filename, error=str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error procesando la carga: {str(e)}"
        )
