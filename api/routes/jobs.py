from typing import Optional
from uuid import UUID
import structlog
from fastapi import APIRouter, HTTPException, Path
from db.connection import get_pool
from api.schemas.job import JobResponse, JobStatus

router = APIRouter()
logger = structlog.get_logger()

@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job_status(
    job_id: UUID = Path(..., title="El ID del trabajo a consultar")
):
    """
    Consulta el estado actual de un trabajo de procesamiento (Polling).

    Permite al cliente saber si su archivo ya fue procesado, si está en progreso
    o si falló.

    Args:
        job_id (UUID): Identificador único del trabajo retornado por /upload.

    Returns:
        JobResponse: Objeto con el estado actual, fecha de creación y errores si los hubo.

    Raises:
        HTTPException 404: Si el Job ID no existe en la base de datos.
        HTTPException 500: Error de conexión a base de datos.
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Consultamos directamente a la BD. 
            # Usamos $1::uuid para asegurar que Postgres entienda el tipo de dato,
            # aunque asyncpg maneja la conversión de UUID de Python automáticamente.
            row = await conn.fetchrow(
                """
                SELECT id, file_name, status, created_at, error_message 
                FROM jobs 
                WHERE id = $1
                """,
                job_id
            )
            
            if not row:
                logger.warning("job_not_found", job_id=str(job_id))
                raise HTTPException(status_code=404, detail=f"Job {job_id} no encontrado")
            
            # Convertimos el string de la BD al Enum de Pydantic
            # Esto valida que el estado sea uno de los permitidos (PENDING, PROCESSING, etc.)
            status_str = row["status"]
            status_enum = JobStatus(status_str.upper())

            return JobResponse(
                job_id=str(row["id"]),
                status=status_enum,
                file_name=row["file_name"],
                created_at=row["created_at"],
                error_message=row["error_message"]
            )
            
    except HTTPException:
        # Re-lanzamos excepciones HTTP ya controladas
        raise
    except Exception as e:
        logger.error("error_fetching_job", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=500, detail="Error interno del servidor")
