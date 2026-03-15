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
    Obtiene el estado actual de un trabajo de procesamiento.
    """
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Importante: usar $1::uuid para casting explícito si es necesario, 
            # aunque asyncpg suele manejar UUIDs nativos de python bien.
            row = await conn.fetchrow(
                "SELECT id, file_name, status, created_at, error_message FROM jobs WHERE id = $1",
                job_id
            )
            
            if not row:
                # Si no encuentra por UUID, intentar buscar como texto (por si acaso)
                # O simplemente lanzar 404
                raise HTTPException(status_code=404, detail=f"Job {job_id} no encontrado")
            
            # Mapear status string a Enum
            status_str = row["status"]
            # Asegurar compatibilidad con mayúsculas/minúsculas
            status_enum = JobStatus(status_str.upper())

            return JobResponse(
                job_id=str(row["id"]),
                status=status_enum,
                file_name=row["file_name"],
                created_at=row["created_at"],
                error_message=row["error_message"]
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("error_fetching_job", job_id=str(job_id), error=str(e))
        raise HTTPException(status_code=500, detail="Error interno del servidor")
