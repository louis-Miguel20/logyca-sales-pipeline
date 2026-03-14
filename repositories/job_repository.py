import structlog
from typing import Optional, List
from datetime import datetime
from db.connection import get_pool

logger = structlog.get_logger()

class JobRepository:
    async def create_job(self, file_name: str) -> str:
        """
        Crea un nuevo job en estado PENDING y retorna su UUID.
        """
        pool = await get_pool()
        async with pool.acquire() as conn:
            job_id = await conn.fetchval(
                """
                INSERT INTO jobs (file_name, status) 
                VALUES ($1, 'PENDING') 
                RETURNING id::text
                """,
                file_name
            )
            return job_id

    async def get_job(self, job_id: str) -> Optional[dict]:
        """
        Obtiene un job por ID. Retorna None si no existe.
        """
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id::text, file_name, status, created_at, updated_at, error_message 
                FROM jobs WHERE id = $1::uuid
                """,
                job_id
            )
            return dict(row) if row else None

    async def update_status(self, job_id: str, status: str, error_message: str = None) -> None:
        """
        Actualiza el estado de un job y opcionalmente su mensaje de error.
        """
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE jobs 
                SET status = $2, updated_at = NOW(), error_message = $3 
                WHERE id = $1::uuid
                """,
                job_id, status, error_message
            )
            await logger.info("job_status_updated", job_id=job_id, status=status)

    async def get_completed_jobs(self, since_minutes: int = 10) -> List[dict]:
        """
        Obtiene los jobs completados en los últimos N minutos.
        """
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id::text, file_name, updated_at 
                FROM jobs 
                WHERE status = 'COMPLETED' 
                AND updated_at > NOW() - $1::interval
                """,
                f'{since_minutes} minutes'
            )
            return [dict(row) for row in rows]
