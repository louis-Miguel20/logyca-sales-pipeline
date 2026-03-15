import structlog
from typing import List, Tuple
from decimal import Decimal
from datetime import date
from db.connection import get_pool

logger = structlog.get_logger()

class SalesRepository:
    async def bulk_insert_sales(self, records: List[Tuple[date, int, int, Decimal, Decimal]]) -> int:
        """
        Inserta masivamente registros de ventas usando PostgreSQL COPY.
        Es 10-50x más rápido que INSERT individual.
        
        Args:
            records: Lista de tuplas (date, product_id, quantity, price, total)
        """
        if not records:
            return 0
            
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # copy_records_to_table espera una lista de tuplas/listas, no diccionarios
                await conn.copy_records_to_table(
                    'sales',
                    records=records,
                    columns=['date', 'product_id', 'quantity', 'price', 'total']
                )
                
        logger.info("bulk_insert_completed", rows=len(records))
        return len(records)

    async def upsert_daily_summary(self, date_str: str, total_sales: float) -> None:
        """
        Actualiza o inserta el resumen diario de ventas.
        """
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO sales_daily_summary (date, total_sales, updated_at) 
                VALUES ($1::date, $2, NOW()) 
                ON CONFLICT (date) DO UPDATE SET 
                    total_sales = EXCLUDED.total_sales, 
                    updated_at = NOW()
                """,
                date_str, total_sales
            )

    async def get_daily_summary(self) -> List[dict]:
        """
        Obtiene el resumen diario de ventas ordenado por fecha descendente.
        """
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT date::text, total_sales 
                FROM sales_daily_summary 
                ORDER BY date DESC
                """
            )
            return [dict(row) for row in rows]
