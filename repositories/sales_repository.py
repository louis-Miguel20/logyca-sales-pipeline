import structlog
from typing import List, Tuple
from decimal import Decimal
from datetime import date
from db.connection import get_pool

logger = structlog.get_logger()

class SalesRepository:
    """
    Capa de acceso a datos para la entidad Ventas (Sales).
    Optimizado para operaciones masivas (Bulk Operations).
    """
    
    async def bulk_insert_sales(self, records: List[Tuple[date, int, int, Decimal, Decimal]]) -> int:
        """
        Realiza una inserción masiva de registros utilizando el protocolo COPY de PostgreSQL.
        
        Eficiencia:
        - INSERT tradicional: ~100-500 filas/segundo.
        - COPY (este método): ~10,000-50,000 filas/segundo.
        
        Args:
            records: Lista de tuplas con el orden exacto de columnas:
                     (date, product_id, quantity, price, total)
                     
        Returns:
            int: Número de filas insertadas.
        """
        if not records:
            return 0
            
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Usamos una transacción para asegurar atomicidad del lote
            async with conn.transaction():
                # copy_records_to_table es la función 'mágica' de asyncpg
                # que usa el protocolo binario COPY
                await conn.copy_records_to_table(
                    'sales',
                    records=records,
                    columns=['date', 'product_id', 'quantity', 'price', 'total']
                )
                
        logger.info("bulk_insert_completed", rows=len(records))
        return len(records)

    async def upsert_daily_summary(self, date_str: str, total_sales: float) -> None:
        """
        Actualiza (Update) o Inserta (Insert) el resumen de ventas de un día específico.
        Utiliza la cláusula ON CONFLICT de PostgreSQL (Upsert).
        
        Args:
            date_str: Fecha en formato 'YYYY-MM-DD'.
            total_sales: Monto total vendido ese día.
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
        Recupera el histórico de ventas diarias para reportes.
        
        Returns:
            List[dict]: Lista de diccionarios con {date, total_sales}.
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
