import asyncio
import structlog
import signal
from config.settings import get_settings
from db.connection import create_pool, close_pool
from services.queue_service import QueueService
from repositories.job_repository import JobRepository
from worker.csv_processor import CsvProcessor

# Configurar logging igual que en la API
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()
settings = get_settings()

async def main():
    await logger.info("worker_starting")
    
    # Inicializar pool de base de datos
    await create_pool()
    
    queue_service = QueueService()
    job_repo = JobRepository()
    processor = CsvProcessor()
    
    # Manejo de señales para shutdown limpio
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    
    def signal_handler():
        logger.info("worker_shutdown_signal_received")
        stop_event.set()

    # Registrar señales (SIGINT/SIGTERM no funcionan igual en Windows, pero útil para Linux/Docker)
    # En Windows, KeyboardInterrupt se maneja en el bloque try/except principal
    if hasattr(signal, 'SIGINT'):
        loop.add_signal_handler(signal.SIGINT, signal_handler)
    if hasattr(signal, 'SIGTERM'):
        loop.add_signal_handler(signal.SIGTERM, signal_handler)

    await logger.info("worker_ready_polling_queue", queue_name=settings.azure_queue_name)

    while not stop_event.is_set():
        try:
            # Polling a la cola
            messages = await queue_service.receive_messages(max_messages=1)
            
            if not messages:
                await asyncio.sleep(settings.worker_poll_interval)
                continue
                
            for message in messages:
                if stop_event.is_set():
                    break
                    
                log = logger.bind(message_id=message.id)
                
                try:
                    payload = queue_service.decode_message(message.content)
                    job_id = payload.get('job_id')
                    blob_name = payload.get('blob_name')
                    
                    log = log.bind(job_id=job_id, blob_name=blob_name)
                    await log.info("processing_message")
                    
                    # 1. Marcar Job como PROCESSING
                    await job_repo.update_status(job_id, 'PROCESSING')
                    
                    # 2. Procesar CSV (Stream)
                    rows_processed = await processor.process_csv_stream(blob_name, job_id)
                    
                    # 3. Marcar Job como COMPLETED
                    await job_repo.update_status(job_id, 'COMPLETED')
                    
                    # 4. Eliminar mensaje de la cola (SOLO SI ÉXITO)
                    await queue_service.delete_message(message.id, message.pop_receipt)
                    
                    await log.info("job_completed_successfully", rows=rows_processed)
                    
                except Exception as e:
                    await log.error("job_processing_failed", error=str(e))
                    # Marcar Job como FAILED
                    if job_id:
                        await job_repo.update_status(job_id, 'FAILED', str(e))
                    
                    # IMPORTANTE: Eliminamos el mensaje aunque falle para evitar bucles infinitos
                    # (Dead Letter Queue sería mejor en producción real, pero por simplicidad aquí se elimina)
                    await queue_service.delete_message(message.id, message.pop_receipt)

        except asyncio.CancelledError:
            await logger.info("worker_task_cancelled")
            break
        except Exception as e:
            await logger.error("worker_loop_error", error=str(e))
            await asyncio.sleep(5)  # Esperar antes de reintentar en caso de error grave

    await close_pool()
    await logger.info("worker_shutdown_complete")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
