import time
import structlog
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from api.routes import upload, jobs
from db.connection import create_pool, close_pool
from services.azure_client import init_azure_resources

# Configuración de Structlog
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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("startup", message="Iniciando aplicación")
    await create_pool()
    try:
        await init_azure_resources()
        logger.info("startup_resources", message="Recursos Azure inicializados")
    except Exception as e:
        logger.warning("startup_azure_error", error=str(e))
    
    yield
    
    # Shutdown
    await close_pool()
    logger.info("shutdown", message="Aplicación detenida")

app = FastAPI(
    title="LOGYCA Sales Pipeline",
    version="1.0.0",
    lifespan=lifespan
)

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# Rutas
app.include_router(upload.router, prefix="/api/v1", tags=["Upload"])
app.include_router(jobs.router, prefix="/api/v1", tags=["Jobs"])

@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "logyca-sales-api"}
