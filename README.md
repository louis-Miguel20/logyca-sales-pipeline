# LOGYCA Sales Pipeline

Sistema distribuido de alto rendimiento para la ingesta, procesamiento y análisis de ventas masivas mediante archivos CSV, diseñado para escalar horizontalmente y manejar millones de registros sin saturar recursos.

## Arquitectura

El sistema sigue una arquitectura orientada a eventos con desacoplamiento total entre la recepción del archivo y su procesamiento.

```ascii
[ Cliente ] 
     |
     | (1) POST /upload (CSV)
     v
[ API FastAPI ] 
     |-----------------------> (2) Sube CSV a Azure Blob Storage
     |
     |-----------------------> (3) Crea registro en DB (Estado: PENDING)
     |
     |-----------------------> (4) Envía mensaje a Azure Queue
     |
     v
( 202 Accepted )

[ Worker ] <---- (5) Lee mensaje de Azure Queue
     |
     |-----> (6) Descarga CSV en Streaming (Chunks) desde Blob
     |
     |-----> (7) Procesa y valida filas en memoria
     |
     v
[ PostgreSQL ] <---- (8) Bulk Insert (COPY) en lotes de 1000 filas
```

## Stack Tecnológico

| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| **Python** | 3.11+ | Lenguaje base optimizado para I/O asíncrono |
| **FastAPI** | 0.115.0 | Framework web asíncrono de alto rendimiento |
| **PostgreSQL** | 16 | Base de datos relacional robusta |
| **AsyncPG** | 0.30.0 | Driver de base de datos más rápido para Python |
| **Azure Storage** | v12 | Almacenamiento distribuido (Blobs y Colas) |
| **Docker** | Latest | Containerización y orquestación |
| **Structlog** | 24.4.0 | Logging estructurado (JSON) para observabilidad |
| **Tenacity** | 9.0.0 | Resiliencia y reintentos automáticos |

## Decisiones Técnicas

### ¿Por qué colas (Azure Queue)?
Desacoplan la recepción del procesamiento. Si llegan 100 archivos de 1GB simultáneamente, la API responde en milisegundos y los workers procesan a su propio ritmo sin colapsar el servidor ni la base de datos (Backpressure natural).

### ¿Cómo se procesan archivos de millones de filas?
Utilizamos **Streaming**. El archivo nunca se carga completo en la memoria RAM.
1. Se descarga del Blob Storage en chunks de bytes.
2. Se reconstruyen líneas de texto en un buffer pequeño.
3. Se parsean y validan filas una a una.
Esto permite procesar un archivo de 50GB con solo 512MB de RAM asignada al contenedor.

### ¿Cómo se evita saturar PostgreSQL?
El cuello de botella suele ser la base de datos.
1. **Bulk Inserts (COPY)**: Usamos `copy_records_to_table` de `asyncpg` en lugar de `INSERT` tradicionales. Es entre **10x y 50x más rápido**.
2. **Batching**: Las filas se agrupan en lotes (default: 1000) antes de enviarse a la DB, reduciendo el overhead de red.
3. **Connection Pooling**: Limitamos el pool a 10 conexiones por worker para no ahogar a Postgres.

### ¿Por qué structlog?
En sistemas distribuidos, los logs de texto plano son inútiles. `structlog` genera logs en JSON que incluyen contexto (job_id, file_name, latency) automáticamente, facilitando la ingestión en sistemas como Datadog, ELK o Azure Monitor.

### ¿Por qué tenacity?
Las redes fallan. Azure puede dar timeouts. `tenacity` implementa decoradores `@retry` con **backoff exponencial** (espera 2s, 4s, 8s...) para recuperar fallos transitorios sin intervención humana.

## Requisitos Previos

*   Docker y Docker Compose instalados.
*   Python 3.11+ (opcional, solo para desarrollo local).
*   Cuenta de Azure Storage (o usar el emulador Azurite incluido si se configura).

Verificar instalación:
```bash
docker --version
docker-compose --version
```

## Configuración Azure

El proyecto usa `pydantic-settings` para validar la configuración al inicio. Crea un archivo `.env` basado en `.env.example`.

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `DATABASE_URL` | Conexión a Postgres | `postgresql://user:pass@host:5432/db` |
| `AZURE_STORAGE_CONNECTION_STRING` | Cadena de conexión Azure | `DefaultEndpointsProtocol=https;...` |
| `AZURE_BLOB_CONTAINER_NAME` | Nombre del contenedor | `csv-uploads` |
| `AZURE_QUEUE_NAME` | Nombre de la cola | `csv-processing-queue` |

## Ejecución con Docker Compose

Levanta todo el entorno (API, Worker, Postgres, N8N):

```bash
docker-compose up --build -d
```

Verificar estado de los servicios:
```bash
docker-compose ps
```

Ver logs del worker (procesamiento):
```bash
docker-compose logs -f worker
```

## Probar la API

La API estará disponible en `http://localhost:8000`. Documentación interactiva: `http://localhost:8000/docs`.

### 1. Subir un archivo CSV
```bash
curl -X POST "http://localhost:8000/api/v1/upload" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@data/sample.csv"
```
**Respuesta:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "Archivo recibido y en proceso",
  "status": "accepted"
}
```

### 2. Consultar estado del Job
```bash
curl -X GET "http://localhost:8000/api/v1/jobs/550e8400-e29b-41d4-a716-446655440000"
```

### 3. Generar CSV masivo para pruebas de estrés
```bash
python data/generate_csv.py --rows 1000000 --output data/million_rows.csv
```

## Pruebas Unitarias

El proyecto cuenta con una suite de tests robusta usando `pytest` y `pytest-cov`.

```bash
# Instalar dependencias de test
pip install -r requirements.txt

# Ejecutar tests con reporte de cobertura
pytest --cov=api --cov=services --cov=worker --cov=repositories --cov-report=term-missing
```

## Workflow n8n

El servicio `n8n` se levanta en el puerto `5679`.
1. Acceder a `http://localhost:5679`.
2. Configurar usuario inicial.
3. Crear un nuevo workflow.
4. Agregar nodo "Postgres" -> Credenciales:
   - Host: `postgres`
   - User: `logyca`
   - Password: `logyca123`
   - Database: `logyca_db`
5. Ejecutar query: `SELECT * FROM sales_daily_summary ORDER BY date DESC;`

## CI/CD Azure DevOps

El pipeline de CI/CD está definido para ejecutarse en Azure DevOps. Incluye:
1. Linting con `ruff`.
2. Tests unitarios con `pytest`.
3. Construcción de imágenes Docker.
4. Push a Azure Container Registry (ACR).
5. Despliegue en Azure Container Apps.
