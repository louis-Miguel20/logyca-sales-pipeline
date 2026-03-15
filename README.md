[![CI/CD Pipeline](https://github.com/louis-Miguel20/logyca-sales-pipeline/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/louis-Miguel20/logyca-sales-pipeline/actions/workflows/ci-cd.yml)
# LOGYCA Sales Pipeline

Sistema distribuido de alto rendimiento para la ingesta, procesamiento y análisis de ventas masivas mediante archivos CSV. Diseñado para escalar horizontalmente y manejar **millones de registros** sin saturar la memoria RAM ni colapsar la base de datos.

## ✨ Lo Novedoso de este Proyecto y su Impacto

Este proyecto resuelve uno de los problemas más comunes y críticos en la ingeniería de datos moderna: **la ingesta de archivos gigantes de forma segura y eficiente.**

Tradicionalmente, cargar un archivo CSV de 5GB en una API hace que el servidor consuma toda su RAM y colapse (Out of Memory). Además, insertar filas una por una en la base de datos toma horas y bloquea otras operaciones.

**Nuestro Impacto y Solución:**
1. **Resiliencia Total (Cero Bloqueos):** La API recibe el archivo, lo sube directamente a Azure Blob Storage, encola un mensaje y responde al usuario en milisegundos. La API nunca se bloquea procesando datos.
2. **Procesamiento Eficiente en Memoria (Streaming):** El Worker descarga el archivo de Azure en "pedacitos" (Chunks). Puede procesar un archivo de 1 millón de filas consumiendo menos de 50MB de RAM.
3. **Velocidad Extrema en Base de Datos (Bulk Inserts):** En lugar de hacer `INSERT` fila por fila, el sistema agrupa los datos en lotes y usa el comando nativo `COPY` de PostgreSQL (vía `asyncpg`), siendo entre **10x y 50x más rápido** que las librerías ORM tradicionales como SQLAlchemy.
4. **Inteligencia de Negocio con Metabase:** Incluye un dashboard interactivo para que los analistas visualicen las ventas en tiempo real sin escribir SQL.
5. **Observabilidad Avanzada:** Todo el sistema emite logs estructurados en formato JSON (vía `structlog`), lo que permite rastrear exactamente qué pasó con cada archivo, en qué lote falló o cuánto tardó.
6. **Automatización con n8n:** Incluye una integración lista para usar con n8n para generar reportes y automatizaciones basadas en los datos procesados.

---

## 🏗️ Arquitectura

El sistema sigue una arquitectura orientada a eventos con desacoplamiento total:

```ascii
[ Cliente ] 
     |
     | (1) POST /api/v1/upload (CSV)
     v
[ API FastAPI ] 
     |-----------------------> (2) Sube CSV a Azure Blob Storage
     |
     |-----------------------> (3) Crea registro en DB (Estado: PENDING)
     |
     |-----------------------> (4) Envía mensaje a Azure Queue Storage
     |
     v
( 202 Accepted - Retorna Job ID )

[ Worker Asíncrono ] <---- (5) Lee mensaje de Azure Queue
     |
     |-----> (6) Descarga CSV en Streaming (Chunks) desde Blob
     |
     |-----> (7) Procesa, parsea y valida filas en memoria
     |
     v
[ PostgreSQL ] <---- (8) Bulk Insert (COPY) en lotes de 1000 filas
     ^
     |
[ Metabase ] <------ (9) Visualización y BI
     |
[ n8n ] <----------- (10) Automatización de Reportes
```

---

## 🛠️ Stack Tecnológico

| Tecnología | Versión | Propósito |
|------------|---------|-----------|
| **Python** | 3.11+ | Lenguaje base optimizado para I/O asíncrono |
| **FastAPI** | 0.115.0 | Framework web asíncrono de altísimo rendimiento |
| **PostgreSQL** | 16 | Base de datos relacional robusta |
| **AsyncPG** | 0.30.0 | Driver de base de datos más rápido para Python (manejo nativo de COPY) |
| **Azure Storage** | v12 | Almacenamiento distribuido (Blobs para archivos y Colas para mensajería) |
| **Docker** | Compose V2 | Containerización y orquestación multi-servicio |
| **Structlog** | 24.4.0 | Logging estructurado (JSON) para observabilidad profesional |
| **Tenacity** | 9.0.0 | Resiliencia y reintentos automáticos (Backoff exponencial) ante fallos de red |
| **n8n** | Latest | Motor de automatización de flujos de trabajo (Low-Code) |
| **Metabase** | Latest | Herramienta de Business Intelligence (BI) open source |

---

## 🚀 Cómo Ejecutar el Proyecto (Paso a Paso)

Todo el proyecto está dockerizado para que no necesites instalar dependencias locales más allá de Docker.

### 1. Configuración Inicial
Asegúrate de tener un archivo `.env` en la raíz del proyecto. Si no lo tienes, copia el ejemplo:
```bash
cp .env.example .env
```
*(El archivo `.env` ya incluye las credenciales de conexión a la base de datos local y a Azure Storage).*

### 2. Levantar la Infraestructura
Ejecuta el siguiente comando para construir las imágenes y levantar la base de datos, la API, el Worker y n8n:
```powershell
docker compose up -d --build
```

### ☁️ Opción: Despliegue desde Docker Hub (Sin construir)
Si prefieres descargar las imágenes ya construidas en lugar de compilar el código fuente:

1. Modifica el archivo `docker-compose.yml`:
   - En el servicio `api`, cambia `build: ...` por `image: tu_usuario/logyca-api:latest`
   - En el servicio `worker`, cambia `build: ...` por `image: tu_usuario/logyca-worker:latest`
2. Ejecuta:
```powershell
docker compose up -d
```

### 3. Ejecutar Pruebas (Tests)

**Test 1: Subir un CSV de ejemplo**
```powershell
curl.exe -X POST http://localhost:8000/api/v1/upload -F "file=@data/sample.csv"
```
*(Esto te devolverá un JSON con un `job_id`, cópialo).*

**Test 2: Verificar el estado del procesamiento**
Reemplaza `TU_JOB_ID` con el ID que obtuviste en el paso anterior:
```powershell
curl.exe http://localhost:8000/api/v1/job/TU_JOB_ID
```

**Test 3: Ver al Worker en acción**
Para ver cómo el worker descarga de Azure y procesa en tiempo real:
```powershell
docker compose logs -f worker
```

**Test 4: Consultar la Base de Datos**
Para confirmar que los datos se guardaron (tu DB se llama `logyca_bd`):
```powershell
docker compose exec postgres psql -U logyca -d logyca_bd -c "SELECT COUNT(*) as total_ventas FROM sales;"
```

**Test 5: Correr las Pruebas Unitarias**
Ejecutamos la suite de tests automatizados dentro del contenedor de la API:
```powershell
docker compose exec api pytest tests/ -v --tb=short
```

**Test 6: Prueba de Carga Extrema (1 Millón de Filas)**
Generamos un archivo gigante dentro del contenedor y lo subimos inmediatamente para ver el poder del streaming:
```powershell
# 1. Generar el archivo y subirlo
docker compose exec api sh -c "python data/generate_csv.py --rows 1000000 --output data/million_rows.csv && curl -X POST http://localhost:8000/api/v1/upload -F 'file=@data/million_rows.csv'"

# 2. Monitorear el consumo y la velocidad del worker
docker compose logs -f worker
```

---

## 🤖 Integración con n8n y Metabase

### Metabase (Business Intelligence)
Accede a **http://localhost:3000** para ver tus datos visualizados.
1. Configura la conexión a Postgres (Host: `postgres`, User: `logyca`, Pass: `logyca123`, DB: `logyca_bd`).
2. Crea preguntas simples como "¿Cuál es el producto más vendido?" o "¿Ventas totales por día?".

### n8n (Automatización)
Hemos incluido un contenedor de **n8n** dedicado para este proyecto, corriendo en el puerto **9000**.

**Pasos para usar n8n:**
1. Abre tu navegador y entra a: `http://localhost:9000`
2. El sistema está preconfigurado con autenticación básica de pruebas (según el `docker-compose.yml`):
   - **Usuario:** `admin`
   - **Contraseña:** `admin123`
3. En la carpeta `n8n/` del proyecto encontrarás el archivo `workflow_sales_summary.json`.
4. En la interfaz de n8n, ve a *Workflows* -> *Add Workflow* -> Menú de los 3 puntos (arriba a la derecha) -> *Import from File*.
5. Selecciona el archivo `workflow_sales_summary.json`.
6. **Configurar Credenciales de Postgres en n8n:**
   - Host: `postgres` *(Usa el nombre del contenedor, no localhost)*
   - Database: `logyca_bd`
   - User: `logyca`
   - Password: `logyca123`
   - Port: `5432`

Este workflow puede configurarse para ejecutarse diariamente (vía nodo Schedule) y consultar la tabla `sales_daily_summary` para enviar reportes automáticos por correo o Slack.
