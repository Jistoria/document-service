import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Tus imports originales
from src.features.sync_master_data.job import run_sync_job
from src.core.setup import init_arango_schema, init_arangosearch_views, configure_minio_cors
from src.core.database import db_instance
from src.features.ocr_updates.consumer import consume_ocr_finalized
from src.features.validation.router import router as validation_router
from src.features.search.router import router as search_router
from src.features.storage.router import router as storage_router
from src.core.storage import storage_instance


# Importar routers futuros aquí
# from src.features.ingest.router import router as ingest_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- 1. LÓGICA DE INICIO (Startup) ---
    print("🚀 Iniciando servicios...")

    # Inicializar DB (Síncrono)
    db = db_instance.get_db()
    init_arango_schema(db)
    init_arangosearch_views(db)

    # Iniciar Consumidor Kafka como tarea de fondo
    # Guardamos la tarea en una variable para poder controlarla después
    consumer_task = asyncio.create_task(consume_ocr_finalized())
    print("Consumidor Kafka iniciado en segundo plano")

    yield  # <-- Aquí es donde la API está corriendo y recibiendo peticiones

    # --- 2. LÓGICA DE APAGADO (Shutdown) ---
    print("Deteniendo servicios...")

    # Cancelamos la tarea del consumidor para liberar la conexión a Kafka
    consumer_task.cancel()
    try:
        # Esperamos a que termine de cerrarse
        await consumer_task
    except asyncio.CancelledError:
        print("✅ Consumidor Kafka detenido correctamente")


# Pasamos el lifespan al constructor de la app
app = FastAPI(title="Document Management Service", lifespan=lifespan)

origins = [
    "http://localhost:3000",    # Tu Frontend Vue
    "http://127.0.0.1:3000",    # Vue (por si acaso)
    "*"                         # (Opcional) Permitir a todos en desarrollo
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,      # Lista de orígenes permitidos
    allow_credentials=True,     # Permitir cookies/tokens
    allow_methods=["*"],        # Permitir GET, POST, PUT, DELETE, etc.
    allow_headers=["*"],        # Permitir todos los headers
)

app.include_router(validation_router)
app.include_router(search_router)
app.include_router(storage_router)

@app.get("/")
def health_check():
    return {"status": "ok", "service": "FastAPI + ArangoDB + MinIO"}


@app.post("/admin/force-sync")
async def force_sync():
    """Endpoint manual para forzar la sincronización"""
    await run_sync_job()
    return {"status": "Sync job triggered"}

# app.include_router(ingest_router)