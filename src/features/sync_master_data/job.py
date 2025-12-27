from src.core.database import get_db
from .client import fetch_master_data
from .logic import sync_to_arango


async def run_sync_job():
    """
    Esta función orquesta todo el proceso.
    Puede ser llamada por un Cron, un endpoint o al inicio.
    """
    try:
        # 1. Obtener datos
        data = await fetch_master_data()

        # 2. Obtener conexión DB
        db = get_db()

        # 3. Guardar en Grafo
        await sync_to_arango(db, data)

    except Exception as e:
        print(f"🚨 Falló el Job de Sincronización: {e}")