import httpx
import os
from .models import MasterDataExport

# URL de tu servicio Laravel (ajusta el host según docker-compose)
LARAVEL_API_URL = os.getenv("LARAVEL_API_URL", "http://management-nginx/api/internal/sync-master-data")


async def fetch_master_data() -> MasterDataExport:
    async with httpx.AsyncClient() as client:
        print(f"Conectando a {LARAVEL_API_URL}...")
        try:
            # Si tienes auth, añade headers aquí
            response = await client.get(LARAVEL_API_URL, timeout=30.0)
            response.raise_for_status()

            data = response.json()
            # Validación automática con Pydantic
            return MasterDataExport(**data)

        except httpx.RequestError as e:
            print(f"Error de ssconexión: {e}")
            raise
        except Exception as e:
            print(f"Error procesando datos: {e}")
            raise