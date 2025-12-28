import logging
import httpx
from src.core.storage import storage_instance
from src.core.config import settings

logger = logging.getLogger(__name__)


async def transfer_all_files(source_urls: dict, base_dest_path: str):
    results = {}

    files_to_transfer = [
        ("minio_pdfa", ".pdf", "pdf", "application/pdf"),
        ("minio_validated", ".json", "json", "application/json"),
        ("minio_text", ".txt", "text", "text/plain"),
        ("minio_original_pdf", ".pdf", "pdf_original_path", "application/pdf")
    ]

    async with httpx.AsyncClient() as client:
        for source_key, ext, internal_key, content_type in files_to_transfer:
            url = source_urls.get(source_key)
            if url:
                try:
                    # SIMPLEMENTE USAMOS LA URL ORIGINAL
                    # Al conectar el contenedor a la red, "http://minio:9000" funcionará
                    logger.info(f"⬇️ Descargando de: {url}")
                    resp = await client.get(url, timeout=30.0)
                    resp.raise_for_status()

                    dest_path = f"{base_dest_path}/{internal_key}_document{ext}"
                    full_relative_path = storage_instance.upload_file(resp.content, dest_path, content_type)

                    results[internal_key] = full_relative_path
                    logger.info(f"📦 Transferido: {internal_key}")

                except Exception as e:
                    logger.warning(f"⚠️ Error transfiriendo {source_key}: {e}")
                    results[internal_key] = None

    return results