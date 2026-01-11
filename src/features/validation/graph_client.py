import logging
from typing import Optional

from src.core.config import settings
from src.features.ocr_updates.pipeline.graph_client import MicrosoftGraphClient

logger = logging.getLogger(__name__)


def get_graph_client() -> Optional[MicrosoftGraphClient]:
    tenant = getattr(settings, "AZURE_TENANT_ID", None)
    client_id = getattr(settings, "AZURE_CLIENT_ID", None)
    client_secret = getattr(settings, "AZURE_CLIENT_SECRET", None)

    if not (tenant and client_id and client_secret):
        logger.warning("⚠️ Faltan credenciales Azure para Graph.")
        return None

    return MicrosoftGraphClient(tenant_id=tenant, client_id=client_id, client_secret=client_secret)
