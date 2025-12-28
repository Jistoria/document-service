# src/features/ocr_updates/pipeline/user_lookup.py
import logging
from typing import Any, Dict, Optional

from src.core.config import settings
from .person_normalizer import build_search_terms
from .graph_client import MicrosoftGraphClient
from .users_repository import ensure_users_collection, upsert_user_from_graph

logger = logging.getLogger(__name__)


def find_user_in_db(db, *, name: Optional[str], email: Optional[str]) -> Optional[Dict[str, Any]]:
    ensure_users_collection(db)

    if email:
        aql = """
        FOR u IN dms_users
            FILTER u.email != null AND LOWER(u.email) == LOWER(@email)
            LIMIT 1
            RETURN u
        """
        res = list(db.aql.execute(aql, bind_vars={"email": email}))
        if res:
            return res[0]

    if name:
        aql = """
        FOR u IN dms_users
            LET full = CONCAT_SEPARATOR(' ', u.name, u.last_name)
            FILTER CONTAINS(LOWER(full), LOWER(@name))
               OR CONTAINS(LOWER(u.name), LOWER(@name))
               OR CONTAINS(LOWER(u.last_name), LOWER(@name))
            LIMIT 1
            RETURN u
        """
        res = list(db.aql.execute(aql, bind_vars={"name": name}))
        if res:
            return res[0]

    return None


async def find_user_with_fallback(db, raw_text: str) -> Optional[Dict[str, Any]]:
    name, email, parts = build_search_terms(raw_text)

    # 1) DB
    user = find_user_in_db(db, name=name, email=email)
    if user:
        return {"source": "db", "user": user, "normalized": {"name": name, "email": email}}

    # 2) Graph creds
    tenant = getattr(settings, "AZURE_TENANT_ID", None)
    client_id = getattr(settings, "AZURE_CLIENT_ID", None)
    client_secret = getattr(settings, "AZURE_CLIENT_SECRET", None)
    if not (tenant and client_id and client_secret):
        return None

    graph = MicrosoftGraphClient(tenant_id=tenant, client_id=client_id, client_secret=client_secret)

    limit = int(getattr(settings, "GRAPH_DEFAULT_LIMIT", 10))

    # ✅ 1 sola llamada optimizada
    candidates = await graph.search_users_optimized(parts=parts, limit=limit)
    if not candidates:
        return None

    # preferir match por email exacto si hay
    chosen = None
    if email:
        em = email.lower()
        for u in candidates:
            m = (u.get("mail") or "").lower()
            upn = (u.get("userPrincipalName") or "").lower()
            if em in (m, upn):
                chosen = u
                break

    chosen = chosen or candidates[0]

    graph_payload = {
        "azure_id": chosen.get("id"),
        "displayName": chosen.get("displayName"),
        "mail": chosen.get("mail"),
        "userPrincipalName": chosen.get("userPrincipalName"),
        "givenName": chosen.get("givenName"),
        "surname": chosen.get("surname"),
        "jobTitle": chosen.get("jobTitle"),
        "department": chosen.get("department"),
        "companyName": chosen.get("companyName"),
        "officeLocation": chosen.get("officeLocation"),
    }

    cached = upsert_user_from_graph(db, graph_user=graph_payload, source="graph")
    return {"source": "graph_cached", "user": cached, "normalized": {"name": name, "email": email}}
