# src/features/ocr_updates/pipeline/users_repository.py
import re
from datetime import datetime
from typing import Any, Dict, Optional

USERS_COLLECTION = "dms_users"


def _now_iso() -> str:
    return datetime.now().isoformat()


def _safe_key_from_guid(guid: str) -> str:
    """
    Arango _key: lo dejamos alfanumérico para evitar problemas.
    """
    if not guid:
        raise ValueError("guid_ms vacío para generar _key")

    key = guid.strip().lower().replace("-", "")
    key = re.sub(r"[^a-z0-9_]", "", key)
    return key


def ensure_users_collection(db):
    if not db.has_collection(USERS_COLLECTION):
        db.create_collection(USERS_COLLECTION)

    col = db.collection(USERS_COLLECTION)

    # Índice por guid_ms (unique, sparse)
    try:
        col.add_hash_index(fields=["guid_ms"], unique=True, sparse=True)
    except Exception:
        pass

    # Índice por email (NO unique si en tu org hay duplicados; yo lo pongo unique sparse por defecto)
    try:
        col.add_hash_index(fields=["email"], unique=True, sparse=True)
    except Exception:
        pass

    # Índice por name/last_name para búsquedas (no unique)
    try:
        col.add_hash_index(fields=["name", "last_name"], unique=False, sparse=True)
    except Exception:
        pass


def find_user_by_guid_or_email(db, *, guid_ms: Optional[str], email: Optional[str]) -> Optional[Dict[str, Any]]:
    if not db.has_collection(USERS_COLLECTION):
        return None

    if guid_ms:
        aql = f"""
        FOR u IN {USERS_COLLECTION}
            FILTER u.guid_ms == @guid
            LIMIT 1
            RETURN u
        """
        res = list(db.aql.execute(aql, bind_vars={"guid": guid_ms}))
        if res:
            return res[0]

    if email:
        aql = f"""
        FOR u IN {USERS_COLLECTION}
            FILTER u.email != null AND LOWER(u.email) == LOWER(@email)
            LIMIT 1
            RETURN u
        """
        res = list(db.aql.execute(aql, bind_vars={"email": email}))
        if res:
            return res[0]

    return None


def upsert_user_from_graph(db, *, graph_user: Dict[str, Any], source: str = "graph") -> Dict[str, Any]:
    """
    Upsert en dms_users basado en Graph (solo lectura en Graph, pero cache local).
    graph_user esperado:
      {
        "azure_id": "...guid...",
        "displayName": "...",
        "mail": "...",
        "userPrincipalName": "...",
        "givenName": "...",
        "surname": "...",
        "jobTitle": "...",
        "department": "...",
        ...
      }
    """
    ensure_users_collection(db)

    guid_ms = graph_user.get("azure_id")
    if not guid_ms:
        raise ValueError("graph_user no trae azure_id")

    email = graph_user.get("mail") or graph_user.get("userPrincipalName")
    display = (graph_user.get("displayName") or "").strip()
    given = (graph_user.get("givenName") or "").strip()
    surname = (graph_user.get("surname") or "").strip()

    name = (given or display).strip() or "Desconocido"
    last_name = surname

    key = _safe_key_from_guid(guid_ms)
    now = _now_iso()

    doc = {
        "_key": key,
        "guid_ms": guid_ms,
        "name": name,
        "last_name": last_name,
        "email": email,
        "status": "active",
        "source": source,
        "job_title": graph_user.get("jobTitle"),
        "department": graph_user.get("department"),
        "company_name": graph_user.get("companyName"),
        "office_location": graph_user.get("officeLocation"),
        "updated_at": now,
    }

    # UPSERT robusto:
    # - Si ya existe por guid_ms, lo actualiza.
    # - Si existe por email (y no tiene guid_ms), lo "vincula" y actualiza.
    aql = f"""
    LET existing = FIRST(
        FOR u IN {USERS_COLLECTION}
            FILTER u.guid_ms == @guid
               OR (u.email != null AND @email != null AND LOWER(u.email) == LOWER(@email))
            LIMIT 1
            RETURN u
    )

    UPSERT {{ _key: @key }}
        INSERT MERGE(@doc, {{ created_at: @now }})
        UPDATE MERGE(existing, @doc)
    IN {USERS_COLLECTION}

    RETURN NEW
    """
    res = list(
        db.aql.execute(
            aql,
            bind_vars={
                "guid": guid_ms,
                "email": email,
                "key": key,
                "doc": doc,
                "now": now,
            },
        )
    )
    return res[0] if res else doc
