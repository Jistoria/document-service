import logging
from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from src.core.storage import storage_instance
from src.features.context.utils import resolve_team_codes

logger = logging.getLogger(__name__)

PUBLIC_DOWNLOAD_STATUSES = ["confirmed", "validated"]


def normalize_object_path(object_path: str) -> str:
    clean_path = object_path
    if object_path.startswith(f"{storage_instance.bucket_name}/"):
        clean_path = object_path.replace(f"{storage_instance.bucket_name}/", "", 1)
    return clean_path


def resolve_media_type(clean_path: str) -> str:
    media_type = "application/octet-stream"
    if clean_path.endswith(".pdf"):
        return "application/pdf"
    if clean_path.endswith(".png"):
        return "image/png"
    if clean_path.endswith(".jpg") or clean_path.endswith(".jpeg"):
        return "image/jpeg"
    if clean_path.endswith(".json"):
        return "application/json"
    return media_type


def get_document_snapshot(db, doc_id: str) -> Optional[Dict[str, Any]]:
    aql = """
    FOR doc IN documents
        FILTER doc._key == @doc_id
        RETURN {
            id: doc._key,
            owner_id: doc.owner.id,
            is_public: TO_BOOL(doc.is_public),
            status: doc.status,
            storage: doc.storage
        }
    """
    cursor = db.aql.execute(aql, bind_vars={"doc_id": doc_id})
    docs = list(cursor)
    return docs[0] if docs else None


def _document_matches_team_scope(db, doc_id: str, valid_owner_ids: List[str]) -> bool:
    if not valid_owner_ids:
        return False

    aql = """
    RETURN LENGTH(
        FOR doc IN documents
            FILTER doc._key == @doc_id
            FOR owner IN 1..2 OUTBOUND doc file_located_in, belongs_to
                FILTER owner._key IN @valid_owner_ids
                LIMIT 1
                RETURN 1
    ) > 0
    """
    cursor = db.aql.execute(aql, bind_vars={"doc_id": doc_id, "valid_owner_ids": valid_owner_ids})
    return bool(next(cursor, False))


def validate_download_permissions(
    db,
    doc_snapshot: Dict[str, Any],
    user_id: str,
    read_teams: Optional[List[str]],
) -> None:
    if doc_snapshot.get("is_public") and doc_snapshot.get("status") in PUBLIC_DOWNLOAD_STATUSES:
        return

    if doc_snapshot.get("owner_id") == user_id:
        return

    if read_teams and "*" in read_teams:
        return

    valid_owner_ids = resolve_team_codes(db, read_teams or [], return_full_object=False)
    if _document_matches_team_scope(db, doc_snapshot["id"], valid_owner_ids):
        return

    raise HTTPException(status_code=403, detail="No tienes permisos para descargar este documento")


def build_streaming_payload(doc_snapshot: Dict[str, Any], object_path: str) -> tuple[Any, str, str]:
    clean_path = normalize_object_path(object_path)

    allowed_paths = {
        (doc_snapshot.get("storage") or {}).get("pdf_path"),
        (doc_snapshot.get("storage") or {}).get("pdf_original_path"),
        (doc_snapshot.get("storage") or {}).get("json_path"),
        (doc_snapshot.get("storage") or {}).get("text_path"),
    }
    allowed_paths = {p for p in allowed_paths if p}

    if allowed_paths and clean_path not in allowed_paths:
        raise HTTPException(status_code=403, detail="Ruta de archivo no autorizada para este documento")

    data_stream = storage_instance.client.get_object(storage_instance.bucket_name, clean_path)
    media_type = resolve_media_type(clean_path)
    filename = clean_path.split("/")[-1]

    return data_stream, media_type, filename


async def log_document_download(db, doc_id: str, user_id: str, ip_address: Optional[str] = None) -> None:
    try:
        aql = """
        INSERT {
            document_id: @document_id,
            user_id: @user_id,
            timestamp: DATE_NOW(),
            ip_address: @ip_address
        } INTO audit_downloads
        """
        db.aql.execute(
            aql,
            bind_vars={
                "document_id": doc_id,
                "user_id": user_id,
                "ip_address": ip_address,
            },
        )
    except Exception:
        logger.warning("No se pudo registrar auditoría de descarga", exc_info=True)
