from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import HTTPException

from src.core.database import db_instance
from src.core.security.auth import AuthContext
from src.core.security.permissions import get_permitted_scopes_logic
from src.core.storage import storage_instance
from src.features.context.utils import resolve_team_codes


class StorageProxyService:
    """Servicio para autorización ABAC y auditoría de descargas de documentos."""

    def __init__(self):
        self.db = db_instance.get_db()

    @staticmethod
    def normalize_object_path(object_path: str) -> str:
        clean_path = object_path
        bucket_prefix = f"{storage_instance.bucket_name}/"
        if object_path.startswith(bucket_prefix):
            clean_path = object_path.replace(bucket_prefix, "", 1)
        return clean_path

    async def authorize_document_download(self, object_path: str, ctx: AuthContext) -> Dict[str, Any]:
        clean_path = self.normalize_object_path(object_path)
        candidate_paths = [object_path, clean_path, f"{storage_instance.bucket_name}/{clean_path}"]

        document = self._get_document_by_storage_paths(candidate_paths)
        if not document:
            raise HTTPException(status_code=404, detail="Documento no encontrado para la ruta solicitada.")

        if await self._has_document_access(document_key=document.get("_key"), document=document, ctx=ctx):
            return document

        raise HTTPException(
            status_code=403,
            detail="No tienes permisos para descargar este documento",
        )

    async def _has_document_access(self, document_key: Optional[str], document: Dict[str, Any], ctx: AuthContext) -> bool:
        is_public = bool(document.get("is_public"))
        owner_id = ((document.get("owner") or {}).get("id") or "")

        if is_public:
            return True

        if owner_id and owner_id == ctx.user_id:
            return True

        read_teams = await get_permitted_scopes_logic("dms.document.read", ctx)
        if "*" in read_teams:
            return True

        valid_owner_ids = self._resolve_team_codes_to_uuids(read_teams)
        if not valid_owner_ids or not document_key:
            return False

        return self._document_in_allowed_teams(document_key, valid_owner_ids)

    def _resolve_team_codes_to_uuids(self, allowed_teams: List[str]) -> List[str]:
        if not allowed_teams:
            return []

        return resolve_team_codes(self.db, allowed_teams, return_full_object=False)

    def _document_in_allowed_teams(self, doc_id: str, valid_owner_ids: List[str]) -> bool:
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

        cursor = self.db.aql.execute(aql, bind_vars={"doc_id": doc_id, "valid_owner_ids": valid_owner_ids})
        result = list(cursor)
        return bool(result and result[0] is True)

    def _get_document_by_storage_paths(self, candidate_paths: List[str]) -> Optional[Dict[str, Any]]:
        aql = """
        FOR doc IN documents
            FILTER doc.storage != null
              AND (
                doc.storage.pdf_path IN @candidate_paths
                OR doc.storage.path IN @candidate_paths
                OR doc.storage.file_path IN @candidate_paths
                OR doc.storage.original_path IN @candidate_paths
              )
            LIMIT 1
            RETURN doc
        """

        cursor = self.db.aql.execute(aql, bind_vars={"candidate_paths": candidate_paths})
        result = list(cursor)
        return result[0] if result else None

    async def log_document_download(
        self,
        doc_id: str,
        user_id: str,
        ip_address: Optional[str] = None,
    ) -> None:
        aql = """
        INSERT {
            document_id: @document_id,
            user_id: @user_id,
            timestamp: DATE_NOW(),
            ip_address: @ip_address
        } INTO audit_downloads
        """

        self.db.aql.execute(
            aql,
            bind_vars={
                "document_id": doc_id,
                "user_id": user_id,
                "ip_address": ip_address,
            },
        )


storage_proxy_service = StorageProxyService()
