import logging
from typing import Any, Dict, Optional

from src.features.ocr_updates.pipeline.person_normalizer import build_search_terms
from src.features.ocr_updates.pipeline.users_repository import (
    find_user_by_guid_or_email,
    upsert_user_from_graph,
)

from .utils import build_display_name

logger = logging.getLogger(__name__)


class UsersService:
    def __init__(self, graph_client):
        self._graph_client = graph_client

    async def find_or_create_user(
        self,
        db,
        *,
        display_name: Optional[str],
        email: Optional[str],
        guid_ms: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        local_user = find_user_by_guid_or_email(db, guid_ms=guid_ms, email=email)
        if local_user:
            return local_user

        graph_user = await self._find_in_graph(display_name=display_name, email=email)
        if not graph_user:
            return None

        graph_payload = {
            "azure_id": graph_user.get("id"),
            "displayName": graph_user.get("displayName"),
            "mail": graph_user.get("mail"),
            "userPrincipalName": graph_user.get("userPrincipalName"),
            "givenName": graph_user.get("givenName"),
            "surname": graph_user.get("surname"),
            "jobTitle": graph_user.get("jobTitle"),
            "department": graph_user.get("department"),
            "officeLocation": graph_user.get("officeLocation"),
            "companyName": graph_user.get("companyName"),
        }

        return upsert_user_from_graph(db, graph_user=graph_payload, source="validation_graph")

    async def create_new_user_node(self, db, display_name: str, email: Optional[str] = None) -> str:
        if not db.has_collection("dms_users"):
            db.create_collection("dms_users")

        parts = (display_name or "").strip().split()
        first_name = parts[0] if parts else None
        last_name = " ".join(parts[1:]) if len(parts) > 1 else None

        aql = """
        INSERT {
            name: @name,
            last_name: @last_name,
            email: @email,
            type: 'user',
            status: 'active',
            source: 'manual_validation_creation',
            created_at: DATE_NOW()
        } IN dms_users
        RETURN NEW._key
        """
        cursor = db.aql.execute(
            aql,
            bind_vars={
                "name": first_name or display_name,
                "last_name": last_name,
                "email": email,
            },
        )
        return list(cursor)[0]

    def build_metadata_from_user(self, user: Dict[str, Any]) -> Dict[str, Any]:
        display_name = build_display_name(user.get("name"), user.get("last_name"))
        payload = {
            "id": user.get("_key"),
            "type": "user",
            "display_name": display_name,
            "email": user.get("email"),
        }
        return {k: v for k, v in payload.items() if v}

    async def _find_in_graph(
        self, *, display_name: Optional[str], email: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        if not self._graph_client:
            return None

        raw_text = display_name or email
        if not raw_text:
            return None

        _, _, parts = build_search_terms(raw_text)
        if not parts:
            return None

        candidates = await self._graph_client.search_users_optimized(parts=parts, limit=5)
        if not candidates:
            return None

        if email:
            email_lower = email.lower()
            for candidate in candidates:
                mail = (candidate.get("mail") or "").lower()
                upn = (candidate.get("userPrincipalName") or "").lower()
                if email_lower in (mail, upn):
                    return candidate

        if display_name:
            display_lower = display_name.lower()
            for candidate in candidates:
                cand_display = (candidate.get("displayName") or "").lower()
                if cand_display == display_lower:
                    return candidate

        return candidates[0]
