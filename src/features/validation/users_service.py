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

    async def verify_user_exists(self, db, user_id: str) -> bool:
        """Verify if a user exists in the dms_users collection."""
        try:
            aql = """
            FOR user IN dms_users
                FILTER user._key == @user_id
                LIMIT 1
                RETURN user
            """
            cursor = db.aql.execute(aql, bind_vars={"user_id": user_id})
            users = list(cursor)
            exists = len(users) > 0
            logger.info("🔍 User verification: id=%s exists=%s", user_id, exists)
            return exists
        except Exception as e:
            logger.error("❌ Error verifying user existence: %s", e)
            return False

    async def find_or_create_user(
        self,
        db,
        *,
        display_name: Optional[str],
        email: Optional[str],
        guid_ms: Optional[str],
    ) -> Optional[Dict[str, Any]]:
        logger.info("🔍 Searching user: display_name=%s, email=%s, guid_ms=%s", display_name, email, guid_ms)
        
        # PRIORITY 1: Search by guid_ms or email in local DB (exact match)
        local_user = find_user_by_guid_or_email(db, guid_ms=guid_ms, email=email)
        if local_user:
            logger.info("✅ Found user in local DB: %s (guid=%s, email=%s)", 
                       local_user.get("_key"), local_user.get("guid_ms"), local_user.get("email"))
            return local_user
        
        # PRIORITY 2: If we have guid_ms or email, search EXACTLY in Graph API
        if guid_ms or email:
            logger.info("🌐 Searching in Graph API by exact guid_ms or email...")
            graph_user = await self._find_in_graph_exact(guid_ms=guid_ms, email=email)
            
            if graph_user:
                logger.info("✅ Found exact match in Graph: %s", graph_user.get("displayName"))
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
        
        # PRIORITY 3: Fuzzy search by display_name (only if no guid_ms/email provided)
        if display_name and not guid_ms and not email:
            logger.info("🌐 Fuzzy searching in Graph API by display name...")
            graph_user = await self._find_in_graph(display_name=display_name, email=None)

            logger.info(f"Graph user found: {graph_user}")

            if graph_user:
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
        
        logger.info("❌ User not found in any source")
        return None

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

    async def _find_in_graph_exact(
        self, *, guid_ms: Optional[str], email: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Search for user in Graph API by exact guid_ms or email."""
        if not self._graph_client:
            logger.warning("⚠️  Graph client not available")
            return None
        
        # Try by guid_ms first (Azure AD ID)
        if guid_ms:
            try:
                logger.info("🔎 Searching Graph by guid_ms: %s", guid_ms)
                user = await self._graph_client.get_user_by_id(guid_ms)
                if user:
                    logger.info("✅ Found user by guid_ms: %s", user.get("displayName"))
                    return user
            except Exception as e:
                logger.warning("⚠️  Error searching by guid_ms: %s", e)
        
        # Try by email
        if email:
            try:
                logger.info("🔎 Searching Graph by email: %s", email)
                # Search with exact email filter
                users = await self._graph_client.search_users_by_email(email)
                if users:
                    # Return exact match
                    email_lower = email.lower()
                    for user in users:
                        user_mail = (user.get("mail") or "").lower()
                        user_upn = (user.get("userPrincipalName") or "").lower()
                        if email_lower in (user_mail, user_upn):
                            logger.info("✅ Found user by email: %s", user.get("displayName"))
                            return user
            except Exception as e:
                logger.warning("⚠️  Error searching by email: %s", e)
        
        return None

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
