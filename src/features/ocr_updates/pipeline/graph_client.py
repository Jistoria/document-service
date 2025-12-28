import logging
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


class MicrosoftGraphClient:
    def __init__(self, *, tenant_id: str, client_id: str, client_secret: str):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: Optional[str] = None
        self._token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        self._graph_base = "https://graph.microsoft.com/v1.0"

    async def _get_token(self) -> str:
        if self._token:
            return self._token

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "https://graph.microsoft.com/.default",
        }

        async with httpx.AsyncClient() as client:
            resp = await client.post(self._token_url, data=data, timeout=30.0)
            resp.raise_for_status()
            payload = resp.json()

        token = payload.get("access_token")
        if not token:
            raise RuntimeError("No se recibió access_token desde Azure AD.")
        self._token = token
        return token

    def _escape_odata(self, s: str) -> str:
        # escapa comillas simples para OData
        return s.replace("'", "''")

    def _build_filter(self, *, email_prefix: Optional[str], first: Optional[str], first2: Optional[str], last: Optional[str]) -> str:
        parts = []

        # email / upn
        if email_prefix:
            ep = self._escape_odata(email_prefix)
            parts.append(f"startsWith(mail,'{ep}')")
            parts.append(f"startsWith(userPrincipalName,'{ep}')")

        # displayName / givenName / surname
        # usamos first / last porque a veces displayName está invertido
        if first:
            f = self._escape_odata(first)
            parts.append(f"startsWith(displayName,'{f}')")
            parts.append(f"startsWith(givenName,'{f}')")

        if first2:
            f2 = self._escape_odata(first2)
            parts.append(f"startsWith(displayName,'{f2}')")

        if last:
            l = self._escape_odata(last)
            parts.append(f"startsWith(displayName,'{l}')")
            parts.append(f"startsWith(surname,'{l}')")

        # Si no hay nada, filtramos por algo neutro (pero mejor no llamar)
        return " or ".join(parts)

    async def search_users_optimized(self, *, parts: Dict[str, Any], limit: int = 10) -> List[Dict[str, Any]]:
        """
        Búsqueda optimizada (1 request) usando $filter con múltiples startsWith.
        """
        token = await self._get_token()

        filter_expr = self._build_filter(
            email_prefix=parts.get("email_prefix"),
            first=parts.get("first"),
            first2=parts.get("first2"),
            last=parts.get("last"),
        )

        if not filter_expr:
            return []

        params = {
            "$filter": filter_expr,
            "$select": "id,displayName,mail,userPrincipalName,givenName,surname,jobTitle,department,companyName,officeLocation",
            "$top": str(limit),
        }

        headers = {
            "Authorization": f"Bearer {token}",
            # ConsistencyLevel solo es obligatorio si usas $count / advanced queries
            # lo dejamos igual por seguridad
            "ConsistencyLevel": "eventual",
        }

        url = f"{self._graph_base}/users"

        async with httpx.AsyncClient() as client:
            resp = await client.get(url, params=params, headers=headers, timeout=30.0)
            resp.raise_for_status()
            data: Dict[str, Any] = resp.json()

        return data.get("value", []) or []