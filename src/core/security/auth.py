# src/core/security/auth.py
import hashlib
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from fastapi import Depends, Header, HTTPException, Request
import redis.asyncio as aioredis

# Ajusta según tu variable de entorno o docker-compose.
REDIS_URL: str = os.getenv("AUTH_REDIS_URL", "redis://auth-redis:6379/0")

@dataclass
class AuthContext:
    user_id: str
    token_hash: str
    token_type: str
    tenant_id: str
    team_ids: List[str]
    microservices_data: Dict[str, Any]

async def _load_unified_session(token_hash: str, token_type: str) -> Optional[Dict[str, Any]]:
    """
    Busca la sesión unificada en Redis. Ajusta los prefijos según los tipos de token
    que uses (por ejemplo 'session:local' o 'session:azure').
    """
    redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    candidate_keys = [
        f"laravel_database_session:{token_type}:{token_hash}",
        f"laravel_database_session:local:{token_hash}",
        f"laravel_database_session:azure:{token_hash}",
        f"laravel_database_session:{token_hash}",
    ]
    for key in candidate_keys:
        raw = await redis.get(key)
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return None
    return None

async def get_auth_context(
    request: Request,
    x_team_id: str = Header(None, alias="X-Team-Id"),
) -> AuthContext:
    """
    Extrae y valida la identidad del usuario a partir de la sesión cacheada en auth-service.
    Comprueba que el equipo solicitado exista entre los team_ids de la sesión.
    """
    # 1. Obtener y validar el token Bearer
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Falta el token Bearer")
    token = auth[len("Bearer "):].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Token vacío")

    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()

    # 2. Cargar sesión desde Redis (sin fallback criptográfico)
    #    token_type se puede deducir del prefijo de la clave; si no lo sabes, usa 'local' por defecto.
    token_type = "local"
    session = await _load_unified_session(token_hash, token_type)
    if session is None:
        raise HTTPException(status_code=401, detail="Sesión no cacheada o expirada. Reintente autenticarse.")

    user_id = str(session.get("user_id"))
    tenant_id = session.get("tenant_id", "default")
    team_ids: List[str] = session.get("team_ids") or []

    # 4. Construir el contexto
    microservices_data = session.get("microservices_data") or {}
    return AuthContext(
        user_id=user_id,
        token_hash=token_hash,
        token_type=token_type,
        tenant_id=tenant_id,
        team_ids=team_ids,
        microservices_data=microservices_data,
    )
