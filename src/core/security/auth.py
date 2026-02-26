# src/core/security/auth.py

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
import jwt
import redis.asyncio as aioredis
from fastapi import Header, HTTPException, Request
from redis.exceptions import ConnectionError, RedisError, TimeoutError

from src.core.config import settings

logger = logging.getLogger(__name__)

REDIS_URL: str = os.getenv("AUTH_REDIS_URL", settings.AUTH_REDIS_URL)

# ---------------------------------------------------------------------
# Tipos / Modelos
# ---------------------------------------------------------------------

@dataclass
class AuthContext:
    user_id: str
    token_hash: str
    token_type: str
    tenant_id: str
    team_ids: List[str]
    microservices_data: Dict[str, Any]


SessionDict = Dict[str, Any]


# ---------------------------------------------------------------------
# Redis client (pool reutilizable)
# ---------------------------------------------------------------------

_redis_client: Optional[aioredis.Redis] = None
_redis_lock = asyncio.Lock()


async def _get_redis() -> aioredis.Redis:
    """
    Retorna un cliente Redis reutilizable (pool). Evita crear conexiones por request.
    """
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    async with _redis_lock:
        if _redis_client is None:
            _redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        return _redis_client


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

_SANITIZE_RE = re.compile(r"[^a-z0-9_]")


def sanitize_key(val: Optional[str]) -> str:
    if not val:
        return ""
    k = val.strip().lower().replace("-", "")
    return _SANITIZE_RE.sub("", k)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _detect_provider(token: str) -> Tuple[str, str, Optional[str]]:
    """
    Retorna (issuer, provider, kid)
    provider: 'azure' | 'local'
    """
    header = jwt.get_unverified_header(token)
    payload = jwt.decode(token, options={"verify_signature": False})

    kid = header.get("kid")
    iss = payload.get("iss", "")

    if "login.microsoftonline.com" in iss:
        return iss, "azure", kid

    # Local (Passport)
    if not kid:
        kid = "passport-v1"
    return iss, "local", kid


# ---------------------------------------------------------------------
# JWKS Cache (por URL) con TTL + lock por URL
# ---------------------------------------------------------------------

_JWKS_CACHE: Dict[str, Dict[str, Any]] = {}
_JWKS_LOCKS: Dict[str, asyncio.Lock] = {}
_JWKS_TTL_SECONDS = 3600


def _get_jwks_lock(url: str) -> asyncio.Lock:
    lock = _JWKS_LOCKS.get(url)
    if lock is None:
        lock = asyncio.Lock()
        _JWKS_LOCKS[url] = lock
    return lock


async def _fetch_jwks(jwks_url: str) -> dict:
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(jwks_url)
        resp.raise_for_status()
        return resp.json()


async def _get_jwks_data(jwks_url: str) -> dict:
    now = time.time()
    cache_entry = _JWKS_CACHE.get(jwks_url)

    if cache_entry and now <= cache_entry["expires_at"]:
        return cache_entry["keys"]

    lock = _get_jwks_lock(jwks_url)
    async with lock:
        # Re-check dentro del lock
        now = time.time()
        cache_entry = _JWKS_CACHE.get(jwks_url)
        if cache_entry and now <= cache_entry["expires_at"]:
            return cache_entry["keys"]

        try:
            logger.info(f" Refrescando JWKS desde {jwks_url}")
            keys_data = await _fetch_jwks(jwks_url)
            _JWKS_CACHE[jwks_url] = {
                "keys": keys_data,
                "expires_at": now + _JWKS_TTL_SECONDS,
            }
            return keys_data
        except Exception as e:
            logger.error(f"Error descargando JWKS de {jwks_url}: {e}")
            if cache_entry:
                # Último recurso: devolver caché vieja
                return cache_entry["keys"]
            raise HTTPException(status_code=503, detail="No se pudieron obtener las llaves de validación.")


async def _get_signing_key(token: str):
    """
    Resuelve la public key correcta (Azure o Local) desde JWKS, usando kid.
    """
    iss, provider, kid = _detect_provider(token)

    if provider == "azure":
        tenant_id = settings.AZURE_TENANT_ID
        jwks_url = f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
    else:
        jwks_url = settings.AUTH_JWKS_URL

    jwks_data = await _get_jwks_data(jwks_url)

    keys = jwks_data.get("keys", [])
    for key_data in keys:
        key_kid = key_data.get("kid")

        # match por kid (Azure y Local moderno)
        if key_kid and kid and key_kid == kid:
            return jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key_data))

        # fallback local: solo una key sin kid o token legacy
        if provider == "local" and (not key_kid) and kid == "passport-v1":
            return jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key_data))

    logger.warning(f"Llave no encontrada para provider={provider}, kid={kid}, iss={iss}")
    raise HTTPException(status_code=401, detail="Token irreconocible o llave no encontrada")


# ---------------------------------------------------------------------
# Sesión desde Redis (Laravel unified session)
# ---------------------------------------------------------------------

async def _load_unified_session(token_hash: str, token_type_hint: str) -> Optional[SessionDict]:
    """
    Busca sesión unificada en Redis probando prefijos comunes.
    token_type_hint típicamente 'local' (pero se prueban variantes).
    """
    try:
        redis = await _get_redis()

        candidate_keys = [
            f"laravel_database_session:{token_type_hint}:{token_hash}",
            f"laravel_database_session:local:{token_hash}",
            f"laravel_database_session:azure:{token_hash}",
            f"laravel_database_session:{token_hash}",
        ]

        pipe = redis.pipeline()
        for key in candidate_keys:
            pipe.get(key)

        results = await pipe.execute()

        for raw in results:
            if not raw:
                continue
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                continue

        return None

    except (RedisError, ConnectionError, TimeoutError) as e:
        logger.warning(f"Redis indisponible al cargar sesión: {e}")
        return None


# ---------------------------------------------------------------------
# Fallback: validación criptográfica + sesión mínima
# ---------------------------------------------------------------------

async def _fallback_validation(token: str) -> SessionDict:
    logger.info("Iniciando validación fallback (Criptográfica)")

    key = await _get_signing_key(token)
    try:
        payload = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            options={"verify_exp": True, "verify_aud": False},
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")

    iss = payload.get("iss", "")
    is_azure = "login.microsoftonline.com" in iss

    if is_azure:
        user_id_raw = payload.get("oid") or payload.get("sub")
        token_type = "azure"
        guid_ms = user_id_raw
        tenant_id = payload.get("tid", settings.AZURE_TENANT_ID)
    else:
        user_id_raw = str(payload.get("sub"))
        token_type = "local"
        guid_ms = None
        tenant_id = payload.get("tid", "default")

    session: SessionDict = {
        "user_id": str(user_id_raw),
        "tenant_id": tenant_id,
        "token_type": token_type,
        "guid_ms": guid_ms,
        "team_ids": [],
        "microservices_data": {},
        "_is_fallback": True,
    }

    # Intentar enriquecer permisos desde Arango (si existe el usuario)
    try:
        from src.core.database import db_instance
        from src.features.ocr_updates.pipeline.users_repository import USERS_COLLECTION

        key_check = guid_ms if guid_ms else str(user_id_raw)
        clean_key = sanitize_key(key_check)

        db = db_instance.get_db()
        if db.has_collection(USERS_COLLECTION):
            col = db.collection(USERS_COLLECTION)
            if col.has(clean_key):
                user_doc = col.get(clean_key)

                dms_perms = user_doc.get("dms_permissions", {})
                ms_id = settings.DMS_MICROSERVICE_ID

                teams_map = dms_perms.get("teams") or dms_perms.get("teams_data") or {}
                session["team_ids"] = list(teams_map.keys())

                session["microservices_data"] = {
                    "by_id": {
                        ms_id: {
                            "roles": dms_perms.get("roles", []),
                            "permissions": dms_perms.get("permissions", []),
                            "teams": teams_map,
                        }
                    }
                }

                logger.info(
                    f"✅ Fallback: Recuperados permisos locales para {clean_key} con {len(session['team_ids'])} equipos"
                )
            else:
                logger.warning(f"⚠️ Fallback: Usuario {clean_key} autenticado pero sin registro local.")
    except Exception as e:
        logger.error(f"Error leyendo DB local en fallback: {e}")

    return session


# ---------------------------------------------------------------------
# Public API: FastAPI dependency
# ---------------------------------------------------------------------

async def get_auth_context(
    request: Request,
    x_team_id: Optional[str] = Header(None, alias="X-Team-Id"),
) -> AuthContext:
    request_path = request.url.path
    request_method = request.method

    # 1) Extraer Bearer token
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        logger.warning(
            "❌ Auth header inválido o ausente | method=%s path=%s has_auth_header=%s auth_prefix=%s",
            request_method,
            request_path,
            bool(auth),
            auth[:20] if auth else "",
        )
        raise HTTPException(status_code=401, detail="Falta el token Bearer")

    token = auth[len("Bearer ") :].strip()
    if not token:
        logger.warning("❌ Token bearer vacío | method=%s path=%s", request_method, request_path)
        raise HTTPException(status_code=401, detail="Token vacío")

    token_hash = sha256_hex(token)
    logger.info(
        "🔐 Token recibido | method=%s path=%s token_hash_prefix=%s x_team_id=%s",
        request_method,
        request_path,
        token_hash[:12],
        x_team_id,
    )

    # 2) Redis session primero
    session = await _load_unified_session(token_hash, "local")

    # 3) Fallback si Redis no respondió o no hubo match
    if session is None:
        logger.warning(
            "⚠️ Sesión no encontrada en Redis, aplicando fallback JWT | path=%s token_hash_prefix=%s",
            request_path,
            token_hash[:12],
        )
        session = await _fallback_validation(token)

    user_id = str(session.get("user_id"))
    tenant_id = session.get("tenant_id", "default")
    token_type = session.get("token_type", "unknown")
    team_ids: List[str] = session.get("team_ids") or []
    microservices_data = session.get("microservices_data") or {}

    logger.info(
        "✅ AuthContext resuelto | path=%s user_id=%s token_type=%s teams=%s fallback=%s",
        request_path,
        user_id,
        token_type,
        len(team_ids),
        session.get("_is_fallback", False),
    )

    # 4) Validación opcional de team si viene header
    # if x_team_id and team_ids and x_team_id not in team_ids:
    #     raise HTTPException(status_code=403, detail="No tienes acceso a este equipo (X-Team-Id)")

    # 5) Sync async a Arango solo si viene de Redis (no fallback)
    if not session.get("_is_fallback", False):
        asyncio.create_task(_sync_user_to_db(user_id, session))

    return AuthContext(
        user_id=user_id,
        token_hash=token_hash,
        token_type=token_type,
        tenant_id=tenant_id,
        team_ids=team_ids,
        microservices_data=microservices_data,
    )


# ---------------------------------------------------------------------
# Sync user a ArangoDB
# ---------------------------------------------------------------------

async def _sync_user_to_db(user_id: str, session: SessionDict) -> None:
    try:
        from src.core.database import db_instance
        from src.features.ocr_updates.pipeline.users_repository import USERS_COLLECTION

        db = db_instance.get_db()
        if not db.has_collection(USERS_COLLECTION):
            return

        ms_id = settings.DMS_MICROSERVICE_ID
        microservices_data = session.get("microservices_data", {}) or {}
        local_data = session.get("local_data", {}) or {}
        user_data = (local_data.get("user_data", {}) or {})

        ms_data = {}
        by_id = microservices_data.get("by_id") or {}
        if ms_id in by_id:
            ms_data = by_id[ms_id] or {}

        guid_ms = user_data.get("azure_id") or user_data.get("guid_ms") or session.get("guid_ms")
        key = sanitize_key(str(guid_ms) if guid_ms else str(user_id))

        # Si es azure y no vino guid_ms explícito, el user_id suele ser oid
        if not guid_ms and session.get("token_type") == "azure":
            guid_ms = user_id

        if not key:
            return

        now_iso = datetime.now().isoformat()

        doc = {
            "_key": key,
            "user_id": user_id,
            "guid_ms": guid_ms,
            "name": user_data.get("name"),
            "email": user_data.get("email"),
            "first_login": user_data.get("first_login"),
            "last_synced_at": now_iso,
            "tenant_id": session.get("tenant_id"),
            "dms_permissions": {
                "roles": ms_data.get("roles", []),
                "permissions": ms_data.get("permissions", []),
                "teams": ms_data.get("teams", {}) or {},
            },
        }

        aql = f"""
        UPSERT {{ _key: @key }}
            INSERT MERGE(@doc, {{ created_at: @now, source: 'sync_auth_py' }})
            UPDATE MERGE(OLD, @doc, {{ updated_at: @now }})
        IN {USERS_COLLECTION}
        """

        db.aql.execute(aql, bind_vars={"key": key, "doc": doc, "now": now_iso})

    except Exception as e:
        logger.error(f"Error syncing user {user_id}: {e}")
