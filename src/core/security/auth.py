# src/core/security/auth.py
import hashlib
import json
import os
import re
import time
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import httpx
import jwt
from jwt import PyJWKClient

from fastapi import Depends, Header, HTTPException, Request
import redis.asyncio as aioredis
from redis.exceptions import RedisError, ConnectionError, TimeoutError
from src.core.config import settings

logger = logging.getLogger(__name__)

# --- Cache Multi-Tenant para JWKS ---
# Mapea URL -> { keys: list, expires_at: timestamp }
_JWKS_CACHE_MAP = {}

# Ajusta según tu variable de entorno.
REDIS_URL: str = os.getenv("AUTH_REDIS_URL", settings.AUTH_REDIS_URL)

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
    Busca la sesión unificada en Redis.
    """
    try:
        redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        # Intentamos adivinar la key correcta probando prefijos comunes
        candidate_keys = [
            f"laravel_database_session:{token_type}:{token_hash}",
            f"laravel_database_session:local:{token_hash}",
            f"laravel_database_session:azure:{token_hash}",
            f"laravel_database_session:{token_hash}", 
        ]
        
        pipe = redis.pipeline()
        for key in candidate_keys:
            pipe.get(key)
        results = await pipe.execute()

        for raw in results:
            if raw:
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    continue
        return None
    except (RedisError, ConnectionError, TimeoutError) as e:
        logger.warning(f"Redis indisponible al cargar sesión: {e}")
        return None

async def _get_jwks_data(jwks_url: str) -> dict:
    """
    Gestiona la descarga y caché de las llaves públicas para una URL dada.
    """
    global _JWKS_CACHE_MAP
    now = time.time()
    
    cache_entry = _JWKS_CACHE_MAP.get(jwks_url)
    
    # Si no existe o expiró (TTL 1 hora)
    if not cache_entry or now > cache_entry["expires_at"]:
        try:
            logger.info(f"🔄 Refrescando JWKS desde {jwks_url}")
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(jwks_url)
                resp.raise_for_status()
                keys_data = resp.json()
                
                _JWKS_CACHE_MAP[jwks_url] = {
                    "keys": keys_data,
                    "expires_at": now + 3600 # 1 hora
                }
                return keys_data
        except Exception as e:
            logger.error(f"Error descargando JWKS de {jwks_url}: {e}")
            # Si tenemos caché vieja, la devolvemos como último recurso
            if cache_entry:
                return cache_entry["keys"]
            raise HTTPException(status_code=503, detail="No se pudieron obtener las llaves de validación.")
            
    return cache_entry["keys"]

async def _get_signing_key(token: str):
    """
    Determina el proveedor (Microsoft vs Local) e identifica la llave pública correcta.
    """
    try:
        # 1. Decodificar sin verificar para detectar issuer y key-id (kid)
        unverified_header = jwt.get_unverified_header(token)
        unverified_payload = jwt.decode(token, options={"verify_signature": False})
        
        kid = unverified_header.get("kid")
        iss = unverified_payload.get("iss", "")
        
        # 2. Seleccionar URL de JWKS según el issuer
        if "login.microsoftonline.com" in iss:
            # Es token de Azure AD
            tenant_id = settings.AZURE_TENANT_ID 
            jwks_url = f"https://login.microsoftonline.com/{tenant_id}/discovery/v2.0/keys"
        else:
            # Es token Local (Passport)
            jwks_url = settings.AUTH_JWKS_URL
            # Si el kid no viene en el header (típico de algunos tokens Passport legacy), 
            # forzamos el kid estándar si no existe.
            if not kid:
                kid = "passport-v1" 

        # 3. Obtener llaves (con cache)
        jwks_data = await _get_jwks_data(jwks_url)
        
        # 4. Encontrar la llave correcta
        for key_data in jwks_data.get("keys", []):
            # En proveedores locales a veces la key no tiene 'kid' explícito si solo hay una
            key_kid = key_data.get("kid")
            
            # Match exacto de KIDs
            if key_kid and key_kid == kid:
                 return jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key_data))
            
            # Fallback para local: si el token pide 'passport-v1' y la key en JWKS no tiene kid 
            # pero es la única o estamos en modo local, probamos esa.
            if not key_kid and kid == "passport-v1":
                 # Asumimos que esta es la llave correcta si no hay discriminador
                 return jwt.algorithms.RSAAlgorithm.from_jwk(json.dumps(key_data))
                
        # Si llegamos aquí y es Azure, es error fatal.
        # Si es local, logs detallados.
        raise Exception(f"Llave con kid={kid} no encontrada en JWKS de {iss}")

    except HTTPException:
        raise
    except Exception as e:
        logger.warning(f"Error obteniendo signing key: {e}")
        raise HTTPException(status_code=401, detail="Token irreconocible o llave no encontrada")

async def _fallback_validation(token: str) -> Dict[str, Any]:
    """
    Valida criptográficamente el token (Microsoft o Local) y reconstruye sessión mínima.
    """
    logger.info("Iniciando validación fallback (Criptográfica)")
    
    # 1. Validar firma JWT
    key = await _get_signing_key(token)
    try:
        # Validamos claims estándar
        payload = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            options={"verify_exp": True, "verify_aud": False} 
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")

    # 2. Normalizar Identidad según proveedor
    iss = payload.get("iss", "")
    
    is_azure = "login.microsoftonline.com" in iss
    
    if is_azure:
        # En Azure v2, 'oid' es el ID inmutable del objeto usuario
        user_id_raw = payload.get("oid") or payload.get("sub")
        token_type = "azure"
        guid_ms = user_id_raw # Para Azure, este es el GUID maestro
        
        # El tenant suele venir en 'tid'
        tenant_id = payload.get("tid", settings.AZURE_TENANT_ID)
    else:
        # Local
        user_id_raw = str(payload.get("sub"))
        token_type = "local"
        # En local, no asumimos que tenemos guid_ms en el token a menos que venga en custom claims
        guid_ms = None 
        tenant_id = payload.get("tid", "default")

    # 3. Datos básicos de sesión
    session = {
        "user_id": user_id_raw,
        "tenant_id": tenant_id,
        "token_type": token_type,
        "guid_ms": guid_ms,
        "team_ids": [],
        "microservices_data": {},
        "_is_fallback": True 
    }

    # 4. Busqueda en DB Local para permisos (ArangoDB)
    try:
        from src.core.database import db_instance
        from src.features.ocr_updates.pipeline.users_repository import USERS_COLLECTION
        from src.core.config import settings
        
        # Debemos calcular la _key con la que guardamos al usuario
        # Usamos la misma lógica de sanitización que el sync
        key_check = guid_ms if guid_ms else user_id_raw
        clean_key = re.sub(r"[^a-z0-9_]", "", key_check.strip().lower().replace("-", ""))
        
        db = db_instance.get_db()
        if db.has_collection(USERS_COLLECTION):
            col = db.collection(USERS_COLLECTION)
            if col.has(clean_key):
                user_doc = col.get(clean_key)
                
                # Recuperar permisos guardados
                dms_perms = user_doc.get("dms_permissions", {})
                ms_id = settings.DMS_MICROSERVICE_ID
                
                # Normalizamos 'teams' vs 'teams_data'
                teams_map = dms_perms.get("teams", {})
                if not teams_map:
                    teams_map = dms_perms.get("teams_data", {})
                
                # FIX CRÍTICO: Poblar team_ids desde las claves del mapa de equipos
                session["team_ids"] = list(teams_map.keys())

                session["microservices_data"] = {
                    "by_id": {
                        ms_id: {
                            "roles": dms_perms.get("roles", []),
                            "permissions": dms_perms.get("permissions", []),
                            "teams": teams_map
                        }
                    }
                }
                logger.info(f"✅ Fallback: Recuperados permisos locales para {clean_key} con {len(session['team_ids'])} equipos")
            else:
                logger.warning(f"⚠️ Fallback: Usuario {clean_key} autenticado pero sin registro local.")
    except Exception as e:
        logger.error(f"Error leyendo DB local en fallback: {e}")
    
    return session

async def get_auth_context(
    request: Request,
    x_team_id: str = Header(None, alias="X-Team-Id"),
) -> AuthContext:
    # 1. Obtener Token
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Falta el token Bearer")
    token = auth[len("Bearer "):].strip()
    
    if not token:
         raise HTTPException(status_code=401, detail="Token vacío")

    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()

    # 2. Intentar Cache Redis
    # Probamos primero como local, si falla la busqueda interna probará otros prefijos
    session = await _load_unified_session(token_hash, "local") 
    
    # 3. Si falla Redis, ir a Fallback
    if session is None:
        try:
            session = await _fallback_validation(token)
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Fallo crítico en auth: {e}")
            raise HTTPException(status_code=401, detail="Error de autenticación del sistema")

    user_id = str(session.get("user_id"))
    tenant_id = session.get("tenant_id", "default")
    team_ids: List[str] = session.get("team_ids") or []
    microservices_data = session.get("microservices_data") or {}

    # 4. Sync Async a ArangoDB (Solo si la sesión viene fresca de Redis/PHP)
    if not session.get("_is_fallback", False):
        import asyncio
        asyncio.create_task(_sync_user_to_db(user_id, session))

    return AuthContext(
        user_id=user_id,
        token_hash=token_hash,
        token_type=session.get("token_type", "unknown"),
        tenant_id=tenant_id,
        team_ids=team_ids,
        microservices_data=microservices_data,
    )

async def _sync_user_to_db(user_id: str, session: Dict[str, Any]):
    """
    Sincroniza los datos del usuario + permisos en ArangoDB.
    """
    try:
        from src.features.ocr_updates.pipeline.users_repository import USERS_COLLECTION
        from src.core.database import db_instance
        from src.core.config import settings

        db = db_instance.get_db()
        if not db.has_collection(USERS_COLLECTION):
            return

        ms_id = settings.DMS_MICROSERVICE_ID 
        microservices_data = session.get("microservices_data", {})
        local_data = session.get("local_data", {})
        user_data = local_data.get("user_data", {})
        
        ms_data = {}
        if "by_id" in microservices_data and ms_id in microservices_data["by_id"]:
            ms_data = microservices_data["by_id"][ms_id]

        # Prioridad para GUID MS (Azure ID o GUID interno)
        guid_ms = user_data.get("azure_id") or user_data.get("guid_ms") or session.get("guid_ms")
        
        # Sanitización estricta para _key
        def sanitize_key(val: str) -> str:
            if not val: return ""
            k = val.strip().lower().replace("-", "")
            return re.sub(r"[^a-z0-9_]", "", k)

        if guid_ms:
            key = sanitize_key(guid_ms)
        else:
            # Fallback a user_id para usuarios puramente locales/sistema
            key = sanitize_key(user_id)
            if not guid_ms and session.get("token_type") == "azure":
                 # Si es session azure pero no vino guid explícito, el user_id SUELE ser el oid
                 guid_ms = user_id
            
        if not key:
            return

        doc = {
            "_key": key,
            "user_id": user_id, 
            "guid_ms": guid_ms,
            "name": user_data.get("name"),
            "email": user_data.get("email"),
            "first_login": user_data.get("first_login"),
            "last_synced_at": datetime.now().isoformat(),
            "tenant_id": session.get("tenant_id"),
            "dms_permissions": {
                "roles": ms_data.get("roles", []),
                "permissions": ms_data.get("permissions", []),
                "teams": ms_data.get("teams", {}) 
            }
        }

        aql = f"""
        UPSERT {{ _key: @key }}
            INSERT MERGE(@doc, {{ created_at: @now, source: 'sync_auth_py' }})
            UPDATE MERGE(OLD, @doc, {{ updated_at: @now }})
        IN {USERS_COLLECTION}
        """
        
        db.aql.execute(aql, bind_vars={
            "key": key,
            "doc": doc,
            "now": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error syncing user {user_id}: {e}")
