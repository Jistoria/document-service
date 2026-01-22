from fastapi import Depends, HTTPException
from typing import List
import redis.asyncio as aioredis
from src.core.security.auth import AuthContext, get_auth_context
from src.core.config import settings
import logging

logger = logging.getLogger(__name__)

# Mantenemos la lógica pura aquí (o puedes meterla dentro de la clase)
async def get_permitted_scopes_logic(permission: str, ctx: AuthContext) -> List[str]:
    ms_id = settings.DMS_MICROSERVICE_ID
    
    # Intentamos primero con Redis (Fuente original)
    try:
        redis = aioredis.from_url(settings.AUTH_REDIS_URL, decode_responses=True)
        # TODO: Mover esto a settings.REDIS_KEY_PREFIX si es posible
        REDIS_PREFIX = "laravel_database_"
        
        # 2. Check Teams
        allowed_teams = []
        user_teams = ctx.team_ids or []

        if not user_teams:
            return []

        pipe = redis.pipeline()
        checked_keys = []
        
        for team_id in user_teams:
            # AJUSTE CRÍTICO: PHP almacena el contexto 'global' (team_id=global o null)
            # en la clave SIN sufijo (perm:{t}:{ms}:{u}).
            # Los teams normales van en con sufijo (perm:{t}:{ms}:{u}:{team}).
            # Si el array team_ids incluye "global", debemos chequear la clave raíz.
            
            if team_id == "global":
                 key = f"{REDIS_PREFIX}perm:{ctx.tenant_id}:{ms_id}:{ctx.user_id}"
            else:
                 key = f"{REDIS_PREFIX}perm:{ctx.tenant_id}:{ms_id}:{ctx.user_id}:{team_id}"
            
            pipe.sismember(key, permission)
            checked_keys.append(key)

        results = await pipe.execute()
        
        for team_id, has_perm in zip(user_teams, results):
            if has_perm:
                if team_id == "global":
                    allowed_teams.append("*")
                else:
                    allowed_teams.append(team_id)

        return allowed_teams

    except Exception as e:
        logger.warning(f"⚠️ Redis permission check failed, switching to memory/fallback: {e}")
        return _check_permissions_in_memory(ctx, permission, ms_id)

def _check_permissions_in_memory(ctx: AuthContext, permission: str, ms_id: str) -> List[str]:
    """
    Fallback: Verifica permisos usando el JSON decodificado en AuthContext
    (que viene de Redis Session o de ArangoDB Fallback).
    """
    allowed_teams = []
    
    logger.info(f"Checking permissions in memory for permission '{permission}' and microservice '{ms_id}', user teams: {ctx.team_ids}")
    # Estructura: ctx.microservices_data['by_id'][ms_id]
    ms_data = ctx.microservices_data.get("by_id", {}).get(ms_id, {})
    if not ms_data:
        return []

    # Permisos Globales (equivale a team_id="global")
    global_perms = set(ms_data.get("permissions", []))
    
    # Permisos por Equipo
    teams_data = ms_data.get("teams", {})

    user_teams = ctx.team_ids or []
    
    for team_id in user_teams:
        has_perm = False
        
        if team_id == "global":
            # Si el usuario tiene asignado explícitamente "global", 
            # verifica contra la lista de permisos globales
            if permission in global_perms:
                has_perm = True
        else:
            # Verifica contra el diccionario de equipos
            t_data = teams_data.get(team_id, {})
            t_perms = t_data.get("permissions", [])
            if permission in t_perms:
                 has_perm = True
        
        if has_perm:
            if team_id == "global":
                allowed_teams.append("*")
            else:
                allowed_teams.append(team_id)
                
    logger.info(f"Fallback permissions result: {allowed_teams}")
    return allowed_teams




# --- ESTA ES LA CLASE MÁGICA ---
class RequirePermission:
    def __init__(self, permission: str):
        self.permission = permission

    async def __call__(self, ctx: AuthContext = Depends(get_auth_context)) -> List[str]:
        """
        Al ser async, FastAPI hace el 'await' automáticamente aquí
        y entrega la lista limpia al endpoint.
        """
        return await get_permitted_scopes_logic(self.permission, ctx)