from fastapi import Depends, HTTPException
from typing import List
import redis.asyncio as aioredis
from src.core.security.auth import AuthContext, get_auth_context
from src.core.config import settings
import logging

logger = logging.getLogger(__name__)

# Mantenemos la lógica pura aquí (o puedes meterla dentro de la clase)
async def get_permitted_scopes_logic(permission: str, ctx: AuthContext) -> List[str]:
    # Definimos el prefijo que usa Laravel/Auth Service
    # TODO: Mover esto a settings.REDIS_KEY_PREFIX si es posible
    REDIS_PREFIX = "laravel_database_"

    redis = aioredis.from_url(settings.AUTH_REDIS_URL, decode_responses=True)
    ms_id = "a0c07d14-98ad-41b9-a3e6-4fc7c1a4d57c"

    # 1. Check Global
    # Nota: Aquí ya lo tenías, pero asegúrate de usar la variable para consistencia
    global_key = f"{REDIS_PREFIX}perm:{ctx.tenant_id}:{ms_id}:{ctx.user_id}:global"

    logger.info(f"Checking global key: {global_key}")

    if await redis.sismember(global_key, permission):
        return ["*"]

    # 2. Check Teams
    allowed_teams = []
    user_teams = ctx.team_ids or []  # O ctx.team_ids, como lo tengas definido

    if not user_teams:
        return []

    pipe = redis.pipeline()

    for team_id in user_teams:
        # CORRECCIÓN: Agregamos el prefijo aquí también
        key = f"{REDIS_PREFIX}perm:{ctx.tenant_id}:{ms_id}:{ctx.user_id}:{team_id}"
        pipe.sismember(key, permission)

    results = await pipe.execute()

    # Debug para verificar
    logger.info(f"Checking teams: {user_teams}")
    logger.info(f"Results raw: {results}")

    for team_id, has_perm in zip(user_teams, results):
        if has_perm:
            # Si el equipo es 'global', y tiene permiso, significa que tiene permiso en el scope global,
            # pero no necesariamente es admin total (*). Lo agregamos a la lista.
            allowed_teams.append(team_id)

    logger.info(f"Allowed teams: {allowed_teams}")

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