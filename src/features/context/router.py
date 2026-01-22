from fastapi import APIRouter, Depends
from typing import List
from src.core.security.auth import get_auth_context, AuthContext
from src.core.database import db_instance
from logging import getLogger

logger = getLogger(__name__)

router = APIRouter(prefix="/me", tags=["User Context"])

@router.get("/entities")
async def get_my_entities(ctx: AuthContext = Depends(get_auth_context)):
    """
    Retorna las entidades (Carreras/Facultades) sobre las que el usuario tiene permisos,
    traduciendo los team_ids (CARR:123) a objetos completos desde ArangoDB.
    """
    db = db_instance.get_db()
    
    # 1. Obtenemos los equipos directamente del contexto (que viene de Redis o Cache local)
    team_ids = ctx.team_ids or [] 

    logger.info(f"Team IDs obtenidos del contexto: {team_ids}")

    if not team_ids:
        return {"success": True, "data": []}

    # 2. Reutilizamos la lógica centralizada
    from src.features.context.utils import resolve_team_codes
    
    entities = resolve_team_codes(db, team_ids, return_full_object=True)


    return {
        "success": True,
        "data": entities,
        "message": "Entidades recuperadas correctamente"
    }
