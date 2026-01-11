import asyncio
from fastapi import Query, Depends, HTTPException
from typing import Optional, List, Tuple
from logging import getLogger
from src.core.security.auth import AuthContext, get_auth_context
from src.core.security.permissions import get_permitted_scopes_logic

# Estados sensibles que requieren permisos de workflow
VERIFICATION_STATUSES = {"attention_required"}

logger = getLogger(__name__)

async def resolve_status_and_teams(
        status: Optional[str] = Query(None, description="Estado del documento. Si se omite, se asume 'validated'."),
        ctx: AuthContext = Depends(get_auth_context),
) -> Tuple[Optional[str], List[str]]:
    """
    Dependency Inteligente:
    1. Determina qué equipos (scopes) aplicar.
    2. Fuerza el estado 'validated' si el usuario no especifica uno.
    3. Protege estados sensibles verificando permisos de workflow.
    """

    # 1. Consultar TODOS los permisos necesarios en paralelo (Optimización)
    #    Esto hace 1 viaje a la red (si Redis pipeline está bien hecho) o 3 concurrentes.
    read_task = get_permitted_scopes_logic("dms.document.read", ctx)
    approve_task = get_permitted_scopes_logic("dms.workflow.approve", ctx)
    reject_task = get_permitted_scopes_logic("dms.workflow.reject", ctx)

    read_teams, approve_teams, reject_teams = await asyncio.gather(read_task, approve_task, reject_task)

    # 2. Lógica de "Smart Default" (Si no envían status)
    if not status:
        # Por defecto, mostramos lo validado para mantener la UI limpia
        status = "attention_required"

    # 3. Lógica de Protección de Estados Sensibles
    if status in VERIFICATION_STATUSES:
        # ¿Tiene permisos globales de workflow?
        if "*" in approve_teams or "*" in reject_teams:
            return status, ["*"]

        # Unimos los equipos donde puede aprobar O rechazar
        allowed_workflow_teams = sorted(set(approve_teams) | set(reject_teams))

        if not allowed_workflow_teams:
            raise HTTPException(
                status_code=403,
                detail=f"No tienes permisos de flujo de trabajo para ver documentos en estado '{status}'"
            )

        # Retornamos solo los equipos donde tiene poder de decisión
        return status, allowed_workflow_teams

    # 4. Lógica Estándar (Lectura)
    # Si pide 'validated', 'confirmed' o cualquier otro estado público
    if "*" in read_teams:
        return status, ["*"]

    if not read_teams:
        raise HTTPException(status_code=403, detail="No tienes permisos de lectura de documentos.")

    return status, read_teams