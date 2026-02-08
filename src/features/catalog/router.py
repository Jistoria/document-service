from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Query
from src.core.responses import wrap_response, StandardResponse
from .service import catalog_service
from .models import CatalogItem, CareerItem

# Router público (sin dependencias de seguridad por defecto si se monta así)
router = APIRouter(prefix="/catalog", tags=["Public Catalog"])

@router.get("/faculties", response_model=StandardResponse[List[CatalogItem]])
async def list_faculties():
    """
    Obtiene la lista de todas las facultades.
    """
    data = await catalog_service.get_faculties()
    return wrap_response(data, message="Facultades obtenidas exitosamente")

@router.get("/careers", response_model=StandardResponse[List[CareerItem]])
async def list_careers(
    faculty_id: Optional[str] = Query(None, description="Filtrar por ID de facultad")
):
    """
    Obtiene la lista de carreras. Puede filtrarse por facultad.
    """
    data = await catalog_service.get_careers(faculty_id)
    return wrap_response(data, message="Carreras obtenidas exitosamente")

@router.get("/processes/tree", response_model=StandardResponse[List[Dict[str, Any]]])
async def get_process_tree():
    """
    Obtiene el árbol jerárquico completo de procesos:
    Subsistemas -> Categorías -> Procesos
    Ideal para menús de navegación o selectores en cascada.
    """
    data = await catalog_service.get_process_tree()
    return wrap_response(data, message="Árbol de procesos obtenido exitosamente")

@router.get("/processes/{process_id}/required-documents", response_model=StandardResponse[List[CatalogItem]])
async def list_process_documents(process_id: str):
    """
    Obtiene la lista de documentos requeridos configurados para un proceso específico.
    """
    data = await catalog_service.get_required_documents(process_id)
    return wrap_response(data, message="Documentos requeridos obtenidos exitosamente")
