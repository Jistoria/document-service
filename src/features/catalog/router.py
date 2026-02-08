from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Query
from .service import catalog_service
from .models import CatalogItem, CareerItem

# Router público (sin dependencias de seguridad por defecto si se monta así)
router = APIRouter(prefix="/catalog", tags=["Public Catalog"])

@router.get("/faculties", response_model=List[CatalogItem])
async def list_faculties():
    """
    Obtiene la lista de todas las facultades.
    """
    return await catalog_service.get_faculties()

@router.get("/careers", response_model=List[CareerItem])
async def list_careers(
    faculty_id: Optional[str] = Query(None, description="Filtrar por ID de facultad")
):
    """
    Obtiene la lista de carreras. Puede filtrarse por facultad.
    """
    return await catalog_service.get_careers(faculty_id)

@router.get("/processes/tree", response_model=List[Dict[str, Any]])
async def get_process_tree():
    """
    Obtiene el árbol jerárquico completo de procesos:
    Subsistemas -> Categorías -> Procesos
    Ideal para menús de navegación o selectores en cascada.
    """
    return await catalog_service.get_process_tree()

@router.get("/processes/{process_id}/required-documents", response_model=List[CatalogItem])
async def list_process_documents(process_id: str):
    """
    Obtiene la lista de documentos requeridos configurados para un proceso específico.
    """
    return await catalog_service.get_required_documents(process_id)
