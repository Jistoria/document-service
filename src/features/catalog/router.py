from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Query, Path, HTTPException
from src.core.responses import wrap_response, StandardResponse
from .service import catalog_service
from .models import (
    CatalogItem, CareerItem, SubsystemItem, CategoryItem,
    ProcessItem, ProcessDetail, RequiredDocumentItem
)

# Router público (sin dependencias de seguridad por defecto si se monta así)
router = APIRouter(prefix="/catalog", tags=["Public Catalog"])

# ========== RUTAS DE ESTRUCTURA ORGANIZACIONAL ==========

@router.get("/faculties", response_model=StandardResponse[List[CatalogItem]])
async def list_faculties(
    search: Optional[str] = Query(None, description="Búsqueda por nombre o código (ignora mayúsculas/minúsculas)")
):
    """
    Obtiene la lista de todas las facultades.
    Soporta búsqueda case-insensitive por nombre o código.
    """
    data = await catalog_service.get_faculties(search)
    return wrap_response(data, message="Facultades obtenidas exitosamente")

@router.get("/careers", response_model=StandardResponse[List[CareerItem]])
async def list_careers(
    faculty_id: Optional[str] = Query(None, description="Filtrar por ID de facultad"),
    search: Optional[str] = Query(None, description="Búsqueda por nombre o código (ignora mayúsculas/minúsculas)")
):
    """
    Obtiene la lista de carreras. Puede filtrarse por facultad y/o búsqueda.
    Soporta búsqueda case-insensitive por nombre o código.
    """
    data = await catalog_service.get_careers(faculty_id, search)
    return wrap_response(data, message="Carreras obtenidas exitosamente")

# ========== RUTAS DE CATÁLOGO DE PROCESOS ==========

@router.get("/processes/tree", response_model=StandardResponse[List[Dict[str, Any]]])
async def get_process_tree():
    """
    Obtiene el árbol jerárquico completo de procesos incluyendo subprocesos:
    Subsistemas -> Categorías -> Procesos -> Subprocesos (recursivo)
    Ideal para menús de navegación o selectores en cascada.
    """
    data = await catalog_service.get_process_tree()
    return wrap_response(data, message="Árbol de procesos obtenido exitosamente")

@router.get("/subsystems", response_model=StandardResponse[List[SubsystemItem]])
async def list_subsystems(
    search: Optional[str] = Query(None, description="Búsqueda por nombre o código (ignora mayúsculas/minúsculas)")
):
    """
    Obtiene la lista de todos los subsistemas del catálogo.
    Útil para menús desplegables de primer nivel.
    Soporta búsqueda case-insensitive por nombre o código.
    """
    data = await catalog_service.get_subsystems(search)
    return wrap_response(data, message="Subsistemas obtenidos exitosamente")

@router.get("/categories", response_model=StandardResponse[List[CategoryItem]])
async def list_categories(
    subsystem_id: Optional[str] = Query(None, description="Filtrar por ID de subsistema"),
    search: Optional[str] = Query(None, description="Búsqueda por nombre o código (ignora mayúsculas/minúsculas)")
):
    """
    Obtiene la lista de categorías de procesos.
    Puede filtrarse por subsistema y/o búsqueda para menús en cascada.
    Soporta búsqueda case-insensitive por nombre o código.
    """
    data = await catalog_service.get_categories(subsystem_id, search)
    return wrap_response(data, message="Categorías obtenidas exitosamente")

@router.get("/processes", response_model=StandardResponse[List[ProcessItem]])
async def list_processes(
    category_id: Optional[str] = Query(None, description="Filtrar procesos raíz de una categoría"),
    parent_process_id: Optional[str] = Query(None, description="Filtrar subprocesos de un proceso"),
    search: Optional[str] = Query(None, description="Búsqueda por nombre o código (ignora mayúsculas/minúsculas)")
):
    """
    Obtiene la lista de procesos.
    - Sin filtros: retorna todos los procesos raíz
    - Con category_id: retorna procesos raíz de esa categoría
    - Con parent_process_id: retorna subprocesos de ese proceso
    Soporta búsqueda case-insensitive por nombre o código.
    """
    data = await catalog_service.get_processes(category_id, parent_process_id, search)
    return wrap_response(data, message="Procesos obtenidos exitosamente")

@router.get("/processes/{process_id}", response_model=StandardResponse[ProcessDetail])
async def get_process_detail(
    process_id: str = Path(..., description="ID del proceso")
):
    """
    Obtiene el detalle completo de un proceso específico, incluyendo:
    - Subprocesos (recursivo)
    - Documentos requeridos
    Ideal para mostrar vista detallada o formularios de un proceso.
    """
    data = await catalog_service.get_process_detail(process_id)
    if not data:
        raise HTTPException(status_code=404, detail="Proceso no encontrado")
    return wrap_response(data, message="Detalle de proceso obtenido exitosamente")

@router.get("/required-documents", response_model=StandardResponse[List[RequiredDocumentItem]])
async def list_required_documents(
    process_id: Optional[str] = Query(None, description="Filtrar por ID de proceso"),
    subsystem_id: Optional[str] = Query(None, description="Filtrar por ID de subsistema (trae todos los docs del subsistema)"),
    search: Optional[str] = Query(None, description="Búsqueda por nombre o código (ignora mayúsculas/minúsculas)")
):
    """
    Obtiene la lista de documentos requeridos.
    - Con process_id: documentos de un proceso específico
    - Con subsystem_id: todos los documentos de todos los procesos del subsistema
    - Sin filtros: todos los documentos requeridos del sistema
    
    Los documentos incluyen información de ancestros (subsistema, categoría) 
    para facilitar filtrado y agrupación en la UI.
    Soporta búsqueda case-insensitive por nombre o código.
    """
    data = await catalog_service.get_required_documents(process_id, subsystem_id, search)
    return wrap_response(data, message="Documentos requeridos obtenidos exitosamente")

# ========== RUTA LEGACY (mantener por compatibilidad) ==========

@router.get("/processes/{process_id}/required-documents", 
            response_model=StandardResponse[List[RequiredDocumentItem]],
            deprecated=True)
async def list_process_documents(process_id: str):
    """
    [DEPRECADO] Use /required-documents?process_id={id} en su lugar.
    
    Obtiene la lista de documentos requeridos configurados para un proceso específico.
    """
    data = await catalog_service.get_required_documents(process_id=process_id)
    return wrap_response(data, message="Documentos requeridos obtenidos exitosamente")
