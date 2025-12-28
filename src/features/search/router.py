from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from .models import DocumentDetailResponse, DocumentListAPIResponse, EntityListAPIResponse
from .service import search_service

router = APIRouter(prefix="/documents", tags=["Search & Retrieval"])


@router.get("/", response_model=DocumentListAPIResponse)
async def get_documents(
        page: int = 1,
        limit: int = 10,
        entity_id: Optional[str] = Query(None, description="Filtro Jerárquico: Busca en esta entidad Y en sus hijas (Ej: Filtrar por Facultad trae documentos de sus Carreras)."),
        process_id: Optional[str] = Query(None, description="Filtro Jerárquico: Busca por Proceso, Categoría o Documento Requerido."),
        status: Optional[str] = Query(None,
                                      description="Filtrar por estado (ej: attention_required, validated, confirmed)")
):
    """
    Lista documentos paginados con formato estándar.
    Permite filtrar por estado y ubicación (Entidad).
    """
    return search_service.search_documents(page, limit, entity_id, process_id, status)


@router.get("/catalogs/entities", response_model=EntityListAPIResponse)
async def get_search_filters():
    """
    Retorna las entidades (Carreras/Facultades) que TIENEN documentos almacenados.
    Útil para llenar los filtros en el Frontend.
    """
    return search_service.get_available_entities()


@router.get("/{doc_id}", response_model=DocumentDetailResponse)
async def get_document_detail(doc_id: str):
    """
    Obtiene el detalle completo de un documento por su ID (Task ID),
    incluyendo sus metadatos, naming, storage y relaciones del grafo.
    """
    result = search_service.get_document_by_id(doc_id)

    # Manejamos el 404 explícitamente si el servicio indica fallo
    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["message"])

    return result