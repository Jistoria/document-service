from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from .models import DocumentDetail, DocumentListResponse, EntityRef
from .service import search_service

router = APIRouter(prefix="/documents", tags=["Search & Retrieval"])

@router.get("/", response_model=DocumentListResponse)
async def get_documents(
    page: int = 1,
    limit: int = 10,
    entity_id: Optional[str] = Query(None, description="Filtrar por ID de Facultad o Carrera"),
    status: Optional[str] = Query(None, description="Filtrar por estado (ej: attention_required, validated, confirmed)")
):
    """
    Lista documentos paginados.
    - Bandeja de Revisión: ?status=attention_required
    - Historial Validado: ?status=confirmed
    - Filtro por Carrera: ?entity_id=XXXX
    """
    return search_service.search_documents(page, limit, entity_id, status)

@router.get("/catalogs/entities", response_model=List[EntityRef])
async def get_search_filters():
    """
    Retorna las entidades (Carreras/Facultades) que TIENEN documentos.
    Útil para el dropdown de filtros.
    """
    return search_service.get_available_entities()

@router.get("/{doc_id}", response_model=DocumentDetail)
async def get_document_detail(doc_id: str):
    """
    Obtiene el detalle completo de un documento por su ID (Task ID),
    incluyendo sus relaciones de grafo.
    """
    doc = search_service.get_document_by_id(doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return doc