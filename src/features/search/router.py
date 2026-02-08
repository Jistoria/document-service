from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query

from src.core.security.auth import AuthContext, get_auth_context

from .dependencies import resolve_status_and_teams
from .models import DocumentDetailResponse, DocumentListAPIResponse, EntityListAPIResponse
from .service import search_service

router = APIRouter(prefix="/documents", tags=["Search & Retrieval"])


@dataclass
class DocumentSearchQueryParams:
    """Parámetros de búsqueda avanzados para documentos."""

    entity_id: Optional[str] = Query(
        None,
        description="Filtro Jerárquico: Busca en esta entidad y en sus hijas.",
    )
    process_id: Optional[str] = Query(
        None,
        description="Filtro Jerárquico: Busca por Proceso, Categoría o Documento Requerido.",
    )
    status: Optional[str] = Query(
        None,
        description="Filtrar por estado (ej: attention_required, validated, confirmed).",
    )
    search: Optional[str] = Query(
        None,
        description="Búsqueda parcial por nombre mostrado u nombre original del archivo.",
    )
    required_document_id: Optional[str] = Query(
        None,
        description="Filtrar documentos cuya arista complies_with apunta al ID indicado.",
    )
    referenced_entity_id: Optional[str] = Query(
        None,
        description="Filtrar documentos cuya arista references apunta a la entidad indicada.",
    )
    schema_id: Optional[str] = Query(
        None,
        description="Filtrar documentos cuya arista usa_esquema apunta al esquema indicado.",
    )
    date_from: Optional[date] = Query(
        None,
        description="Fecha inicial (inclusive) para filtrar por doc.created_at.",
    )
    date_to: Optional[date] = Query(
        None,
        description="Fecha final (inclusive) para filtrar por doc.created_at.",
    )
    owner_id: Optional[str] = Query(
        None,
        description="Filtrar por ID de propietario/cargador del documento (doc.owner.id).",
    )


@router.get("/", response_model=DocumentListAPIResponse)
async def get_documents(
    page: int = 1,
    limit: int = 10,
    params: DocumentSearchQueryParams = Depends(),
    search_context: Tuple[Optional[str], List[str]] = Depends(resolve_status_and_teams),
    ctx: AuthContext = Depends(get_auth_context),
):
    """Lista documentos paginados con formato estándar y filtros avanzados."""
    resolved_status, allowed_teams = search_context

    return search_service.search_documents(
        page=page,
        page_size=limit,
        entity_id=params.entity_id,
        process_id=params.process_id,
        status=resolved_status,
        allowed_teams=allowed_teams,
        current_user_id=ctx.user_id,
        search=params.search,
        required_document_id=params.required_document_id,
        referenced_entity_id=params.referenced_entity_id,
        schema_id=params.schema_id,
        date_from=params.date_from,
        date_to=params.date_to,
        owner_id=params.owner_id,
    )


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

    if not result["success"]:
        raise HTTPException(status_code=404, detail=result["message"])

    return result
