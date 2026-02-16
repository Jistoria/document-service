import logging
from datetime import date
from typing import Any, Dict, List, Optional

from arango.exceptions import ArangoError

from src.core.database import db_instance
from src.features.search.dependencies import VERIFICATION_STATUSES

from .repository import SearchRepository
from .response_builder import ResponseBuilder

logger = logging.getLogger(__name__)


class SearchService:
    """Servicio principal para búsqueda y recuperación de documentos."""

    def __init__(self):
        self.repository = SearchRepository()

    def get_db(self):
        """Obtiene la instancia de base de datos."""
        return db_instance.get_db()

    def get_document_by_id(self, doc_id: str):
        """Obtiene un documento por su ID incluyendo todas sus relaciones."""
        result = self.repository.get_by_id(doc_id)

        if result:
            return ResponseBuilder.build_document_detail_response(result)

        return ResponseBuilder.error_response(
            message="El documento no existe o no fue encontrado."
        )

    def search_documents(
        self,
        page: int = 1,
        page_size: int = 10,
        entity_id: Optional[str] = None,
        process_id: Optional[str] = None,
        status: Optional[str] = None,
        allowed_teams: Optional[List[str]] = None,
        current_user_id: Optional[str] = None,
        search: Optional[str] = None,
        required_document_id: Optional[str] = None,
        referenced_entity_id: Optional[str] = None,
        schema_id: Optional[str] = None,
        date_from: Optional[date] = None,
        date_to: Optional[date] = None,
        owner_id: Optional[str] = None,
        metadata_filters: Optional[Dict[str, Any]] = None,
        fuzziness: Optional[int] = None,
    ):
        """Busca documentos con filtros dinámicos y paginación."""
        try:
            db = self.get_db()

            valid_owner_ids = self._validate_permissions(db, allowed_teams)
            if valid_owner_ids is None:
                return ResponseBuilder.build_empty_list_response(
                    page,
                    page_size,
                    message="No tienes permisos sobre ninguna entidad válida.",
                )

            filters = {
                "valid_owner_ids": valid_owner_ids,
                "status": status,
                "entity_id": entity_id,
                "process_id": process_id,
                "search": search,
                "required_document_id": required_document_id,
                "referenced_entity_id": referenced_entity_id,
                "schema_id": schema_id,
                "date_from": date_from,
                "date_to": date_to,
                "owner_id": owner_id,
                "metadata_filters": metadata_filters or {},
                "fuzziness": fuzziness,
            }

            if status in VERIFICATION_STATUSES and current_user_id:
                filters["current_user_id"] = current_user_id

            offset = (page - 1) * page_size
            query_result = self.repository.search(offset=offset, limit=page_size, filters=filters)

            if not query_result:
                return ResponseBuilder.build_empty_list_response(page, page_size)

            return ResponseBuilder.build_paginated_response(
                items_data=query_result.get("items", []),
                total_items=query_result.get("total", 0),
                page=page,
                page_size=page_size,
            )

        except ArangoError as e:
            logger.error("Error AQL / ArangoDB en search_documents", exc_info=True)
            return ResponseBuilder.error_response(message=f"Error en base de datos: {str(e)}")
        except Exception:
            logger.error("Error inesperado en search_documents", exc_info=True)
            return ResponseBuilder.error_response(
                message="Error interno al procesar la búsqueda."
            )

    def get_available_entities(self):
        """Retorna las entities (Carreras/Facultades) que TIENEN documentos asociados."""
        entities_data = self.repository.get_entities_with_docs()
        return ResponseBuilder.build_entities_response(entities_data)

    def _validate_permissions(
        self,
        db,
        allowed_teams: Optional[List[str]],
    ) -> Optional[List[str]]:
        """Valida y resuelve los permisos del usuario a UUIDs de entidades."""
        if allowed_teams and "*" not in allowed_teams:
            valid_owner_ids = self._resolve_team_codes_to_uuids(db, allowed_teams)
            if not valid_owner_ids:
                return None
            return valid_owner_ids

        return []

    def _resolve_team_codes_to_uuids(self, db, allowed_teams: List[str]) -> List[str]:
        """Traduce códigos de permisos a _key UUID de entidades."""
        from src.features.context.utils import resolve_team_codes

        return resolve_team_codes(db, allowed_teams, return_full_object=False)


search_service = SearchService()
