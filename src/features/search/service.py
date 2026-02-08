import logging
from typing import Optional, List
from arango.exceptions import ArangoError

from src.core.database import db_instance
from src.features.search.dependencies import VERIFICATION_STATUSES

from .queries import SearchQueries
from .filters import SearchFilters
from .response_builder import ResponseBuilder

logger = logging.getLogger(__name__)


class SearchService:
    """Servicio principal para búsqueda y recuperación de documentos."""

    def get_db(self):
        """Obtiene la instancia de base de datos."""
        return db_instance.get_db()

    def get_document_by_id(self, doc_id: str):
        """
        Obtiene un documento por su ID incluyendo todas sus relaciones.
        
        Args:
            doc_id: ID del documento a buscar
            
        Returns:
            dict: Respuesta estandarizada con el documento o error
        """
        db = self.get_db()
        
        # Ejecutar query
        aql = SearchQueries.get_document_by_id_query()
        cursor = db.aql.execute(aql, bind_vars={"doc_id": doc_id})
        result = list(cursor)

        if result:
            return ResponseBuilder.build_document_detail_response(result[0])
        else:
            return ResponseBuilder.error_response(
                message="El documento no existe o no fue encontrado."
            )

    def search_documents(
            self,
            page: int = 1,
            page_size: int = 10,
            entity_id: str = None,
            process_id: str = None,
            status: str = None,
            allowed_teams: List[str] = None,
            current_user_id: str = None
    ):
        """
        Busca documentos con filtros dinámicos y paginación.
        
        Args:
            page: Número de página (1-indexed)
            page_size: Cantidad de items por página
            entity_id: ID de entidad para filtrar
            process_id: ID de proceso para filtrar
            status: Estado del documento para filtrar
            allowed_teams: Lista de códigos de equipos permitidos
            current_user_id: ID del usuario actual (para filtros de verificación)
            
        Returns:
            dict: Respuesta estandarizada con lista paginada de documentos
        """
        try:
            db = self.get_db()

            # --- Validación de Permisos ---
            valid_owner_ids = self._validate_permissions(
                db, allowed_teams, page, page_size
            )
            
            # Si no hay permisos válidos, retornar respuesta vacía
            if valid_owner_ids is None:
                return ResponseBuilder.build_empty_list_response(
                    page, page_size,
                    message="No tienes permisos sobre ninguna entidad válida."
                )

            # --- Construcción de Filtros ---
            filter_builder = self._build_filters(
                allowed_teams=allowed_teams,
                valid_owner_ids=valid_owner_ids,
                status=status,
                current_user_id=current_user_id,
                entity_id=entity_id,
                process_id=process_id,
                page=page,
                page_size=page_size
            )

            # --- Ejecución de Query ---
            query_result = self._execute_search_query(
                db, 
                filter_builder.build_filter_clause(),
                filter_builder.get_bind_vars()
            )

            # Si no hay resultados, retornar lista vacía
            if not query_result:
                return ResponseBuilder.build_empty_list_response(page, page_size)

            # --- Construcción de Respuesta ---
            return ResponseBuilder.build_paginated_response(
                items_data=query_result.get("items", []),
                total_items=query_result.get("total", 0),
                page=page,
                page_size=page_size
            )

        except ArangoError as e:
            logger.error("Error AQL / ArangoDB en search_documents", exc_info=True)
            return ResponseBuilder.error_response(
                message=f"Error en base de datos: {str(e)}"
            )

        except Exception as e:
            logger.error("Error inesperado en search_documents", exc_info=True)
            return ResponseBuilder.error_response(
                message="Error interno al procesar la búsqueda."
            )

    def get_available_entities(self):
        """
        Retorna las entities (Carreras/Facultades) que TIENEN documentos asociados.
        
        Returns:
            dict: Respuesta estandarizada con lista de entidades
        """
        db = self.get_db()
        aql = SearchQueries.get_entities_with_documents_query()
        cursor = db.aql.execute(aql)
        entities_data = list(cursor)
        
        return ResponseBuilder.build_entities_response(entities_data)

    # ===== MÉTODOS PRIVADOS DE AYUDA =====

    def _validate_permissions(
        self, 
        db, 
        allowed_teams: Optional[List[str]], 
        page: int, 
        page_size: int
    ) -> Optional[List[str]]:
        """
        Valida y resuelve los permisos del usuario a UUIDs de entidades.
        
        Args:
            db: Instancia de base de datos
            allowed_teams: Lista de códigos de equipos permitidos
            page: Número de página (para respuesta vacía si falla)
            page_size: Tamaño de página (para respuesta vacía si falla)
            
        Returns:
            List[str] | None: Lista de UUIDs válidos o None si no hay permisos
        """
        if allowed_teams and "*" not in allowed_teams:
            valid_owner_ids = self._resolve_team_codes_to_uuids(db, allowed_teams)

            # Si tenía permisos en Redis (ej: 'CARR:999') pero esa carrera
            # NO existe en Arango, valid_owner_ids estará vacío.
            # En ese caso, debemos bloquear el acceso (Fail Safe).
            if not valid_owner_ids:
                return None
            
            return valid_owner_ids
        
        return []

    def _build_filters(
        self,
        allowed_teams: Optional[List[str]],
        valid_owner_ids: List[str],
        status: Optional[str],
        current_user_id: Optional[str],
        entity_id: Optional[str],
        process_id: Optional[str],
        page: int,
        page_size: int
    ) -> SearchFilters:
        """
        Construye todos los filtros de búsqueda.
        
        Args:
            allowed_teams: Lista de equipos permitidos
            valid_owner_ids: UUIDs de entidades válidas
            status: Estado a filtrar
            current_user_id: ID del usuario actual
            entity_id: ID de entidad a filtrar
            process_id: ID de proceso a filtrar
            page: Número de página
            page_size: Tamaño de página
            
        Returns:
            SearchFilters: Constructor de filtros configurado
        """
        filter_builder = SearchFilters()
        
        # Filtro de seguridad (siempre se aplica)
        filter_builder.add_security_filter(allowed_teams, valid_owner_ids)
        
        # Filtros opcionales
        if status:
            filter_builder.add_status_filter(
                status, VERIFICATION_STATUSES, current_user_id
            )
        
        if entity_id:
            filter_builder.add_entity_filter(entity_id)
        
        if process_id:
            filter_builder.add_process_filter(process_id)
        
        # Paginación
        filter_builder.add_pagination(page, page_size)
        
        return filter_builder

    def _execute_search_query(self, db, filter_clause: str, bind_vars: dict) -> dict:
        """
        Ejecuta la query de búsqueda en ArangoDB.
        
        Args:
            db: Instancia de base de datos
            filter_clause: Cláusula FILTER construida
            bind_vars: Variables de binding para la query
            
        Returns:
            dict: Resultado con items y total, o diccionario vacío
        """
        aql = SearchQueries.search_documents_query(filter_clause)
        cursor = db.aql.execute(aql, bind_vars=bind_vars)
        result = list(cursor)
        
        return result[0] if result else {"items": [], "total": 0}

    def _resolve_team_codes_to_uuids(self, db, allowed_teams: List[str]) -> List[str]:
        """
        Traduce los códigos de permisos (ej: 'CARR:213.11', 'FAC:10')
        a los _key (UUIDs) reales de las entidades en ArangoDB.
        
        Args:
            db: Instancia de base de datos
            allowed_teams: Lista de códigos de equipos
            
        Returns:
            List[str]: Lista de UUIDs de entidades válidas
        """
        from src.features.context.utils import resolve_team_codes
        return resolve_team_codes(db, allowed_teams, return_full_object=False)


search_service = SearchService()