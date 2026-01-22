import math
from src.features.search.dependencies import VERIFICATION_STATUSES
from src.core.database import db_instance
import logging
from arango.exceptions import ArangoError
from typing import Optional, List

logger = logging.getLogger(__name__)
from .models import (
    DocumentDetail,
    DocumentListResponse,
    EntityRef,
    DetailPagination
)


class SearchService:

    def get_db(self):
        return db_instance.get_db()

    def get_document_by_id(self, doc_id: str):
        db = self.get_db()

        # AQL Actualizado con los nuevos nombres de aristas
        aql = """
        FOR doc IN documents
            FILTER doc._key == @doc_id

            // 1. Buscar Entidad (Ubicación)
            LET entity = (
                FOR v IN 1..1 OUTBOUND doc file_located_in
                RETURN { id: v._key, name: v.name, type: v.type, code: v.code }
            )[0]

            // 2. Buscar Esquema
            LET schema = (
                FOR v IN 1..1 OUTBOUND doc usa_esquema
                RETURN { id: v._key, name: v.name, version: v.version }
            )[0]

            // 3. Buscar Documento Requerido (Definición)
            LET req_doc = (
                FOR v IN 1..1 OUTBOUND doc complies_with
                RETURN { id: v._key, name: v.name, code_default: v.code }
            )[0]

            RETURN MERGE(doc, { 
                context_entity: entity, 
                used_schema: schema,
                required_document: req_doc
            })
        """
        cursor = db.aql.execute(aql, bind_vars={"doc_id": doc_id})
        result = list(cursor)

        if result:
            return {
                "success": True,
                "data": DocumentDetail(**result[0]),
                "message": "Documento encontrado exitosamente."
            }
        else:
            return {
                "success": False,
                "data": None,
                "message": "El documento no existe o no fue encontrado."
            }

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
        try:
            db = self.get_db()

            if allowed_teams and "*" not in allowed_teams:
                valid_owner_ids = self._resolve_team_codes_to_uuids(db, allowed_teams)

                # Si tenía permisos en Redis (ej: 'CARR:999') pero esa carrera
                # NO existe en Arango, valid_owner_ids estará vacío.
                # En ese caso, debemos bloquear el acceso (Fail Safe).
                if not valid_owner_ids:
                    return {
                        "success": True,
                        "data": DocumentListResponse(
                        data=[],
                        pagination=DetailPagination(
                            currentPage=page, lastPage=1, perPage=page_size,
                            total=0, to=0, hasMorePages=False
                        )
                    ),
                        "message": "No tienes permisos sobre ninguna entidad válida."
                    }


            offset = (page - 1) * page_size

            bind_vars = {
                "offset": offset,
                "limit": page_size
            }

            # --- Filtros Dinámicos ---
            filters = []

            # 1. FILTRO DE SEGURIDAD (Permisos)
            # Si allowed_teams es None o vacío, bloqueamos todo por seguridad (Fail-Safe)
            if allowed_teams is None:
                allowed_teams = []

            if "*" not in allowed_teams:
                # Lógica:
                # El documento está en una Entidad (ej: Carrera).
                # Esa Carrera pertenece a una Facultad.
                # El usuario puede tener permiso en la Carrera (Directo) o en la Facultad (Heredado).
                # Buscamos 1..2 niveles hacia arriba para ver si alguna entidad padre está en allowed_teams.

                filters.append("""
                                LENGTH(
                                    FOR owner IN 1..2 OUTBOUND doc file_located_in, belongs_to
                                    FILTER owner._key IN @valid_owner_ids
                                    LIMIT 1
                                    RETURN 1
                                ) > 0
                            """)
                bind_vars["valid_owner_ids"] = valid_owner_ids

            if status:
                filters.append("doc.status == @status")
                bind_vars["status"] = status
                # Si el estado es 'required_attention', filtramos para que el usuario solo vea SU trabajo pendiente
                if status in VERIFICATION_STATUSES and current_user_id:
                     logger.info(f"Filtrando documentos para el usuario: {current_user_id}")
                     filters.append("doc.owner.id == @current_user_id")
                     bind_vars["current_user_id"] = current_user_id

            # 1. Filtro jerárquico de ENTIDAD
            if entity_id:
                filters.append("""
                    LENGTH(
                        FOR entity IN 1..5 OUTBOUND doc file_located_in, belongs_to
                        FILTER entity._key == @entity_id
                        LIMIT 1
                        RETURN 1
                    ) > 0
                """)
                bind_vars["entity_id"] = entity_id

            # 2. Filtro jerárquico de PROCESO
            if process_id:
                filters.append("""
                    LENGTH(
                        FOR node IN 1..6 OUTBOUND doc complies_with, catalog_belongs_to
                        FILTER node._key == @process_id
                        LIMIT 1
                        RETURN 1
                    ) > 0
                """)
                bind_vars["process_id"] = process_id

            # Unimos todos los filtros con AND
            filter_clause = "FILTER " + " AND ".join(filters) if filters else ""

            # --- AQL ---
            aql = f"""
            LET docs = (
                FOR doc IN documents
                    {filter_clause}
                    SORT doc.created_at DESC
                    LIMIT @offset, @limit

                    LET entity = (
                        FOR v IN 1..1 OUTBOUND doc file_located_in
                        RETURN {{ id: v._key, name: v.name, type: v.type }}
                    )[0]

                    LET schema = (
                        FOR v IN 1..1 OUTBOUND doc usa_esquema
                        RETURN {{ id: v._key, name: v.name }}
                    )[0]

                    LET req_doc = (
                        FOR v IN 1..1 OUTBOUND doc complies_with
                        RETURN {{ id: v._key, name: v.name, code_default: v.code }}
                    )[0]

                    RETURN MERGE(doc, {{
                        context_entity: entity,
                        used_schema: schema,
                        required_document: req_doc
                    }})
            )

            LET total_count = (
                FOR doc IN documents
                    {filter_clause}
                    RETURN 1
            )

            RETURN {{ items: docs, total: LENGTH(total_count) }}
            """

            cursor = db.aql.execute(aql, bind_vars=bind_vars)
            result = list(cursor)

            if not result:
                # Si no hay resultados (página vacía), no es un error, es data vacía
                # Ajustamos para devolver lista vacía en lugar de explotar
                return {
                    "success": True,
                    "data": DocumentListResponse(
                        data=[],
                        pagination=DetailPagination(
                            currentPage=page, lastPage=1, perPage=page_size,
                            total=0, to=0, hasMorePages=False
                        )
                    ),
                    "message": "No se encontraron documentos."
                }

            data = result[0]

            # --- Procesamiento ---
            items_list = [DocumentDetail(**d) for d in data.get("items", [])]
            total_items = data.get("total", 0)

            last_page = max(1, math.ceil(total_items / page_size))
            to_item = offset + len(items_list)
            has_more = page < last_page

            internal_data = DocumentListResponse(
                data=items_list,
                pagination=DetailPagination(
                    currentPage=page,
                    lastPage=last_page,
                    perPage=page_size,
                    total=total_items,
                    to=to_item,
                    hasMorePages=has_more
                )
            )

            return {
                "success": True,
                "data": internal_data,
                "message": "Búsqueda completada exitosamente."
            }

        except ArangoError as e:
            logger.error("Error AQL / ArangoDB en search_documents", exc_info=True)
            return {
                "success": False,
                "data": None,
                "message": f"Error en base de datos: {str(e)}"
            }

        except Exception as e:
            logger.error("Error inesperado en search_documents", exc_info=True)
            return {
                "success": False,
                "data": None,
                "message": "Error interno al procesar la búsqueda."
            }

    def get_available_entities(self):
        """
        Retorna las entities (Carreras/Facultades) que TIENEN documentos asociados.
        """
        db = self.get_db()
        # CAMBIO: Usamos 'file_located_in'
        aql = """
        FOR doc IN documents
            FOR entity IN 1..1 OUTBOUND doc file_located_in
            RETURN DISTINCT {
                id: entity._key,
                name: entity.name,
                type: entity.type
            }
        """
        cursor = db.aql.execute(aql)
        entities = [EntityRef(**d) for d in cursor]

        return {
            "success": True,
            "data": entities,
            "message": f"Se encontraron {len(entities)} entidades con documentos."
        }

    def _resolve_team_codes_to_uuids(self, db, allowed_teams: List[str]) -> List[str]:
        """
        Traduce los códigos de permisos (ej: 'CARR:213.11', 'FAC:10')
        a los _key (UUIDs) reales de las entidades en ArangoDB.
        """
        from src.features.context.utils import resolve_team_codes
        return resolve_team_codes(db, allowed_teams, return_full_object=False)


search_service = SearchService()