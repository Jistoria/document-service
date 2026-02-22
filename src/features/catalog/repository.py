import unicodedata
from typing import Dict, Optional

# Listas de caracteres acentuados para normalización en AQL SUBSTITUTE
_ACCENT_FROM = ['á','é','í','ó','ú','à','è','ì','ò','ù','â','ê','î','ô','û','ã','õ','ä','ë','ï','ö','ü','ñ','ç']
_ACCENT_TO   = ['a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','o','a','e','i','o','u','n','c']


class CatalogRepository:
    """
    Repository para operaciones de catálogo en ArangoDB.
    Separa la lógica de acceso a datos del servicio.
    """

    @staticmethod
    def _search_vars(search: str) -> Dict:
        """
        Normaliza el término de búsqueda (minúsculas + sin acentos) y retorna
        los bind_vars necesarios para la búsqueda en AQL con SUBSTITUTE.
        """
        nfd = unicodedata.normalize('NFD', search.lower())
        normalized = ''.join(c for c in nfd if unicodedata.category(c) != 'Mn')
        return {
            "search": normalized,
            "accent_from": _ACCENT_FROM,
            "accent_to": _ACCENT_TO,
        }

    # ========== ESTRUCTURA ORGANIZACIONAL ==========

    @staticmethod
    def get_faculties_query(search: Optional[str] = None) -> tuple[str, Dict]:
        """Query para obtener facultades"""
        if search:
            aql = """
            FOR doc IN entities
                FILTER doc.type == 'facultad'
                FILTER (
                    SUBSTITUTE(LOWER(doc.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                    SUBSTITUTE(LOWER(doc.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                )
                SORT doc.name ASC
                RETURN {
                    id: doc._key,
                    name: doc.name,
                    code: doc.code
                }
            """
            bind_vars = CatalogRepository._search_vars(search)
        else:
            aql = """
            FOR doc IN entities
                FILTER doc.type == 'facultad'
                SORT doc.name ASC
                RETURN {
                    id: doc._key,
                    name: doc.name,
                    code: doc.code
                }
            """
            bind_vars = {}
        return aql, bind_vars

    @staticmethod
    def get_careers_query(faculty_id: Optional[str] = None, search: Optional[str] = None) -> tuple[str, Dict]:
        """Query para obtener carreras"""
        if faculty_id:
            aql = """
            FOR fac IN entities
                FILTER fac._key == @faculty_id AND fac.type == 'facultad'
                FOR car, edge IN INBOUND fac belongs_to
                    FILTER car.type == 'carrera'
            """
            if search:
                aql += """
                    FILTER (
                        SUBSTITUTE(LOWER(car.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                        SUBSTITUTE(LOWER(car.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                    )
                """
            aql += """
                    SORT car.name ASC
                    RETURN {
                        id: car._key,
                        name: car.name,
                        code: car.code,
                        faculty_id: fac._key,
                        faculty_name: fac.name
                    }
            """
            bind_vars = {"faculty_id": faculty_id}
            if search:
                bind_vars.update(CatalogRepository._search_vars(search))
        else:
            aql = """
            FOR car IN entities
                FILTER car.type == 'carrera'
            """
            if search:
                aql += """
                FILTER (
                    SUBSTITUTE(LOWER(car.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                    SUBSTITUTE(LOWER(car.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                )
                """
            aql += """
                LET parents = (
                    FOR fac IN OUTBOUND car belongs_to
                    FILTER fac.type == 'facultad'
                    RETURN fac
                )
                LET fac = LENGTH(parents) > 0 ? parents[0] : null
                
                SORT car.name ASC
                RETURN {
                    id: car._key,
                    name: car.name,
                    code: car.code,
                    faculty_id: fac._key,
                    faculty_name: fac.name
                }
            """
            bind_vars = CatalogRepository._search_vars(search) if search else {}
        
        return aql, bind_vars

    # ========== CATÁLOGO DE PROCESOS ==========

    @staticmethod
    def get_subsystems_query(search: Optional[str] = None) -> tuple[str, Dict]:
        """Query para obtener subsistemas"""
        if search:
            aql = """
            FOR doc IN subsystems
                FILTER (
                    SUBSTITUTE(LOWER(doc.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                    SUBSTITUTE(LOWER(doc.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                )
                SORT doc.name ASC
                RETURN {
                    id: doc._key,
                    name: doc.name,
                    code: doc.code
                }
            """
            bind_vars = CatalogRepository._search_vars(search)
        else:
            aql = """
            FOR doc IN subsystems
                SORT doc.name ASC
                RETURN {
                    id: doc._key,
                    name: doc.name,
                    code: doc.code
                }
            """
            bind_vars = {}
        return aql, bind_vars

    @staticmethod
    def get_categories_query(subsystem_id: Optional[str] = None, search: Optional[str] = None) -> tuple[str, Dict]:
        """Query para obtener categorías"""
        if subsystem_id:
            aql = """
            FOR sub IN subsystems
                FILTER sub._key == @subsystem_id
                FOR cat IN INBOUND sub catalog_belongs_to
                    FILTER IS_SAME_COLLECTION('process_categories', cat)
            """
            if search:
                aql += """
                    FILTER (
                        SUBSTITUTE(LOWER(cat.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                        SUBSTITUTE(LOWER(cat.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                    )
                """
            aql += """
                    SORT cat.name ASC
                    RETURN {
                        id: cat._key,
                        name: cat.name,
                        code: cat.code,
                        subsystem_id: sub._key,
                        subsystem_name: sub.name
                    }
            """
            bind_vars = {"subsystem_id": subsystem_id}
            if search:
                bind_vars.update(CatalogRepository._search_vars(search))
        else:
            aql = """
            FOR cat IN process_categories
            """
            if search:
                aql += """
                FILTER (
                    SUBSTITUTE(LOWER(cat.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                    SUBSTITUTE(LOWER(cat.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                )
                """
            aql += """
                LET parents = (
                    FOR sub IN OUTBOUND cat catalog_belongs_to
                    FILTER IS_SAME_COLLECTION('subsystems', sub)
                    RETURN sub
                )
                LET sub = LENGTH(parents) > 0 ? parents[0] : null
                
                SORT cat.name ASC
                RETURN {
                    id: cat._key,
                    name: cat.name,
                    code: cat.code,
                    subsystem_id: sub._key,
                    subsystem_name: sub.name
                }
            """
            bind_vars = CatalogRepository._search_vars(search) if search else {}
        
        return aql, bind_vars

    @staticmethod
    def get_processes_query(category_id: Optional[str] = None, parent_process_id: Optional[str] = None, 
                           search: Optional[str] = None) -> tuple[str, Dict]:
        """Query para obtener procesos"""
        bind_vars = {}
        
        if category_id:
            # Procesos de una categoría
            aql = """
            FOR cat IN process_categories
                FILTER cat._key == @category_id
                FOR proc IN INBOUND cat catalog_belongs_to
                    FILTER IS_SAME_COLLECTION('processes', proc)
            """
            bind_vars["category_id"] = category_id
            
            if search:
                aql += """
                    FILTER (
                        SUBSTITUTE(LOWER(proc.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                        SUBSTITUTE(LOWER(proc.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                    )
                """
                bind_vars.update(CatalogRepository._search_vars(search))
            
            aql += """
                    LET has_subprocesses = LENGTH(
                        FOR sub IN INBOUND proc catalog_belongs_to
                        FILTER IS_SAME_COLLECTION('processes', sub)
                        LIMIT 1
                        RETURN 1
                    ) > 0
                    
                    SORT proc.name ASC
                    RETURN {
                        id: proc._key,
                        name: proc.name,
                        code: proc.code,
                        parent_id: cat._key,
                        parent_name: cat.name,
                        parent_type: 'category',
                        has_subprocesses: has_subprocesses
                    }
            """
        elif parent_process_id:
            # Subprocesos de un proceso
            aql = """
            FOR parent IN processes
                FILTER parent._key == @parent_process_id
                FOR proc IN INBOUND parent catalog_belongs_to
                    FILTER IS_SAME_COLLECTION('processes', proc)
            """
            bind_vars["parent_process_id"] = parent_process_id
            
            if search:
                aql += """
                    FILTER (
                        SUBSTITUTE(LOWER(proc.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                        SUBSTITUTE(LOWER(proc.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                    )
                """
                bind_vars.update(CatalogRepository._search_vars(search))
            
            aql += """
                    LET has_subprocesses = LENGTH(
                        FOR sub IN INBOUND proc catalog_belongs_to
                        FILTER IS_SAME_COLLECTION('processes', sub)
                        LIMIT 1
                        RETURN 1
                    ) > 0
                    
                    SORT proc.name ASC
                    RETURN {
                        id: proc._key,
                        name: proc.name,
                        code: proc.code,
                        parent_id: parent._key,
                        parent_name: parent.name,
                        parent_type: 'process',
                        has_subprocesses: has_subprocesses
                    }
            """
        else:
            # Todos los procesos
            aql = """
            FOR proc IN processes
            """
            if search:
                aql += """
                FILTER (
                    SUBSTITUTE(LOWER(proc.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                    SUBSTITUTE(LOWER(proc.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                )
                """
                bind_vars.update(CatalogRepository._search_vars(search))
            
            aql += """
                LET parents = (
                    FOR parent IN OUTBOUND proc catalog_belongs_to
                    RETURN parent
                )
                LET parent = LENGTH(parents) > 0 ? parents[0] : null
                LET parent_type = parent 
                    ? (IS_SAME_COLLECTION('process_categories', parent) ? 'category' : 'process')
                    : null
                
                LET has_subprocesses = LENGTH(
                    FOR sub IN INBOUND proc catalog_belongs_to
                    FILTER IS_SAME_COLLECTION('processes', sub)
                    LIMIT 1
                    RETURN 1
                ) > 0
                
                SORT proc.name ASC
                RETURN {
                    id: proc._key,
                    name: proc.name,
                    code: proc.code,
                    parent_id: parent._key,
                    parent_name: parent.name,
                    parent_type: parent_type,
                    has_subprocesses: has_subprocesses
                }
            """
        
        return aql, bind_vars

    @staticmethod
    def get_process_detail_query(process_id: str) -> tuple[str, Dict]:
        """Query para obtener detalle de un proceso"""
        aql = """
        FOR proc IN processes
            FILTER proc._key == @process_id
            
            LET parents = (
                FOR parent IN OUTBOUND proc catalog_belongs_to
                RETURN parent
            )
            LET parent = LENGTH(parents) > 0 ? parents[0] : null
            LET parent_type = parent 
                ? (IS_SAME_COLLECTION('process_categories', parent) ? 'category' : 'process')
                : null
            
            RETURN {
                id: proc._key,
                name: proc.name,
                code: proc.code,
                parent_id: parent._key,
                parent_name: parent.name,
                parent_type: parent_type
            }
        """
        bind_vars = {"process_id": process_id}
        return aql, bind_vars

    @staticmethod
    def get_subprocesses_query(process_id: str) -> tuple[str, Dict]:
        """Query para obtener subprocesos de un proceso"""
        aql = """
        FOR parent IN processes
            FILTER parent._key == @process_id
            FOR proc IN INBOUND parent catalog_belongs_to
                FILTER IS_SAME_COLLECTION('processes', proc)
                SORT proc.name ASC
                RETURN {
                    id: proc._key,
                    name: proc.name,
                    code: proc.code,
                    parent_id: parent._key,
                    parent_name: parent.name,
                    parent_type: 'process'
                }
        """
        bind_vars = {"process_id": process_id}
        return aql, bind_vars

    @staticmethod
    def get_process_documents_query(process_id: str) -> tuple[str, Dict]:
        """Query para obtener documentos requeridos de un proceso"""
        aql = """
        FOR proc IN processes
            FILTER proc._key == @process_id
            FOR doc IN INBOUND proc catalog_belongs_to
                FILTER IS_SAME_COLLECTION('required_documents', doc)
                SORT doc.name ASC
                RETURN {
                    id: doc._key,
                    name: doc.name,
                    code: doc.code,
                    process_id: proc._key,
                    process_name: proc.name,
                    schema_id: doc.schema_id,
                    is_public: doc.is_public
                }
        """
        bind_vars = {"process_id": process_id}
        return aql, bind_vars

    @staticmethod
    def get_required_documents_query(process_id: Optional[str] = None, subsystem_id: Optional[str] = None,
                                     search: Optional[str] = None) -> tuple[str, Dict]:
        """Query para obtener documentos requeridos"""
        bind_vars = {}
        
        if process_id:
            # Documentos de un proceso específico
            aql = """
            FOR proc IN processes
                FILTER proc._key == @process_id
                FOR doc IN INBOUND proc catalog_belongs_to
                    FILTER IS_SAME_COLLECTION('required_documents', doc)
            """
            bind_vars["process_id"] = process_id
            
            if search:
                aql += """
                    FILTER (
                        SUBSTITUTE(LOWER(doc.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                        SUBSTITUTE(LOWER(doc.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                    )
                """
                bind_vars.update(CatalogRepository._search_vars(search))
            
            aql += """
                    SORT doc.name ASC
                    RETURN {
                        id: doc._key,
                        name: doc.name,
                        code: doc.code,
                        process_id: proc._key,
                        process_name: proc.name,
                        schema_id: doc.schema_id,
                        is_public: doc.is_public
                    }
            """
        elif subsystem_id:
            # Documentos de un subsistema (a través de categoría y proceso)
            aql = """
            FOR sub IN subsystems
                FILTER sub._key == @subsystem_id
                FOR cat IN INBOUND sub catalog_belongs_to
                    FILTER IS_SAME_COLLECTION('process_categories', cat)
                    FOR proc IN INBOUND cat catalog_belongs_to
                        FILTER IS_SAME_COLLECTION('processes', proc)
                        FOR doc IN INBOUND proc catalog_belongs_to
                            FILTER IS_SAME_COLLECTION('required_documents', doc)
            """
            bind_vars["subsystem_id"] = subsystem_id
            
            if search:
                aql += """
                            FILTER (
                                SUBSTITUTE(LOWER(doc.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                                SUBSTITUTE(LOWER(doc.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                            )
                """
                bind_vars.update(CatalogRepository._search_vars(search))
            
            aql += """
                            SORT doc.name ASC
                            RETURN DISTINCT {
                                id: doc._key,
                                name: doc.name,
                                code: doc.code,
                                process_id: proc._key,
                                process_name: proc.name,
                                schema_id: doc.schema_id,
                                is_public: doc.is_public,
                                category_id: cat._key,
                                category_name: cat.name,
                                subsystem_id: sub._key,
                                subsystem_name: sub.name
                            }
            """
        else:
            # Todos los documentos requeridos
            aql = """
            FOR doc IN required_documents
            """
            if search:
                aql += """
                FILTER (
                    SUBSTITUTE(LOWER(doc.name), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%') OR
                    SUBSTITUTE(LOWER(doc.code), @accent_from, @accent_to) LIKE CONCAT('%', @search, '%')
                )
                """
                bind_vars.update(CatalogRepository._search_vars(search))
            
            aql += """
                LET proc_list = (
                    FOR proc IN OUTBOUND doc catalog_belongs_to
                    FILTER IS_SAME_COLLECTION('processes', proc)
                    RETURN proc
                )
                LET proc = LENGTH(proc_list) > 0 ? proc_list[0] : null
                
                SORT doc.name ASC
                RETURN {
                    id: doc._key,
                    name: doc.name,
                    code: doc.code,
                    process_id: proc._key,
                    process_name: proc.name,
                    schema_id: doc.schema_id,
                    is_public: doc.is_public
                }
            """
        
        return aql, bind_vars

    @staticmethod
    def get_process_tree_base_query() -> tuple[str, Dict]:
        """Query para obtener estructura base del árbol de procesos"""
        aql = """
        FOR sub IN subsystems
            SORT sub.name ASC
            LET categories = (
                FOR cat IN INBOUND sub catalog_belongs_to
                    FILTER IS_SAME_COLLECTION('process_categories', cat)
                    SORT cat.name ASC
                    LET processes = (
                        FOR proc IN INBOUND cat catalog_belongs_to
                            FILTER IS_SAME_COLLECTION('processes', proc)
                            SORT proc.name ASC
                            RETURN proc
                    )
                    RETURN {
                        id: cat._key,
                        name: cat.name,
                        code: cat.code,
                        type: 'category',
                        children: processes
                    }
            )
            RETURN {
                id: sub._key,
                name: sub.name,
                code: sub.code,
                type: 'subsystem',
                children: categories
            }
        """
        bind_vars = {}
        return aql, bind_vars


catalog_repository = CatalogRepository()
