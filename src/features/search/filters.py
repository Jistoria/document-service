"""
Construcción de filtros dinámicos para queries AQL.
Maneja la lógica de permisos y filtros de búsqueda.
"""
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class SearchFilters:
    """Clase para construir filtros dinámicos de búsqueda."""
    
    def __init__(self):
        self.filters = []
        self.bind_vars = {}
    
    def add_security_filter(
        self, 
        allowed_teams: Optional[List[str]], 
        valid_owner_ids: List[str]
    ) -> 'SearchFilters':
        """
        Añade filtro de seguridad basado en permisos de equipo.
        
        Args:
            allowed_teams: Lista de códigos de equipos permitidos o None
            valid_owner_ids: Lista de UUIDs de entidades válidas
            
        Returns:
            self: Para encadenamiento de métodos
        """
        # Si allowed_teams es None o vacío, bloqueamos todo por seguridad (Fail-Safe)
        if allowed_teams is None:
            allowed_teams = []
        
        if "*" not in allowed_teams:
            # Lógica:
            # El documento está en una Entidad (ej: Carrera).
            # Esa Carrera pertenece a una Facultad.
            # El usuario puede tener permiso en la Carrera (Directo) o en la Facultad (Heredado).
            # Buscamos 1..2 niveles hacia arriba para ver si alguna entidad padre está en allowed_teams.
            self.filters.append("""
                LENGTH(
                    FOR owner IN 1..2 OUTBOUND doc file_located_in, belongs_to
                    FILTER owner._key IN @valid_owner_ids
                    LIMIT 1
                    RETURN 1
                ) > 0
            """)
            self.bind_vars["valid_owner_ids"] = valid_owner_ids
        
        return self
    
    def add_status_filter(
        self, 
        status: str,
        verification_statuses: List[str],
        current_user_id: Optional[str] = None
    ) -> 'SearchFilters':
        """
        Añade filtro por estado del documento.
        Si el estado es de verificación, filtra por usuario actual.
        
        Args:
            status: Estado del documento a filtrar
            verification_statuses: Lista de estados que requieren verificación
            current_user_id: ID del usuario actual (para estados de verificación)
            
        Returns:
            self: Para encadenamiento de métodos
        """
        self.filters.append("doc.status == @status")
        self.bind_vars["status"] = status
        
        # Si el estado es 'required_attention', filtramos para que el usuario solo vea SU trabajo pendiente
        if status in verification_statuses and current_user_id:
            logger.info(f"Filtrando documentos para el usuario: {current_user_id}")
            self.filters.append("doc.owner.id == @current_user_id")
            self.bind_vars["current_user_id"] = current_user_id
        
        return self
    
    def add_entity_filter(self, entity_id: str) -> 'SearchFilters':
        """
        Añade filtro jerárquico por entidad (búsqueda en árbol de pertenencia).
        
        Args:
            entity_id: ID de la entidad a filtrar
            
        Returns:
            self: Para encadenamiento de métodos
        """
        self.filters.append("""
            LENGTH(
                FOR entity IN 1..5 OUTBOUND doc file_located_in, belongs_to
                FILTER entity._key == @entity_id
                LIMIT 1
                RETURN 1
            ) > 0
        """)
        self.bind_vars["entity_id"] = entity_id
        return self
    
    def add_process_filter(self, process_id: str) -> 'SearchFilters':
        """
        Añade filtro jerárquico por proceso (búsqueda en catálogo).
        
        Args:
            process_id: ID del proceso a filtrar
            
        Returns:
            self: Para encadenamiento de métodos
        """
        self.filters.append("""
            LENGTH(
                FOR node IN 1..6 OUTBOUND doc complies_with, catalog_belongs_to
                FILTER node._key == @process_id
                LIMIT 1
                RETURN 1
            ) > 0
        """)
        self.bind_vars["process_id"] = process_id
        return self
    
    def add_pagination(self, page: int, page_size: int) -> 'SearchFilters':
        """
        Añade variables de paginación.
        
        Args:
            page: Número de página (1-indexed)
            page_size: Cantidad de items por página
            
        Returns:
            self: Para encadenamiento de métodos
        """
        offset = (page - 1) * page_size
        self.bind_vars["offset"] = offset
        self.bind_vars["limit"] = page_size
        return self
    
    def build_filter_clause(self) -> str:
        """
        Construye la cláusula FILTER completa para la query AQL.
        
        Returns:
            str: Cláusula FILTER con todos los filtros unidos por AND, o string vacío
        """
        if not self.filters:
            return ""
        return "FILTER " + " AND ".join(self.filters)
    
    def get_bind_vars(self) -> Dict[str, Any]:
        """
        Retorna las variables de binding para la query.
        
        Returns:
            dict: Diccionario con todas las bind_vars acumuladas
        """
        return self.bind_vars
