from typing import List, Optional, Any, Dict
from pydantic import BaseModel

# --- Modelos Base ---

class CatalogItem(BaseModel):
    id: str  # _key
    name: str
    code: Optional[str] = None
    
class CareerItem(CatalogItem):
    faculty_id: Optional[str] = None
    faculty_name: Optional[str] = None

# --- Modelos de Catálogo de Procesos ---

class SubsystemItem(CatalogItem):
    """Subsistema del catálogo de procesos"""
    pass

class CategoryItem(CatalogItem):
    """Categoría de proceso"""
    subsystem_id: Optional[str] = None
    subsystem_name: Optional[str] = None

class ProcessItem(CatalogItem):
    """Proceso (puede tener padre: categoría u otro proceso)"""
    parent_id: Optional[str] = None
    parent_name: Optional[str] = None
    parent_type: Optional[str] = None  # "category" o "process"
    has_subprocesses: bool = False

class ProcessDetail(ProcessItem):
    """Detalle completo de un proceso con subprocesos"""
    subprocesses: List['ProcessDetail'] = []
    required_documents: List['RequiredDocumentItem'] = []

class RequiredDocumentItem(CatalogItem):
    """Documento requerido de un proceso"""
    process_id: str
    process_name: Optional[str] = None
    schema_id: Optional[str] = None
    is_public: bool = False
    # Información de ancestros (para filtrado)
    category_id: Optional[str] = None
    category_name: Optional[str] = None
    subsystem_id: Optional[str] = None
    subsystem_name: Optional[str] = None

# --- Modelos de Árbol Jerárquico ---

class ProcessTreeNode(BaseModel):
    """Nodo en el árbol jerárquico completo"""
    id: str
    name: str
    code: Optional[str] = None
    type: str  # "subsystem", "category", "process", "subprocess"
    children: List['ProcessTreeNode'] = []

class HierarchyNode(BaseModel):
    """Nodo genérico (legacy, mantener para compatibilidad)"""
    id: str
    name: str
    type: str
    children: List['HierarchyNode'] = []
