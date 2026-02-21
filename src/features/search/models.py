from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict, Generic, TypeVar
from datetime import datetime


# --- Bloques de Objetos Anidados ---

class OwnerRef(BaseModel):
    id: str
    name: str
    email: Optional[str] = None


class NamingRef(BaseModel):
    display_name: Optional[str] = None
    name_code: Optional[str] = None
    name_code_numeric: Optional[str] = None
    name_path: Optional[str] = None
    code_path: Optional[str] = None
    code_numeric_path: Optional[str] = None
    timestamp_tag: Optional[str] = None
    required_document_code: Optional[str] = None


class StorageRef(BaseModel):
    bucket: Optional[str] = None
    pdf_path: Optional[str] = None
    json_path: Optional[str] = None
    text_path: Optional[str] = None
    pdf_original_path: Optional[str] = None
    # Campo calculado opcional por si decides generar URLs firmadas al vuelo
    pdf_signed_url: Optional[str] = None


# --- Bloques de Grafos (Relaciones) ---

class EntityRef(BaseModel):
    id: str
    name: str
    type: Optional[str] = "unknown"
    code: Optional[str] = None


class SchemaRef(BaseModel):
    id: str
    name: str
    version: Optional[int] = None


class RequiredDocumentRef(BaseModel):
    id: str
    name: str
    code_default: Optional[str] = None


# --- Respuesta Detallada de Documento ---

class DocumentDetail(BaseModel):
    id: str = Field(..., alias="_key")

    owner: Optional[OwnerRef] = None
    status: Optional[str] = "processing"

    original_filename: Optional[str] = "Sin nombre"
    processing_time: Optional[float] = None
    is_public: Optional[bool] = True
    keep_original: Optional[bool] = False
    has_custom_display_name: Optional[bool] = False
    has_integrity_signature: Optional[bool] = False

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Nuevo bloque de naming
    naming: Optional[NamingRef] = None

    # Metadatos validados (Estructura dinámica)
    metadata: Dict[str, Any] = Field(default={}, alias="validated_metadata")

    # Advertencias de integridad
    integrity_warnings: List[str] = []

    # Storage (Rutas)
    storage: Optional[StorageRef] = None

    # Snapshot del contexto (Lo que está guardado en el documento)
    context_snapshot: Dict[str, Any] = {}

    # --- Relaciones "Vivas" del Grafo (Se llenan en el Service) ---
    graph_entity: Optional[EntityRef] = Field(None, alias="context_entity")
    graph_schema: Optional[SchemaRef] = Field(None, alias="used_schema")
    graph_required_document: Optional[RequiredDocumentRef] = Field(None, alias="required_document")


# --- Paginación y Wrappers ---

class DetailPagination(BaseModel):
    currentPage: int
    lastPage: int
    perPage: int
    total: int
    to: int
    hasMorePages: bool


class DocumentListResponse(BaseModel):
    data: List[DocumentDetail]
    pagination: DetailPagination


T = TypeVar('T')




class MetadataFilterOption(BaseModel):
    key: str
    label: str
    data_type: Optional[str] = None
    input_type: Optional[str] = None
    entity_type: Optional[str] = None
    required: bool = False
    sort_order: int = 0


class MetadataFilterCatalog(BaseModel):
    required_document: RequiredDocumentRef
    schema: SchemaRef
    metadata_fields: List[MetadataFilterOption]

class ApiResponse(BaseModel, Generic[T]):
    success: bool
    message: str
    data: Optional[T] = None


# Alias para el Router
DocumentDetailResponse = ApiResponse[DocumentDetail]
DocumentListAPIResponse = ApiResponse[DocumentListResponse]
EntityListAPIResponse = ApiResponse[List[EntityRef]]
MetadataFilterCatalogResponse = ApiResponse[MetadataFilterCatalog]
