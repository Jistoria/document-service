from pydantic import BaseModel, Field
from typing import List, Optional, Any, Dict
from datetime import datetime


# --- Bloques básicos para relaciones ---

class EntityRef(BaseModel):
    id: str
    name: str
    # En NoSQL a veces falta el tipo, ponemos un default por seguridad
    type: Optional[str] = "unknown"
    code: Optional[str] = None


class SchemaRef(BaseModel):
    id: str
    name: str
    # CORRECCIÓN AQUÍ: Hacemos la versión Opcional
    # Si no viene en la BD, valdrá None (o puedes poner = 1 si prefieres un default)
    version: Optional[int] = None


class StorageRef(BaseModel):
    pdf_url: Optional[str] = None
    json_validated_url: Optional[str] = None
    text_url: Optional[str] = None


# --- Respuesta Detallada de Documento ---

class DocumentDetail(BaseModel):
    id: str = Field(..., alias="_key")
    original_filename: Optional[str] = "Sin nombre"  # Por seguridad
    status: Optional[str] = "processing"
    created_at: Optional[datetime] = None

    # Metadatos validados
    metadata: Dict[str, Any] = Field(default={}, alias="validated_metadata")

    # Enlaces a archivos (Puede venir vacío si falló la subida)
    storage: Optional[StorageRef] = None

    # Relaciones del Grafo
    context_entity: Optional[EntityRef] = None
    used_schema: Optional[SchemaRef] = None


# --- Respuesta de Lista Paginada ---

class DetailPagination(BaseModel):
    currentPage: int
    lastPage: int
    perPage: int
    total: int
    to: int # El índice del último elemento mostrado (ej: mostrando 1-10, to=10)
    hasMorePages: bool # Usamos bool (true/false) que es el estándar JSON

class DocumentListResponse(BaseModel):
    data: List[DocumentDetail]
    pagination: DetailPagination