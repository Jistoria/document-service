from typing import List, Dict, Any, Optional

from pydantic import BaseModel, field_validator


class ValidationRequest(BaseModel):
    # El diccionario de metadatos corregidos por el usuario (pre-validación)
    metadata: Dict[str, Any]


class ValidationConfirmRequest(BaseModel):
    metadata: Dict[str, Any]
    display_name: Optional[str] = None
    is_public: bool = False
    keep_original: bool = False

    @field_validator("display_name")
    @classmethod
    def normalize_display_name(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None

        normalized = value.strip()
        if not normalized:
            raise ValueError("display_name no puede ser vacío")
        if len(normalized) < 3:
            raise ValueError("display_name debe tener al menos 3 caracteres")
        return normalized


class FieldReport(BaseModel):
    key: str
    label: str
    is_valid: bool
    warnings: List[str] = []
    actions: List[str] = [] # ej: ["CREATE_ENTITY", "CREATE_USER"]

class ValidationReportResponse(BaseModel):
    score: float # 0.0 a 100.0
    is_ready: bool # Si el puntaje es suficiente para guardar
    fields_report: List[FieldReport]
    summary_warnings: List[str]


# Modelo para cuando el usuario corrige la Entidad (Carrera/Facultad)
class EntityCorrection(BaseModel):
    id: str
    name: str
    type: str  # 'carrera', 'facultad', etc.
    code: Optional[str] = None
