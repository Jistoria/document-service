from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class ValidationRequest(BaseModel):
    # El diccionario de metadatos corregidos por el usuario
    metadata: Dict[str, Any]

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
