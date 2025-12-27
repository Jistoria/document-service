from pydantic import BaseModel, Field
from typing import Dict, Any, Optional


# Modelo para cuando el usuario corrige la Entidad (Carrera/Facultad)
class EntityCorrection(BaseModel):
    id: str
    name: str
    type: str  # 'carrera', 'facultad', etc.
    code: Optional[str] = None


# El payload principal que manda el usuario al dar click en "Guardar/Validar"
class ValidationRequest(BaseModel):
    # Los metadatos corregidos campo por campo
    validated_metadata: Dict[str, Any]

    # Opcional: Si el usuario cambió la carrera/facultad a la que pertenece
    corrected_context: Optional[EntityCorrection] = None

    class Config:
        schema_extra = {
            "example": {
                "validated_metadata": {
                    "faculty": {"value": "Ciencias de la Vida", "is_valid": True},
                    "career": {"value": "Enfermería", "is_valid": True}
                },
                "corrected_context": {
                    "id": "uuid-nuevo-o-existente",
                    "name": "Enfermería",
                    "type": "carrera"
                }
            }
        }