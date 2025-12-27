from fastapi import APIRouter, HTTPException, Body
from .models import ValidationRequest
from .service import validation_service

router = APIRouter(prefix="/documents", tags=["Validation & Workflow"])

@router.patch("/{doc_id}/validate")
async def validate_document(doc_id: str, payload: ValidationRequest):
    """
    Endpoint para que el usuario guarde la corrección manual.
    - Actualiza los metadatos finales.
    - Si se cambió la carrera/facultad, repara el grafo automáticamente.
    """
    try:
        return validation_service.confirm_validation(doc_id, payload)
    except Exception as e:
        # En producción usa logs reales
        print(f"Error validating: {e}")
        raise HTTPException(status_code=500, detail=str(e))