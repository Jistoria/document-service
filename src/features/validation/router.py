import logging
from fastapi import APIRouter, HTTPException
from .models import ValidationRequest, ValidationReportResponse
from .service import validation_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/documents", tags=["Validation & Workflow"])


@router.post("/{doc_id}/metadata/quality-check", response_model=ValidationReportResponse)
async def check_validation_rules(doc_id: str, payload: ValidationRequest):
    """
    Endpoint de PRE-VALIDACIÓN (Solo lectura/simulación).
    - Analiza los datos enviados contra el esquema del documento.
    - Verifica tipos de datos (email, fecha, json).
    - Verifica existencia de entidades (¿La carrera existe o se creará?).
    - Calcula puntaje de completitud.
    """
    try:
        return await validation_service.dry_run_validation(doc_id, payload)
    except Exception as e:
        logger.exception("Error checking validation")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/{doc_id}/metadata/confirm")
async def validate_document(doc_id: str, payload: ValidationRequest):
    """
    Endpoint de GUARDADO FINAL.
    - Aplica los cambios.
    - Actualiza grafo.
    - Cambia estado a 'validated'.
    """
    try:
        return await validation_service.confirm_validation(doc_id, payload)
    except Exception as e:
        logger.exception("Error validating document")
        raise HTTPException(status_code=500, detail=str(e))
