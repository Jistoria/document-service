from typing import Any, Dict, List
from .dto import ParsedOcrPayload

def parse_payload(payload: Dict[str, Any]) -> ParsedOcrPayload:
    # --- 1. Desempaquetado ---
    task_id = payload.get("task_id")
    timestamp = payload.get("timestamp")

    doc_data = payload.get("document_data", {})
    internal_result = doc_data.get("internal_result", {})
    ocr_extracted_list: List[Dict[str, Any]] = internal_result.get("metadata", [])

    # Contexto
    external_doc = doc_data.get("external_document", {})
    file_info = external_doc.get("files", [{}])[0]
    context_values = file_info.get("metadataValues", {}) or {}
    schema_info = file_info.get("metadataSchema", {}) or {}

    # --- 2. Usuario ---
    raw_user = external_doc.get("user", {}) or {}
    user_snapshot = {
        "id": raw_user.get("id") or payload.get("user_id"),
        "name": raw_user.get("name", "Desconocido"),
        "email": raw_user.get("email", "")
    }

    required_doc_info = {
        "id": file_info.get("requiredDocumentId"),
        "name": file_info.get("requiredDocumentName"),
        "code": file_info.get("requiredDocumentCode")
    }

    presigned_source = internal_result.get("presigned_urls", {}) or {}

    return ParsedOcrPayload(
        task_id=task_id,
        timestamp=timestamp,
        doc_data=doc_data,
        internal_result=internal_result,
        ocr_extracted_list=ocr_extracted_list,
        external_doc=external_doc,
        context_values=context_values,
        schema_info=schema_info,
        user_snapshot=user_snapshot,
        presigned_source=presigned_source,
        required_document=required_doc_info,
    )
