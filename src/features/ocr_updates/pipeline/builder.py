# src/features/ocr_updates/pipeline/builder.py
from typing import Any, Dict, Optional


def build_document_record(
    *,
    task_id: str,
    timestamp: str,
    internal_result: Dict[str, Any],
    user_snapshot: Dict[str, Any],
    status: str,
    stored_paths: Dict[str, str],
    validated_metadata: Dict[str, Any],
    integrity_warnings: Any,
    context_values: Dict[str, Any],
    schema_info: Dict[str, Any],
    now_iso: str,
    naming: Optional[Dict[str, Any]] = None,
    required_document: Dict[str, Any],
) -> Dict[str, Any]:
    naming = naming or {}

    return {
        "_key": task_id,
        "owner": user_snapshot,
        "status": status,
        "original_filename": internal_result.get("filename"),
        "processing_time": internal_result.get("processing_time"),
        "created_at": timestamp,
        "updated_at": now_iso,

        "naming": {
            "display_name": naming.get("display_name"),
            "name_code": naming.get("name_code"),
            "name_code_numeric": naming.get("name_code_numeric"),
            "name_path": naming.get("name_path"),
            "code_path": naming.get("code_path"),
            "code_numeric_path": naming.get("code_numeric_path"),
            "timestamp_tag": naming.get("timestamp_tag"),
        },

        "storage": {
            "bucket": "documents-storage",
            "pdf_path": stored_paths.get("pdf"),
            "json_path": stored_paths.get("json"),
            "text_path": stored_paths.get("text"),
            "pdf_original_path": stored_paths.get("pdf_original_path"),
        },

        "validated_metadata": validated_metadata,
        "integrity_warnings": integrity_warnings,

        "context_snapshot": {
            "entity_id": context_values.get("id"),
            "entity_name": context_values.get("name"),
            "schema_id": schema_info.get("id"),
            "schema_name": schema_info.get("name"),
            "required_doc_id": required_document.get("id"),
            "required_doc_name": required_document.get("name"),
            "required_doc_code": required_document.get("code")
        },
    }
