from typing import Any, Dict

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
) -> Dict[str, Any]:
    return {
        "_key": task_id,
        "owner": user_snapshot,
        "status": status,
        "original_filename": internal_result.get("filename"),
        "processing_time": internal_result.get("processing_time"),
        "created_at": timestamp,
        "updated_at": now_iso,
        "storage": {
            "bucket": "documents-storage",
            "pdf_path": stored_paths.get("pdf"),
            "json_path": stored_paths.get("json"),
            "text_path": stored_paths.get("text"),
            "minio_original_pdf": stored_paths.get("minio_original_pdf"),
        },
        "validated_metadata": validated_metadata,
        "integrity_warnings": integrity_warnings,
        "context_snapshot": {
            "entity_id": context_values.get("id"),
            "entity_name": context_values.get("name"),
            "schema_id": schema_info.get("id"),
            "schema_name": schema_info.get("name"),
        },
    }
