from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class ParsedOcrPayload:
    task_id: str
    timestamp: str

    doc_data: Dict[str, Any]
    internal_result: Dict[str, Any]
    ocr_extracted_list: List[Dict[str, Any]]

    external_doc: Dict[str, Any]
    context_values: Dict[str, Any]
    schema_info: Dict[str, Any]

    user_snapshot: Dict[str, Any]
    presigned_source: Dict[str, Any]

    required_document: Dict[str, Any] = field(default_factory=dict)
