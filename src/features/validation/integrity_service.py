import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class IntegrityService:
    def __init__(self, signing_secret: Optional[str] = None):
        self._signing_secret = signing_secret or os.getenv("DOCUMENT_INTEGRITY_SECRET", "dev-integrity-secret")

    def _sign(self, payload: bytes) -> str:
        return hmac.new(
            self._signing_secret.encode("utf-8"),
            payload,
            digestmod=hashlib.sha256,
        ).hexdigest()

    def _hash_json(self, data: Dict[str, Any]) -> str:
        canonical = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def _hash_storage_object(self, storage_path: str) -> str:
        from src.core.storage import storage_instance

        object_path = storage_path.replace(f"{storage_instance.bucket_name}/", "", 1)
        response = storage_instance.client.get_object(storage_instance.bucket_name, object_path)

        try:
            hasher = hashlib.sha256()
            for chunk in response.stream(amt=64 * 1024):
                hasher.update(chunk)
            return hasher.hexdigest()
        finally:
            response.close()
            response.release_conn()

    def build_integrity_payload(
        self,
        *,
        doc_id: str,
        validated_metadata: Dict[str, Any],
        confirmed_by: str,
        keep_original: bool,
        selected_pdf_path: Optional[str],
    ) -> Dict[str, Any]:
        confirmed_at = datetime.now(timezone.utc).isoformat()
        metadata_hash = self._hash_json(validated_metadata)
        pdf_hash = self._hash_storage_object(selected_pdf_path) if selected_pdf_path else None

        manifest = {
            "doc_id": doc_id,
            "confirmed_by": confirmed_by,
            "confirmed_at": confirmed_at,
            "keep_original": keep_original,
            "selected_pdf_path": selected_pdf_path,
            "hashes": {
                "validated_metadata_sha256": metadata_hash,
                "pdf_sha256": pdf_hash,
            },
            "signature_algorithm": "HMAC-SHA256",
        }

        canonical_manifest = json.dumps(
            manifest,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

        manifest_signature = self._sign(canonical_manifest)

        return {
            "manifest": manifest,
            "manifest_signature": manifest_signature,
        }

    def verify_integrity_payload(
        self,
        *,
        validated_metadata: Dict[str, Any],
        storage: Dict[str, Any],
        integrity: Dict[str, Any],
    ) -> Dict[str, Any]:
        manifest = (integrity or {}).get("manifest") or {}
        stored_signature = (integrity or {}).get("manifest_signature")

        if not manifest or not stored_signature:
            return {
                "is_valid": False,
                "signature_valid": False,
                "metadata_hash_valid": False,
                "pdf_hash_valid": False,
                "message": "No existe manifiesto/firma de integridad para verificar.",
            }

        canonical_manifest = json.dumps(
            manifest,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        recalculated_signature = self._sign(canonical_manifest)
        signature_valid = hmac.compare_digest(recalculated_signature, stored_signature)

        expected_metadata_hash = ((manifest.get("hashes") or {}).get("validated_metadata_sha256"))
        current_metadata_hash = self._hash_json(validated_metadata or {})
        metadata_hash_valid = expected_metadata_hash == current_metadata_hash

        expected_pdf_hash = ((manifest.get("hashes") or {}).get("pdf_sha256"))
        selected_pdf_path = manifest.get("selected_pdf_path") or (storage or {}).get("pdf_path")
        current_pdf_hash = self._hash_storage_object(selected_pdf_path) if selected_pdf_path else None
        pdf_hash_valid = expected_pdf_hash == current_pdf_hash

        is_valid = signature_valid and metadata_hash_valid and pdf_hash_valid
        return {
            "is_valid": is_valid,
            "signature_valid": signature_valid,
            "metadata_hash_valid": metadata_hash_valid,
            "pdf_hash_valid": pdf_hash_valid,
            "selected_pdf_path": selected_pdf_path,
            "message": "Integridad verificada correctamente." if is_valid else "Fallo en verificación de integridad.",
        }


integrity_service = IntegrityService()
