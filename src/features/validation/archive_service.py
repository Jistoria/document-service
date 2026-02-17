import re
from typing import Any, Dict, Optional

from minio.commonconfig import CopySource


class ArchiveService:
    def _slug(self, value: Optional[str]) -> str:
        if not value:
            return "na"
        cleaned = re.sub(r"\s+", "-", str(value).strip().lower())
        cleaned = re.sub(r"[^a-z0-9\-_]", "", cleaned)
        return cleaned or "na"

    def _object_name(self, storage_path: str) -> str:
        from src.core.storage import storage_instance

        return storage_path.replace(f"{storage_instance.bucket_name}/", "", 1)

    def _build_archive_prefix(self, doc_snapshot: Dict[str, Any]) -> str:
        naming = doc_snapshot.get("naming") or {}
        context = doc_snapshot.get("context_snapshot") or {}
        process = doc_snapshot.get("process") or {}

        code_path = naming.get("code_path") or naming.get("name_path") or context.get("entity_name") or "general"
        context_segments = [self._slug(segment) for segment in str(code_path).split("/") if str(segment).strip()]
        if not context_segments:
            context_segments = ["general"]

        process_seg = self._slug(process.get("code") or process.get("name") or "sin-proceso")
        required_seg = self._slug(context.get("required_doc_code") or context.get("required_doc_name") or "sin-documento")

        return f"archive/{'/'.join(context_segments)}/{process_seg}/{required_seg}/{doc_snapshot['_key']}"

    def promote_from_stage(self, doc_snapshot: Dict[str, Any], storage_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Copia archivos desde stage-validate a ruta archivística según grafo/contexto.
        Retorna storage actualizado con nuevas rutas y metadatos de promoción.
        """
        from src.core.storage import storage_instance

        prefix = self._build_archive_prefix(doc_snapshot)
        updated = dict(storage_data or {})

        mapping = {
            "pdf_path": "principal.pdf",
            "json_path": "metadata.json",
            "text_path": "extracted.txt",
            "pdf_original_path": "original.pdf",
        }

        for key, filename in mapping.items():
            src = updated.get(key)
            if not src:
                continue

            src_object = self._object_name(src)
            dst_object = f"{prefix}/{filename}"

            storage_instance.client.copy_object(
                storage_instance.bucket_name,
                dst_object,
                CopySource(storage_instance.bucket_name, src_object),
            )

            # Si viene de stage, lo removemos para completar el "move"
            if src_object.startswith("stage-validate/"):
                try:
                    storage_instance.client.remove_object(storage_instance.bucket_name, src_object)
                except Exception:
                    pass

            updated[key] = f"{storage_instance.bucket_name}/{dst_object}"

        updated["archive_prefix"] = prefix
        updated["storage_tier"] = "archive"

        return updated


archive_service = ArchiveService()
