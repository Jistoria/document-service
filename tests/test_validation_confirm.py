import asyncio

import pytest

from src.features.validation.models import ValidationConfirmRequest
from src.features.validation.service import ValidationService


class FakeEntitiesService:
    async def ensure_entities_exist(self, db, raw_metadata, schema=None):
        return raw_metadata

    def add_semantic_relation(self, db, task_id, entity_id, relation):
        return None


class FakeRepo:
    def __init__(self, docs):
        self.docs = docs

    def get_document_snapshot(self, doc_id):
        doc = self.docs.get(doc_id)
        if not doc:
            return None
        return {
            "_key": doc_id,
            "owner_id": doc["owner_id"],
            "display_name": doc.get("display_name"),
            "snap_context_name": doc.get("snap_context_name"),
            "storage": doc.get("storage", {}),
            "naming": doc.get("naming", {}),
            "context_snapshot": doc.get("context_snapshot", {}),
            "process": doc.get("process", {}),
        }

    def confirm_document(
        self,
        *,
        doc_id,
        clean_metadata,
        is_public,
        display_name,
        confirmed_by,
        keep_original,
        integrity_payload,
        storage_data,
    ):
        doc = self.docs[doc_id]
        current_display_name = doc.get("display_name") or (doc.get("naming") or {}).get("display_name")
        display_name_changed = display_name is not None and display_name != current_display_name

        if display_name_changed:
            if doc.get("snap_context_name") is None:
                doc["snap_context_name"] = current_display_name
            doc["display_name"] = display_name
        elif doc.get("display_name") is None and current_display_name is not None:
            doc["display_name"] = current_display_name

        doc["validated_metadata"] = clean_metadata
        doc["status"] = "confirmed"
        doc["confirmed_by"] = confirmed_by
        doc["is_public"] = is_public
        doc["keep_original"] = keep_original
        doc["integrity"] = integrity_payload
        doc["storage"] = storage_data
        doc["naming"] = {
            **doc.get("naming", {}),
            "display_name": doc.get("display_name"),
        }
        return doc


class FakeIntegrityService:
    def build_integrity_payload(self, **kwargs):
        return {"manifest": kwargs, "manifest_signature": "fake-signature"}


class FakeArchiveService:
    def promote_from_stage(self, doc_snapshot, storage_data):
        promoted = dict(storage_data)
        promoted["pdf_path"] = promoted.get("pdf_path", "").replace("stage-validate/", "archive/").replace("stage/", "archive/")
        if promoted.get("json_path"):
            promoted["json_path"] = promoted["json_path"].replace("stage-validate/", "archive/").replace("stage/", "archive/")
        if promoted.get("text_path"):
            promoted["text_path"] = promoted["text_path"].replace("stage-validate/", "archive/").replace("stage/", "archive/")
        if promoted.get("pdf_original_path"):
            promoted["pdf_original_path"] = promoted["pdf_original_path"].replace("stage-validate/", "archive/").replace("stage/", "archive/")
        promoted["archive_prefix"] = "archive/fake"
        promoted["storage_tier"] = "archive"
        return promoted


def test_confirm_without_display_name_keeps_names(monkeypatch):
    docs = {"doc1": {"owner_id": "u1", "display_name": "Nombre Institucional - 20260216_222908"}}
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService(), archive=FakeArchiveService())
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, is_public=False)
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["display_name"] == "Nombre Institucional - 20260216_222908"
    assert docs["doc1"].get("snap_context_name") is None


def test_confirm_with_different_display_name_sets_snap_and_updates_name(monkeypatch):
    docs = {
        "doc1": {
            "owner_id": "u1",
            "display_name": "Nombre Institucional - 20260216_222908",
            "storage": {"pdf_path": "documents-storage/stage/doc1/pdf.pdf"},
        }
    }
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService(), archive=FakeArchiveService())
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, display_name="Nombre Amigable", is_public=True)
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["display_name"] == "Nombre Amigable"
    assert docs["doc1"]["snap_context_name"] == "Nombre Institucional - 20260216_222908"


def test_confirm_with_same_display_name_does_not_set_snap(monkeypatch):
    docs = {"doc1": {"owner_id": "u1", "display_name": "Nombre actual"}}
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService(), archive=FakeArchiveService())
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, display_name="Nombre actual", is_public=True)
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["display_name"] == "Nombre actual"
    assert docs["doc1"].get("snap_context_name") is None


def test_confirm_sets_is_public(monkeypatch):
    docs = {
        "doc1": {
            "owner_id": "u1",
            "display_name": "Nombre",
            "storage": {
                "pdf_path": "documents-storage/stage/doc1/ocr_pdfa.pdf",
                "pdf_original_path": "documents-storage/stage/doc1/original.pdf",
            },
        }
    }
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService(), archive=FakeArchiveService())
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, is_public=True)
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["is_public"] is True


def test_confirm_with_keep_original_true_sets_flag_and_integrity(monkeypatch):
    docs = {
        "doc1": {
            "owner_id": "u1",
            "display_name": "Nombre",
            "storage": {
                "pdf_path": "documents-storage/stage/doc1/ocr_pdfa.pdf",
                "pdf_original_path": "documents-storage/stage/doc1/original.pdf",
            },
        }
    }
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService(), archive=FakeArchiveService())
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, is_public=True, keep_original=True)
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["keep_original"] is True
    assert docs["doc1"]["integrity"]["manifest_signature"] == "fake-signature"
    assert docs["doc1"]["storage"]["storage_tier"] == "archive"
    assert docs["doc1"]["storage"]["pdf_path"].startswith("documents-storage/archive/")


def test_confirm_with_keep_original_true_requires_original_pdf(monkeypatch):
    docs = {
        "doc1": {
            "owner_id": "u1",
            "display_name": "Nombre",
            "storage": {"pdf_path": "documents-storage/stage/doc1/ocr_pdfa.pdf"},
        }
    }
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService(), archive=FakeArchiveService())
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, is_public=True, keep_original=True)

    with pytest.raises(ValueError, match="PDF original"):
        asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))


def test_confirm_persists_display_name_and_is_public_outside_metadata(monkeypatch):
    docs = {
        "doc1": {
            "owner_id": "u1",
            "display_name": "Nombre institucional",
            "naming": {"display_name": "Nombre institucional"},
            "storage": {
                "pdf_path": "documents-storage/stage/doc1/ocr_pdfa.pdf",
                "pdf_original_path": "documents-storage/stage/doc1/original.pdf",
            },
            "validated_metadata": {"author": {"value": "Viejo"}},
        }
    }
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService(), archive=FakeArchiveService())
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(
        metadata={"author": {"value": "Nuevo"}},
        display_name="Nombre usuario final",
        is_public=True,
        keep_original=False,
    )
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["display_name"] == "Nombre usuario final"
    assert docs["doc1"]["naming"]["display_name"] == "Nombre usuario final"
    assert docs["doc1"]["is_public"] is True
    assert docs["doc1"]["validated_metadata"] == {"author": {"value": "Nuevo"}}


def test_confirm_sets_snap_when_original_name_only_exists_in_naming(monkeypatch):
    docs = {
        "doc1": {
            "owner_id": "u1",
            "display_name": None,
            "naming": {"display_name": "FCVT-TDI - Tecnologías de la Información - 20260217_050249"},
            "storage": {
                "pdf_path": "documents-storage/stage/doc1/ocr_pdfa.pdf",
                "pdf_original_path": "documents-storage/stage/doc1/original.pdf",
            },
        }
    }
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService(), archive=FakeArchiveService())
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(
        metadata={"author": {"value": "Nuevo"}},
        display_name="Nombre personalizado final",
        is_public=False,
        keep_original=False,
    )
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["display_name"] == "Nombre personalizado final"
    assert docs["doc1"]["snap_context_name"] == "FCVT-TDI - Tecnologías de la Información - 20260217_050249"
