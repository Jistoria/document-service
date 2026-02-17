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
    ):
        doc = self.docs[doc_id]
        display_name_changed = display_name is not None and display_name != doc.get("display_name")

        if display_name_changed:
            if doc.get("snap_context_name") is None:
                doc["snap_context_name"] = doc.get("display_name")
            doc["display_name"] = display_name

        doc["validated_metadata"] = clean_metadata
        doc["status"] = "confirmed"
        doc["confirmed_by"] = confirmed_by
        doc["is_public"] = is_public
        doc["keep_original"] = keep_original
        doc["integrity"] = integrity_payload
        return doc


class FakeIntegrityService:
    def build_integrity_payload(self, **kwargs):
        return {"manifest": kwargs, "manifest_signature": "fake-signature"}


def test_confirm_without_display_name_keeps_names(monkeypatch):
    docs = {"doc1": {"owner_id": "u1", "display_name": "Nombre Institucional - 20260216_222908"}}
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService())
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
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService())
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
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService())
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
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService())
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
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService())
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, is_public=True, keep_original=True)
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["keep_original"] is True
    assert docs["doc1"]["integrity"]["manifest_signature"] == "fake-signature"


def test_confirm_with_keep_original_true_requires_original_pdf(monkeypatch):
    docs = {
        "doc1": {
            "owner_id": "u1",
            "display_name": "Nombre",
            "storage": {"pdf_path": "documents-storage/stage/doc1/ocr_pdfa.pdf"},
        }
    }
    service = ValidationService(repository=FakeRepo(docs), integrity=FakeIntegrityService())
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, is_public=True, keep_original=True)

    with pytest.raises(ValueError, match="PDF original"):
        asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))
