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
        }

    def confirm_document(self, *, doc_id, clean_metadata, is_public, display_name, confirmed_by):
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
        return doc


def test_confirm_without_display_name_keeps_names(monkeypatch):
    docs = {"doc1": {"owner_id": "u1", "display_name": "Nombre Institucional - 20260216_222908"}}
    service = ValidationService(repository=FakeRepo(docs))
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, is_public=False)
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["display_name"] == "Nombre Institucional - 20260216_222908"
    assert docs["doc1"].get("snap_context_name") is None


def test_confirm_with_different_display_name_sets_snap_and_updates_name(monkeypatch):
    docs = {"doc1": {"owner_id": "u1", "display_name": "Nombre Institucional - 20260216_222908"}}
    service = ValidationService(repository=FakeRepo(docs))
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
    service = ValidationService(repository=FakeRepo(docs))
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, display_name="Nombre actual", is_public=True)
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["display_name"] == "Nombre actual"
    assert docs["doc1"].get("snap_context_name") is None


def test_confirm_sets_is_public(monkeypatch):
    docs = {"doc1": {"owner_id": "u1", "display_name": "Nombre"}}
    service = ValidationService(repository=FakeRepo(docs))
    service._entities_service = FakeEntitiesService()
    service.get_db = lambda: None

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: None)
    monkeypatch.setattr("src.features.validation.service.sanitize_metadata", lambda metadata, allowed_keys=None: metadata)

    payload = ValidationConfirmRequest(metadata={"campo": "valor"}, is_public=True)
    asyncio.run(service.confirm_validation("doc1", payload, current_user_id="u1"))

    assert docs["doc1"]["is_public"] is True
