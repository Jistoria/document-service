import asyncio

import pytest

from src.features.validation.service import ValidationService


class FakeRepo:
    def __init__(self, doc):
        self.doc = doc

    def get_document_integrity_snapshot(self, doc_id):
        if self.doc and self.doc.get("_key") == doc_id:
            return self.doc
        return None


class FakeIntegrityService:
    def __init__(self, result):
        self.result = result

    def verify_integrity_payload(self, **kwargs):
        return self.result


class DummyArchive:
    def promote_from_stage(self, doc_snapshot, storage_data):
        return storage_data


def test_verify_document_integrity_ok_for_owner():
    repo = FakeRepo(
        {
            "_key": "doc1",
            "owner_id": "u1",
            "is_public": False,
            "validated_metadata": {"a": {"value": "x"}},
            "storage": {"pdf_path": "documents-storage/archive/doc1/principal.pdf"},
            "integrity": {"manifest": {}, "manifest_signature": "sig"},
        }
    )
    service = ValidationService(repository=repo, integrity=FakeIntegrityService({
        "is_valid": True,
        "signature_valid": True,
        "metadata_hash_valid": True,
        "pdf_hash_valid": True,
        "message": "ok",
    }), archive=DummyArchive())

    result = asyncio.run(service.verify_document_integrity("doc1", "u1"))
    assert result["status"] == "success"
    assert result["integrity"]["is_valid"] is True


def test_verify_document_integrity_public_doc_allows_non_owner():
    repo = FakeRepo(
        {
            "_key": "doc1",
            "owner_id": "u1",
            "is_public": True,
            "validated_metadata": {},
            "storage": {},
            "integrity": {},
        }
    )
    service = ValidationService(repository=repo, integrity=FakeIntegrityService({
        "is_valid": False,
        "signature_valid": False,
        "metadata_hash_valid": False,
        "pdf_hash_valid": False,
        "message": "missing",
    }), archive=DummyArchive())

    result = asyncio.run(service.verify_document_integrity("doc1", "u2"))
    assert result["status"] == "warning"


def test_verify_document_integrity_rejects_non_owner_private_doc():
    repo = FakeRepo(
        {
            "_key": "doc1",
            "owner_id": "u1",
            "is_public": False,
            "validated_metadata": {},
            "storage": {},
            "integrity": {},
        }
    )
    service = ValidationService(repository=repo, integrity=FakeIntegrityService({}), archive=DummyArchive())

    with pytest.raises(PermissionError):
        asyncio.run(service.verify_document_integrity("doc1", "u2"))
