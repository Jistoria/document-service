import asyncio

from src.features.validation.models import ValidationRequest
from src.features.validation.service import ValidationService


async def _noop_validate_entity_object(*args, **kwargs):
    return None


class FakeDocumentsCollection:
    def get(self, key):
        return {"_key": key}


class FakeDB:
    def collection(self, name):
        if name == "documents":
            return FakeDocumentsCollection()
        raise KeyError(name)


def test_quality_check_rejects_string_for_entity_json_field(monkeypatch):
    service = ValidationService()
    service.get_db = lambda: FakeDB()

    schema = {
        "fields": [
            {
                "fieldKey": "tutor",
                "label": "Tutor",
                "isRequired": True,
                "dataType": "json",
                "entityType": {"key": "user"},
            }
        ]
    }

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: schema)
    monkeypatch.setattr("src.features.validation.service.validate_entity_object", _noop_validate_entity_object)

    payload = ValidationRequest(metadata={"tutor": "Juan Perez"})
    result = asyncio.run(service.dry_run_validation("doc-1", payload))

    assert result["is_ready"] is False
    report = result["fields_report"][0]
    assert report["is_valid"] is False
    assert "estructura de entidad/usuario" in report["warnings"][0]


def test_quality_check_accepts_dict_for_entity_json_field(monkeypatch):
    service = ValidationService()
    service.get_db = lambda: FakeDB()

    schema = {
        "fields": [
            {
                "fieldKey": "faculty",
                "label": "Facultad",
                "isRequired": True,
                "dataType": "json",
                "entityType": {"key": "faculty"},
            }
        ]
    }

    monkeypatch.setattr("src.features.validation.service.get_schema_for_document", lambda *_: schema)
    monkeypatch.setattr("src.features.validation.service.validate_entity_object", _noop_validate_entity_object)

    payload = ValidationRequest(metadata={"faculty": {"id": "f1", "name": "Ingenieria", "type": "faculty"}})
    result = asyncio.run(service.dry_run_validation("doc-1", payload))

    report = result["fields_report"][0]
    assert report["is_valid"] is True
