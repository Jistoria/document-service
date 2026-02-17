import asyncio

import pytest

from src.features.validation.entities_service import EntitiesService
from src.features.validation.validators import validate_entity_object


class DummyUsersService:
    async def find_or_create_user(self, *args, **kwargs):
        return None

    async def create_new_user_node(self, *args, **kwargs):
        return "u1"

    async def verify_user_exists(self, *args, **kwargs):
        return False

    def build_metadata_from_user(self, user_doc):
        return user_doc


class FakeCollection:
    def __init__(self, keys):
        self.keys = set(keys)

    def has(self, key):
        return key in self.keys


class FakeDB:
    def __init__(self, entities_keys=None):
        self._entities = FakeCollection(entities_keys or [])

    def collection(self, name):
        if name == "entities":
            return self._entities
        return FakeCollection([])


def test_ensure_entities_exist_requires_existing_non_user_entity_id():
    service = EntitiesService(DummyUsersService())
    metadata = {
        "career": {
            "value": {
                "name": "Tecnologías de la Información",
                "type": "career",
            }
        }
    }
    schema = {"fields": [{"fieldKey": "career", "entityType": {"key": "career"}}]}

    with pytest.raises(ValueError, match="id obligatorio"):
        asyncio.run(service.ensure_entities_exist(FakeDB(), metadata, schema=schema))


def test_ensure_entities_exist_rejects_unknown_non_user_entity_id():
    service = EntitiesService(DummyUsersService())
    metadata = {
        "career": {
            "value": {
                "id": "missing-id",
                "name": "Tecnologías de la Información",
                "type": "career",
            }
        }
    }
    schema = {"fields": [{"fieldKey": "career", "entityType": {"key": "career"}}]}

    with pytest.raises(ValueError, match="no existe"):
        asyncio.run(service.ensure_entities_exist(FakeDB(entities_keys=[]), metadata, schema=schema))


def test_validate_entity_object_marks_non_user_without_id_as_invalid():
    report = {"is_valid": True, "warnings": [], "actions": []}

    asyncio.run(
        validate_entity_object(
            FakeDB(entities_keys=[]),
            {"name": "Tecnologías de la Información", "type": "career"},
            "career",
            report,
        )
    )

    assert report["is_valid"] is False
    assert any("sin id" in warning for warning in report["warnings"])
    assert "CREATE_ENTITY" not in report["actions"]
