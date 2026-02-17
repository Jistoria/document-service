import asyncio

from src.features.validation.users_service import UsersService
from src.features.validation.validators import validate_entity_object


class FakeCollection:
    def __init__(self, keys=None):
        self.keys = set(keys or [])

    def has(self, key):
        return key in self.keys


class FakeAQL:
    def __init__(self, users):
        self.users = users

    def execute(self, query, bind_vars=None):
        bind_vars = bind_vars or {}
        user_id = bind_vars.get("user_id")
        normalized_key = bind_vars.get("normalized_key")
        entity_id = bind_vars.get("entity_id")

        # users_service.verify_user_exists
        if user_id is not None:
            matches = [
                u for u in self.users
                if u.get("_key") == user_id
                or (normalized_key and u.get("_key") == normalized_key)
                or u.get("guid_ms") == user_id
                or (normalized_key and u.get("guid_ms") == normalized_key)
                or u.get("user_id") == user_id
            ]
            return matches[:1]

        # validators.validate_entity_object fallback for users
        if entity_id is not None:
            matches = [
                u for u in self.users
                if u.get("guid_ms") == entity_id
                or (normalized_key and u.get("guid_ms") == normalized_key)
                or u.get("user_id") == entity_id
            ]
            return [m.get("_key") for m in matches[:1]]

        return []


class FakeDB:
    def __init__(self, users):
        self.users = users
        self.aql = FakeAQL(users)

    def collection(self, name):
        if name == "dms_users":
            # fuerza fallback AQL (simula que el id recibido no está como _key)
            return FakeCollection(keys=[])
        return FakeCollection(keys=[])


class DummyGraphClient:
    async def search_user(self, *args, **kwargs):
        return None


def test_verify_user_exists_accepts_guid_ms_with_hyphens():
    users = [
        {
            "_key": "e0cfa1ba143f4141afef4c6e92209197",
            "guid_ms": "e0cfa1ba-143f-4141-afef-4c6e92209197",
            "user_id": "legacy-local-id",
        }
    ]
    service = UsersService(DummyGraphClient())

    exists = asyncio.run(service.verify_user_exists(FakeDB(users), "e0cfa1ba-143f-4141-afef-4c6e92209197"))
    assert exists is True


def test_validate_entity_object_user_guid_ms_does_not_mark_as_new():
    users = [
        {
            "_key": "886af191ce274870a6c8494ea7f24c37",
            "guid_ms": "886af191-ce27-4870-a6c8-494ea7f24c37",
            "user_id": "auth-service-user-id",
        }
    ]
    report = {"is_valid": True, "warnings": [], "actions": []}

    asyncio.run(
        validate_entity_object(
            FakeDB(users),
            {
                "id": "886af191-ce27-4870-a6c8-494ea7f24c37",
                "display_name": "SANTANA CEDEÑO HIRAIDA MONSERRATE",
                "email": "hiraida.santana@uleam.edu.ec",
            },
            "user",
            report,
        )
    )

    assert report["is_valid"] is True
    assert report["warnings"] == []
    assert report["actions"] == []
