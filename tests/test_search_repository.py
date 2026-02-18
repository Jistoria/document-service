from src.features.search.repository import SearchRepository


class FakeAQL:
    def __init__(self):
        self.last_query = None
        self.last_bind_vars = None

    def execute(self, query, bind_vars=None):
        self.last_query = query
        self.last_bind_vars = bind_vars or {}
        return iter([{"items": [], "total": 0}])


class FakeDB:
    def __init__(self):
        self.aql = FakeAQL()


def test_repository_applies_public_or_team_scope_filter():
    repo = SearchRepository.__new__(SearchRepository)
    fake_db = FakeDB()
    repo.db = fake_db

    repo.search(
        offset=0,
        limit=10,
        filters={
            "enforce_team_scope": True,
            "valid_owner_ids": ["entity-1"],
            "metadata_filters": {},
            "process_ids": [],
        },
    )

    assert "TO_BOOL(doc.is_public)" in fake_db.aql.last_query
    assert "owner._key IN @valid_owner_ids" in fake_db.aql.last_query
    assert fake_db.aql.last_bind_vars["valid_owner_ids"] == ["entity-1"]


def test_repository_applies_public_only_when_team_scope_without_entities():
    repo = SearchRepository.__new__(SearchRepository)
    fake_db = FakeDB()
    repo.db = fake_db

    repo.search(
        offset=0,
        limit=10,
        filters={
            "enforce_team_scope": True,
            "valid_owner_ids": [],
            "metadata_filters": {},
            "process_ids": [],
        },
    )

    assert "TO_BOOL(doc.is_public)" in fake_db.aql.last_query
    assert fake_db.aql.last_bind_vars["valid_owner_ids"] == []
