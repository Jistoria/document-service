from src.features.validation.archive_service import ArchiveService


class FakeClient:
    def __init__(self):
        self.copies = []
        self.removals = []

    def copy_object(self, bucket_name, dst_object, copy_source):
        self.copies.append(
            {
                "bucket": bucket_name,
                "dst": dst_object,
                "src_bucket": copy_source.bucket_name,
                "src": copy_source.object_name,
            }
        )

    def remove_object(self, bucket_name, object_name):
        self.removals.append((bucket_name, object_name))


class FakeStorage:
    bucket_name = "documents-storage"

    def __init__(self):
        self.client = FakeClient()


def test_promote_from_stage_removes_each_source_once_when_reused(monkeypatch):
    fake_storage = FakeStorage()
    service = ArchiveService()
    monkeypatch.setattr(service, "_get_storage_instance", lambda: fake_storage)

    doc_snapshot = {
        "_key": "task_1",
        "naming": {"code_path": "ULEAM / FCVT / TDI"},
        "context_snapshot": {"required_doc_code": "PAP-01-002"},
        "process": {"code": "PAP"},
    }
    storage_data = {
        "pdf_path": "documents-storage/stage-validate/u1/task_1/pdf_original_path_document.pdf",
        "pdf_original_path": "documents-storage/stage-validate/u1/task_1/pdf_original_path_document.pdf",
    }

    promoted = service.promote_from_stage(doc_snapshot, storage_data)

    # Se copian ambos destinos esperados desde el mismo origen
    copied_dsts = [c["dst"] for c in fake_storage.client.copies]
    assert copied_dsts[0].endswith("/principal.pdf")
    assert copied_dsts[1].endswith("/original.pdf")

    copied_srcs = {c["src"] for c in fake_storage.client.copies}
    assert copied_srcs == {"stage-validate/u1/task_1/pdf_original_path_document.pdf"}

    # El origen compartido se elimina solo una vez, después de copiar todo
    assert fake_storage.client.removals == [
        ("documents-storage", "stage-validate/u1/task_1/pdf_original_path_document.pdf")
    ]

    assert promoted["pdf_path"].startswith("documents-storage/archive/")
    assert promoted["pdf_original_path"].startswith("documents-storage/archive/")
