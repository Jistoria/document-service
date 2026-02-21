from src.features.ocr_updates.pipeline import context_naming


class DummyDB:
    pass


def test_build_context_names_without_required_document(monkeypatch):
    chain = [
        {"_key": "u", "name": "Universidad", "code": "ULEAM", "code_numeric": "174"},
        {"_key": "f", "name": "Facultad", "code": "FCVT", "code_numeric": "213"},
        {"_key": "c", "name": "Tecnologías de la Información", "code": "TDI", "code_numeric": "213.9"},
    ]

    monkeypatch.setattr(context_naming, "_now_tag", lambda: "20260217_055551")
    monkeypatch.setattr(context_naming, "get_context_chain", lambda *_args, **_kwargs: chain)

    naming = context_naming.build_context_names(DummyDB(), "c")

    assert naming["name_path"] == "Universidad / Facultad / Tecnologías de la Información"
    assert naming["code_path"] == "ULEAM / FCVT / TDI"
    assert naming["name_code"] == "FCVT-TDI - Tecnologías de la Información"
    assert naming["name_code_numeric"] == "213-213.9 - Tecnologías de la Información"
    assert naming["display_name"] == "FCVT-TDI - Tecnologías de la Información - 20260217_055551"
    assert naming["required_document_code"] is None


def test_build_context_names_with_required_document(monkeypatch):
    chain = [
        {"_key": "u", "name": "Universidad", "code": "ULEAM-MAT", "code_numeric": "174"},
        {"_key": "f", "name": "Facultad", "code": "FCVT", "code_numeric": "213"},
        {"_key": "c", "name": "Tecnologías de la Información", "code": "TDI", "code_numeric": "213.9"},
    ]

    monkeypatch.setattr(context_naming, "_now_tag", lambda: "20260217_055551")
    monkeypatch.setattr(context_naming, "get_context_chain", lambda *_args, **_kwargs: chain)

    naming = context_naming.build_context_names(
        DummyDB(),
        "c",
        required_document={
            "id": "doc-1",
            "name": "Registro Actividades Diarias del Estudiante",
            "code": "PAP-01-002",
        },
    )

    assert naming["name_path"] == (
        "Universidad / Facultad / Tecnologías de la Información / "
        "Registro Actividades Diarias del Estudiante"
    )
    assert naming["code_path"] == "ULEAM-MAT / FCVT / TDI / PAP-01-002"
    assert naming["code_numeric_path"] == "174 / 213 / 213.9"
    assert naming["name_code"] == "FCVT-TDI-PAP-01-002 - Registro Actividades Diarias del Estudiante"
    assert naming["name_code_numeric"] == "213-213.9 - Registro Actividades Diarias del Estudiante"
    assert naming["display_name"] == (
        "FCVT-TDI-PAP-01-002 - Registro Actividades Diarias del Estudiante - 20260217_055551"
    )
    assert naming["required_document_code"] == "PAP-01-002"
