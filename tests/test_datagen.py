import json

from utilization import datagen


def _read(path):
    return path.read_bytes()


def test_datagen_is_deterministic(tmp_path):
    datagen.generate(tmp_path / "a")
    datagen.generate(tmp_path / "b")
    files_a = sorted(p.relative_to(tmp_path / "a") for p in (tmp_path / "a").rglob("*.*"))
    assert files_a == sorted(p.relative_to(tmp_path / "b") for p in (tmp_path / "b").rglob("*.*"))
    for rel in files_a:
        assert _read(tmp_path / "a" / rel) == _read(tmp_path / "b" / rel), rel


def test_datagen_plants_the_documented_defects(tmp_path):
    datagen.generate(tmp_path)
    exports = sorted((tmp_path / "timesheets").glob("*.json"))
    assert len(exports) == 4

    parsed, malformed = [], 0
    for path in exports:
        for line in path.read_text().splitlines():
            try:
                parsed.append((path.name, json.loads(line)))
            except json.JSONDecodeError:
                malformed += 1
    rows = [r for _, r in parsed]

    assert malformed == 1
    assert any(isinstance(r["hours"], str) and "," in r["hours"] for r in rows)
    assert any(r.get("is_deleted") for r in rows)
    assert {name for name, r in parsed if "is_deleted" in r} == {"timesheets_2026-09-21.json"}
    assert {name for name, r in parsed if "activity_type" in r} == {"timesheets_2026-09-28.json"}
    assert len([r for r in rows if r["entry_id"] == "TE-900001"]) == 2
    ids = [r["entry_id"] for r in rows]
    assert len(ids) > len(set(ids)), "re-sent rows and corrections share entry_id"

    fx = (tmp_path / "reference" / "fx_rates.csv").read_text()
    assert "USD" not in fx and "2026-08-31,GBP" not in fx
