"""Autoevaluación de ingest.py (no necesita AWS ni Spark). No modifiques este archivo."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))

from ingest import raw_prefix, to_json_lines  # noqa: E402


def test_raw_prefix_formato_hive():
    assert raw_prefix("transactions", "2026-09-30") == "raw/transactions/ingest_date=2026-09-30/"
    assert raw_prefix("customers", "2026-10-01") == "raw/customers/ingest_date=2026-10-01/"


@pytest.mark.parametrize("bad", ["30-09-2026", "2026-13-01", "2026/09/30", "ayer"])
def test_raw_prefix_rechaza_fechas_invalidas(bad):
    with pytest.raises(ValueError):
        raw_prefix("transactions", bad)


def test_json_lines_un_objeto_por_linea():
    records = [{"id": 1, "city": "Medellín"}, {"id": 2, "city": None}]
    lines = to_json_lines(records).strip().split("\n")
    assert len(lines) == 2
    assert [json.loads(line) for line in lines] == records


def test_json_lines_conserva_tildes():
    assert "Medellín" in to_json_lines([{"city": "Medellín"}])
