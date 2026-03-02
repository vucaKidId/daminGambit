import json
import os
from collections import Counter
from decimal import Decimal
from pathlib import Path
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from damin_gambit.db import DbConfig, query_events, reset_db, seed_db, seed_sports_db
from damin_gambit.nl import parse as rule_parse
from damin_gambit.webapp import app


@pytest.fixture(scope="session")
def client():
    return TestClient(app)


@pytest.fixture()
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "events.sqlite3"


@pytest.fixture()
def seeded_db(db_path: Path) -> Path:
    cfg = DbConfig(path=db_path)
    reset_db(cfg)
    seed_db(cfg, rows=200, replace=True)
    seed_sports_db(cfg, replace=False)  # add football/basketball/handball for sport_limits and include_teams queries
    return db_path


def _load_cases() -> list[dict]:
    here = Path(__file__).resolve().parent
    return json.loads((here / "prompt_cases.json").read_text("utf-8"))


@pytest.mark.parametrize("case", _load_cases(), ids=lambda c: c.get("name", "case"))
def test_prompt_case_rules_fallback(case: dict, client: TestClient, seeded_db: Path, monkeypatch):
    # Make tests deterministic and not dependent on Ollama.
    monkeypatch.setenv("DAMIN_GAMBIT_USE_LLM", "0")
    monkeypatch.setenv("DAMIN_GAMBIT_REQUIRE_LLM", "0")
    # Freeze "now" so end_time filtering doesn't depend on wall-clock time.
    monkeypatch.setenv("DAMIN_GAMBIT_NOW", "2024-01-01 00:00:00")

    text = case["text"]
    expect = case["expect"]

    r = client.post("/api/query", json={"text": text, "explain": True, "db_path": str(seeded_db)})
    if "http_status" in expect:
        assert r.status_code == int(expect["http_status"]), r.text
        if "detail_contains" in expect:
            detail = (r.json() or {}).get("detail", "")
            assert str(expect["detail_contains"]) in str(detail)
        return

    assert r.status_code == 200, r.text
    data = r.json()

    results = data.get("results", [])
    if "rows_min" in expect or "rows_max" in expect:
        if "rows_min" in expect:
            assert len(results) >= expect["rows_min"], f"Expected at least {expect['rows_min']} results, got {len(results)}"
        if "rows_max" in expect:
            assert len(results) <= expect["rows_max"], f"Expected at most {expect['rows_max']} results, got {len(results)}"
    else:
        assert len(results) == expect["rows"], f"Expected {expect['rows']} results, got {len(results)}"

    # ensure one row per match (no tripling)
    if expect.get("unique_event_names"):
        names = [row.get("event_name") for row in results]
        assert len(names) == len(set(names))

    # ensure certain rows are included
    if "must_include" in expect:
        for inc in expect["must_include"]:
            want_name = str(inc.get("event_name", "")).strip().lower()
            want_type = inc.get("type")
            found = False
            for row in results:
                if str(row.get("event_name", "")).strip().lower() != want_name:
                    continue
                if want_type is not None and str(row.get("type", "")).strip().lower() != str(want_type).strip().lower():
                    continue
                found = True
                break
            assert found, f"Expected to include {inc}, got {[ (r.get('event_name'), r.get('type')) for r in results ]}"

    # validate per-country quotas
    if "country_counts" in expect:
        counts = Counter([row.get("country") for row in results])
        for k, v in expect["country_counts"].items():
            assert counts[k] == v

    # validate per-sport quotas
    if "sport_counts" in expect:
        counts = Counter([row.get("sport") for row in results])
        for k, v in expect["sport_counts"].items():
            assert counts.get(k) == v, f"sport_counts: expected {k}={v}, got {dict(counts)}"

    # validate value range
    if "value_min" in expect and "value_max" in expect:
        vmin = float(expect["value_min"])
        vmax = float(expect["value_max"])
        vals = [float(row["value"]) for row in results]
        if "value_range_count" in expect:
            cnt = sum(1 for v in vals if vmin <= v <= vmax)
            assert cnt == int(expect["value_range_count"])
        else:
            assert all(vmin <= v <= vmax for v in vals)
    elif "value_min" in expect:
        vmin = float(expect["value_min"])
        exclusive = bool(expect.get("value_min_exclusive", False))
        vals = [float(row["value"]) for row in results]
        assert all(v > vmin for v in vals) if exclusive else all(v >= vmin for v in vals)
    elif "value_max" in expect:
        vmax = float(expect["value_max"])
        exclusive = bool(expect.get("value_max_exclusive", False))
        vals = [float(row["value"]) for row in results]
        assert all(v < vmax for v in vals) if exclusive else all(v <= vmax for v in vals)

    # validate outcome type (won/draw/lost) if requested
    if "type" in expect:
        assert all((row.get("type") or "").lower() == str(expect["type"]).lower() for row in results)

    # validate total product range (API computes product over returned values); skip when 0 results
    if results and ("total_product_min" in expect or "total_product_max" in expect):
        totals = data.get("totals") or {}
        prod_s = totals.get("product")
        assert prod_s is not None
        prod = Decimal(str(prod_s))
        if "total_product_min" in expect and "total_product_max" in expect:
            assert prod >= Decimal(str(expect["total_product_min"]))
            assert prod <= Decimal(str(expect["total_product_max"]))
        elif "total_product_min" in expect:
            bound = Decimal(str(expect["total_product_min"]))
            if expect.get("total_product_min_exclusive", False):
                assert prod > bound
            else:
                assert prod >= bound
        elif "total_product_max" in expect:
            bound = Decimal(str(expect["total_product_max"]))
            if expect.get("total_product_max_exclusive", False):
                assert prod < bound
            else:
                assert prod <= bound

    # validate start time window if specified
    if "start_date" in expect and "start_time_min" in expect and "start_time_max" in expect:
        date_s = expect["start_date"]
        tmin_s = expect["start_time_min"]
        tmax_s = expect["start_time_max"]
        d = datetime.strptime(date_s, "%Y-%m-%d").date()
        tmin = datetime.strptime(tmin_s, "%H:%M").time()
        tmax = datetime.strptime(tmax_s, "%H:%M").time()
        for row in results:
            st = datetime.fromisoformat(row["start_time"])
            assert st.date() == d
            assert tmin <= st.time() <= tmax

    # validate "starts after" constraint if specified
    if "start_date" in expect and "start_time_after" in expect:
        date_s = expect["start_date"]
        after_s = expect["start_time_after"]
        d = datetime.strptime(date_s, "%Y-%m-%d").date()
        after_t = datetime.strptime(after_s, "%H:%M").time()
        for row in results:
            st = datetime.fromisoformat(row["start_time"])
            assert st.date() == d
            assert st.time() >= after_t

    # validate submission-time constraints if specified
    if "submitted_at" in expect:
        sub = datetime.fromisoformat(str(expect["submitted_at"]))
        if expect.get("not_ended_before_submit", False):
            for row in results:
                et = datetime.fromisoformat(row["end_time"])
                assert et >= sub
        if expect.get("not_started_after_submit", False):
            for row in results:
                st = datetime.fromisoformat(row["start_time"])
                assert st <= sub


def test_england_3_matches_total_odd_3_6_parser_spec():
    """Parser must produce england (quota or group), total 3-6, limit 3 for the England query."""
    spec = rule_parse("I need 3 matches from Engleand 1 league with total odd betwen 3-6")
    # Quota form: country_limits + group_names; or single group_name
    assert (spec.group_name == "england" or (spec.group_names == ["england"] and spec.country_limits == {"england": 3})), spec
    assert spec.total_product_min == 3.0, spec
    assert spec.total_product_max == 6.0, spec
    assert spec.limit == 3, spec
    assert spec.distinct_matches is True, spec


def test_england_3_matches_total_odd_3_6_direct_query(seeded_db: Path):
    """Direct query_events with parsed spec and frozen now must return 3 England matches."""
    from datetime import datetime
    cfg = DbConfig(path=seeded_db)
    spec = rule_parse("I need 3 matches from Engleand 1 league with total odd betwen 3-6")
    now = datetime(2026, 1, 1)
    events = query_events(
        cfg,
        group_name=spec.group_name,
        group_names=spec.group_names,
        country_limits=spec.country_limits,
        total_product_min=spec.total_product_min,
        total_product_max=spec.total_product_max,
        limit=spec.limit,
        distinct_matches=spec.distinct_matches,
        end_time_from=now,
    )
    assert len(events) == 3, f"Expected 3 events, got {len(events)}"
    assert len(set(e.event_name for e in events)) == 3
    for e in events:
        assert (e.group_name or "").strip().lower() == "england"


def test_england_3_matches_total_odd_3_6_rules(client: TestClient, seeded_db: Path, monkeypatch):
    """Rules interpreter must return 3 England matches with total odds between 3 and 6."""
    monkeypatch.setenv("DAMIN_GAMBIT_USE_LLM", "0")
    monkeypatch.setenv("DAMIN_GAMBIT_REQUIRE_LLM", "0")
    monkeypatch.setenv("DAMIN_GAMBIT_NOW", "2026-01-01 00:00:00")

    r = client.post(
        "/api/query",
        json={
            "text": "I need 3 matches from Engleand 1 league with total odd betwen 3-6",
            "explain": True,
            "db_path": str(seeded_db),
        },
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data.get("interpreter") == "rules", data
    results = data.get("results", [])
    assert len(results) == 3, f"Expected 3 results, got {len(results)}. message={data.get('message')} spec={data.get('spec')}"
    assert len(set(row.get("event_name") for row in results)) == 3
    for row in results:
        assert (row.get("country") or "").strip().lower() == "england", row
    prod_s = (data.get("totals") or {}).get("product")
    assert prod_s is not None, data
    assert Decimal("3") <= Decimal(str(prod_s)) <= Decimal("6")


def test_sport_limits_direct_query(seeded_db: Path):
    """Direct query_events with sport_limits returns 6 (3 football + 3 basketball) with value_max."""
    from datetime import datetime
    cfg = DbConfig(path=seeded_db)
    spec = rule_parse("3 utakmice iz fudbala i tri iz kosarke sa ukupnom kvotom 11 i pojedinacnom kovtom ne vecom od 3")
    assert spec.sport_limits == {"football": 3, "basketball": 3}, spec.sport_limits
    now = datetime(2024, 1, 1)
    events = query_events(
        cfg,
        sport_limits=spec.sport_limits,
        value_max=spec.value_max,
        value_max_inclusive=spec.value_max_inclusive,
        limit=spec.limit,
        distinct_matches=spec.distinct_matches,
        end_time_from=now,
    )
    assert len(events) == 6, f"Expected 6 events, got {len(events)}"
    from collections import Counter
    sport_counts = Counter([e.sport for e in events])
    assert sport_counts.get("football") == 3, sport_counts
    assert sport_counts.get("basketball") == 3, sport_counts


def test_nonsense_is_friendly_even_when_llm_required(client: TestClient, seeded_db: Path, monkeypatch):
    # In production-like mode (LLM required), nonsense queries should still return a friendly 422,
    # not a technical "LLM required..." error.
    monkeypatch.setenv("DAMIN_GAMBIT_USE_LLM", "1")
    monkeypatch.setenv("DAMIN_GAMBIT_REQUIRE_LLM", "1")
    monkeypatch.setenv("DAMIN_GAMBIT_NOW", "2024-01-01 00:00:00")

    r = client.post("/api/query", json={"text": "blabla bla", "explain": True, "db_path": str(seeded_db)})
    assert r.status_code == 422, r.text
    detail = (r.json() or {}).get("detail", "")
    assert "I couldn’t understand your query" in str(detail)

