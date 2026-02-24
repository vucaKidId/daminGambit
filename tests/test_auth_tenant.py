import os
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from jose import jwt

from damin_gambit.db import DbConfig, Event, open_session, reset_db, seed_db
from damin_gambit.webapp import app


@pytest.fixture()
def client():
    return TestClient(app)


@pytest.fixture()
def seeded_db(tmp_path):
    p = tmp_path / "events.sqlite3"
    cfg = DbConfig(path=p)
    reset_db(cfg)
    seed_db(cfg, rows=120, replace=True)
    return cfg


def _token(*, tenant_id: str, sub: str = "user-1") -> str:
    secret = "test-secret"
    now = datetime.utcnow()
    claims = {
        "sub": sub,
        "tenant_id": tenant_id,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(hours=1)).timestamp()),
    }
    return jwt.encode(claims, secret, algorithm="HS256")


def test_require_auth_missing_token_is_401(client: TestClient, seeded_db: DbConfig, monkeypatch):
    monkeypatch.setenv("DAMIN_GAMBIT_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DAMIN_GAMBIT_JWT_ALG", "HS256")
    monkeypatch.setenv("DAMIN_GAMBIT_JWT_SECRET", "test-secret")
    monkeypatch.setenv("DAMIN_GAMBIT_NOW", "2024-01-01 00:00:00")

    r = client.post("/api/query", json={"text": "give me 2 events", "db_path": str(seeded_db.path)})
    assert r.status_code == 401


def test_tenant_scoping_filters_results(client: TestClient, seeded_db: DbConfig, monkeypatch):
    # Mark all seeded events as tenant A, then copy one match to tenant B.
    with open_session(seeded_db) as s:
        for e in s.query(Event).all():
            e.tenant_id = "tenant-a"
        s.commit()
        any_ev = s.query(Event).first()
        assert any_ev is not None
        any_ev.tenant_id = "tenant-b"
        s.commit()

    monkeypatch.setenv("DAMIN_GAMBIT_REQUIRE_AUTH", "1")
    monkeypatch.setenv("DAMIN_GAMBIT_JWT_ALG", "HS256")
    monkeypatch.setenv("DAMIN_GAMBIT_JWT_SECRET", "test-secret")
    monkeypatch.setenv("DAMIN_GAMBIT_NOW", "2024-01-01 00:00:00")

    token_a = _token(tenant_id="tenant-a")
    r = client.post(
        "/api/query",
        headers={"Authorization": f"Bearer {token_a}"},
        json={"text": "give me 5 events", "db_path": str(seeded_db.path)},
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data.get("tenant_id") == "tenant-a"
    assert len(data.get("results") or []) == 5
    assert all((row.get("country") is not None) for row in data["results"])

    # tenant-b should not see 5 results (only 1 row was assigned)
    token_b = _token(tenant_id="tenant-b")
    r2 = client.post(
        "/api/query",
        headers={"Authorization": f"Bearer {token_b}"},
        json={"text": "give me 5 events", "db_path": str(seeded_db.path)},
    )
    assert r2.status_code == 200, r2.text
    data2 = r2.json()
    assert data2.get("tenant_id") == "tenant-b"
    assert len(data2.get("results") or []) <= 1

