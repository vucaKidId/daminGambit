from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
import os
from pathlib import Path
from typing import Any, Optional

from dateutil import parser as date_parser
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .auth import Principal, require_principal
from .db import DbConfig, init_db, query_events, ensure_seeded, ensure_today_fixtures
from .llm import LlmRequiredError, explain_spec, interpret_with_source
from .nl import parse as rule_parse
from .nl import QuerySpec


def _default_db_path() -> Path:
    return Path.cwd() / "data" / "events.sqlite3"


def _cfg_from_env_or_default(db_path: Optional[str] = None) -> DbConfig:
    # Default to local sqlite, but allow overriding via DAMIN_GAMBIT_DATABASE_URL.
    if db_path:
        return DbConfig(path=Path(db_path))
    return DbConfig.from_env(default_path=_default_db_path())


def _json_dt(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    try:
        return dt.isoformat(sep=" ", timespec="seconds")
    except Exception:
        return str(dt)


def _spec_to_json(spec: QuerySpec) -> dict[str, Any]:
    d = explain_spec(spec)
    d["start_time_from"] = _json_dt(spec.start_time_from)
    d["start_time_to"] = _json_dt(spec.start_time_to)
    d["submitted_at"] = _json_dt(spec.submitted_at)
    # When type is not specified, total product can use any outcome (won, draw or lost) per match.
    if spec.type_ is None:
        d["type_explanation"] = "any outcome (won, draw or lost) per match"
    d["min_hours_between"] = getattr(spec, "min_hours_between", None)
    d["round_num"] = getattr(spec, "round_num", None)
    d["sport_limits"] = getattr(spec, "sport_limits", None)
    d["include_teams"] = getattr(spec, "include_teams", None)
    return d


def _spec_has_any_filters(spec: QuerySpec) -> bool:
    return any(
        [
            spec.event_name,
            spec.group_name,
            spec.group_names,
            spec.country_limits,
            spec.league,
            spec.type_,
            spec.value_min is not None,
            spec.value_max is not None,
            spec.total_product_min is not None,
            spec.total_product_max is not None,
            spec.start_time_from is not None,
            spec.start_time_to is not None,
            getattr(spec, "include_event_name", None),
            getattr(spec, "sport_limits", None),
            getattr(spec, "include_teams", None),
        ]
    )


def _spec_is_meaningful(spec: QuerySpec) -> bool:
    # Accept either a real filter OR an explicit small limit request ("give me 5 events").
    if _spec_has_any_filters(spec):
        return True
    try:
        return 1 <= int(spec.limit) <= 500 and int(spec.limit) != 50
    except Exception:
        return False


def _example_prompts() -> list[str]:
    return [
        "give me 3 matches from Bulgaria and 3 from Romania where win is over 1.5",
        "I need 2 matches that start between 12:00 and 17:00 on 25.02.2025 from Bulgaria with total odds less than 2",
        "daj mi dvije utakmice iz Rumunije sa ukupnom vrijednoscu vecom od 3",
        "Sastavi mi tiket od 10 parova sa kvotama od 1.3-1.5",
    ]


def _friendly_unrecognized_query_message() -> str:
    examples = _example_prompts()
    formatted = "\n\n".join([f"- “{x}”" for x in examples])
    return (
        "I couldn’t understand your query.\n\n"
        "If you typed something like:\n\n"
        "- “ovo je bezvezna porkuka koja ne sadrzi nista”\n\n"
        "Please retry using one of these examples (edit as needed):\n\n"
        f"{formatted}"
    )


def _now_from_env() -> Optional[datetime]:
    raw = (os.getenv("DAMIN_GAMBIT_NOW") or "").strip()
    if not raw:
        return None
    try:
        return date_parser.parse(raw)
    except Exception:
        return None


def _effective_now(spec: QuerySpec) -> datetime:
    return spec.submitted_at or _now_from_env() or datetime.now()


class QueryRequest(BaseModel):
    text: str = Field(..., min_length=1)
    explain: bool = False
    db_path: Optional[str] = None


class QueryResponse(BaseModel):
    results: list[dict[str, Any]]
    spec: Optional[dict[str, Any]] = None
    totals: Optional[dict[str, Any]] = None
    interpreter: str
    message: Optional[str] = None
    tenant_id: Optional[str] = None
    fallback_message: Optional[str] = None
    fallback_results: Optional[list[dict[str, Any]]] = None
    fallback_totals_product: Optional[str] = None


app = FastAPI(title="damin-gambit web", version="0.1.0")


@app.on_event("startup")
def _startup() -> None:
    # Ensure schema exists for the default path.
    init_db(_cfg_from_env_or_default())


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>damin-gambit — Sports betting</title>
    <link rel="preconnect" href="https://fonts.googleapis.com" />
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700&display=swap" rel="stylesheet" />
    <style>
      :root {
        --bg-dark: #0c1222;
        --bg-card: #131d33;
        --bg-card-hover: #1a2842;
        --gold: #eab308;
        --gold-dim: #a16207;
        --emerald: #10b981;
        --emerald-dim: #059669;
        --text: #e2e8f0;
        --text-muted: #94a3b8;
        --border: rgba(234, 179, 8, 0.2);
        --danger: #ef4444;
        --danger-bg: rgba(239, 68, 68, 0.12);
      }
      * { box-sizing: border-box; }
      body {
        font-family: 'Outfit', ui-sans-serif, system-ui, sans-serif;
        margin: 0;
        min-height: 100vh;
        background: linear-gradient(145deg, #0c1222 0%, #0f172a 40%, #0a0f1a 100%);
        color: var(--text);
        padding: 24px 16px;
      }
      .wrap {
        max-width: 900px;
        margin: 0 auto;
      }
      .hero {
        text-align: center;
        margin-bottom: 32px;
        padding: 28px 20px;
        background: linear-gradient(135deg, rgba(234, 179, 8, 0.08) 0%, rgba(16, 185, 129, 0.06) 100%);
        border-radius: 16px;
        border: 1px solid var(--border);
      }
      .hero h1 {
        margin: 0 0 8px;
        font-size: 1.85rem;
        font-weight: 700;
        letter-spacing: 0.02em;
        background: linear-gradient(90deg, var(--gold) 0%, #fcd34d 50%, var(--emerald) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
      }
      .hero p {
        margin: 0;
        font-size: 0.95rem;
        color: var(--text-muted);
        max-width: 520px;
        margin-left: auto;
        margin-right: auto;
      }
      .card {
        background: var(--bg-card);
        border-radius: 14px;
        border: 1px solid var(--border);
        padding: 22px;
        margin-bottom: 20px;
      }
      form {
        display: flex;
        gap: 12px;
        align-items: center;
        margin-bottom: 16px;
      }
      input[type="text"] {
        flex: 1;
        padding: 14px 18px;
        font-size: 15px;
        font-family: inherit;
        border-radius: 12px;
        border: 1px solid var(--border);
        background: var(--bg-dark);
        color: var(--text);
        transition: border-color 0.2s, box-shadow 0.2s;
      }
      input[type="text"]::placeholder { color: var(--text-muted); opacity: 0.8; }
      input[type="text"]:focus {
        outline: none;
        border-color: var(--gold);
        box-shadow: 0 0 0 3px rgba(234, 179, 8, 0.15);
      }
      button, #go {
        padding: 14px 24px;
        font-size: 15px;
        font-weight: 600;
        font-family: inherit;
        border-radius: 12px;
        border: none;
        cursor: pointer;
        transition: transform 0.15s, box-shadow 0.2s;
        background: linear-gradient(135deg, var(--emerald) 0%, var(--emerald-dim) 100%);
        color: #fff;
      }
      button:hover, #go:hover {
        transform: translateY(-1px);
        box-shadow: 0 6px 20px rgba(16, 185, 129, 0.35);
      }
      button:active, #go:active { transform: translateY(0); }
      .row {
        display: flex;
        gap: 16px;
        align-items: center;
        margin-bottom: 14px;
      }
      label {
        font-size: 14px;
        color: var(--text-muted);
        display: flex;
        align-items: center;
        gap: 8px;
        cursor: pointer;
      }
      input[type="checkbox"] {
        accent-color: var(--gold);
        width: 18px;
        height: 18px;
      }
      #status.muted { color: var(--text-muted); font-size: 13px; }
      table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 8px;
        font-size: 13px;
        border-radius: 12px;
        overflow: hidden;
      }
      thead {
        background: linear-gradient(90deg, var(--gold-dim) 0%, rgba(234, 179, 8, 0.25) 100%);
        color: #0c1222;
      }
      th {
        text-align: left;
        padding: 14px 16px;
        font-weight: 600;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
      }
      td {
        padding: 12px 16px;
        border-bottom: 1px solid rgba(255,255,255,0.06);
        color: var(--text);
      }
      tbody tr {
        background: var(--bg-card);
        transition: background 0.15s;
      }
      tbody tr:hover { background: var(--bg-card-hover); }
      tbody tr:last-child td { border-bottom: none; }
      .totals {
        display: none;
        border-radius: 12px;
        padding: 16px 20px;
        margin-top: 16px;
        font-size: 15px;
        font-weight: 600;
        background: linear-gradient(135deg, rgba(234, 179, 8, 0.12) 0%, rgba(234, 179, 8, 0.06) 100%);
        border: 1px solid var(--border);
        color: var(--gold);
      }
      .errorbox {
        display: none;
        white-space: pre-wrap;
        border-radius: 12px;
        padding: 14px 18px;
        margin-top: 12px;
        font-size: 14px;
        line-height: 1.4;
        background: var(--danger-bg);
        border: 1px solid rgba(239, 68, 68, 0.35);
        color: var(--danger);
      }
      .spec {
        white-space: pre-wrap;
        border-radius: 12px;
        padding: 14px 18px;
        font-size: 12px;
        margin-top: 14px;
        background: var(--bg-dark);
        border: 1px solid var(--border);
        color: var(--text-muted);
      }
      .fallback-box {
        margin-top: 16px;
        padding: 18px 20px;
        border-radius: 12px;
        border: 1px solid rgba(16, 185, 129, 0.35);
        background: linear-gradient(135deg, rgba(16, 185, 129, 0.1) 0%, rgba(16, 185, 129, 0.05) 100%);
      }
      .fallback-msg {
        margin: 0 0 12px;
        font-size: 14px;
        color: var(--text);
      }
      .fallback-box button {
        padding: 10px 20px;
        border-radius: 10px;
        font-weight: 600;
        background: linear-gradient(135deg, var(--gold) 0%, var(--gold-dim) 100%);
        color: #0c1222;
        border: none;
        cursor: pointer;
        font-family: inherit;
        transition: transform 0.15s, box-shadow 0.2s;
      }
      .fallback-box button:hover {
        transform: translateY(-1px);
        box-shadow: 0 6px 20px rgba(234, 179, 8, 0.3);
      }
      #out .muted {
        color: var(--text-muted);
        font-size: 14px;
        padding: 20px;
      }
      #out-wrap {
        max-height: 70vh;
        overflow-y: auto;
      }
      #out-wrap table { margin-bottom: 0; }
    </style>
  </head>
  <body>
    <div class="wrap">
      <header class="hero">
        <h1>damin-gambit</h1>
        <p>Describe what matches you want — country, time, odds, totals. We’ll pick the best bets for you.</p>
      </header>

      <div class="card">
        <form id="qform">
          <input id="q" type="text" placeholder='e.g. "5 matches from England today, total odds around 8"' autocomplete="off" />
          <button id="go" type="submit">Find bets</button>
        </form>

        <div class="row">
          <label><input id="explain" type="checkbox" /> Explain interpretation</label>
          <span id="status" class="muted"></span>
        </div>

        <div id="err" class="error errorbox"></div>
        <div id="totals" class="totals"></div>
        <div id="fallback" class="fallback-box" style="display:none">
          <p id="fallback-msg" class="fallback-msg"></p>
          <button id="accept-fallback" type="button">Accept</button>
        </div>
        <div id="spec" class="spec" style="display:none"></div>
      </div>

      <div class="card" id="out-wrap">
        <div id="out"></div>
      </div>
    </div>

    <script>
      const $ = (id) => document.getElementById(id);
      const qform = $("qform");
      const q = $("q");
      const status = $("status");
      const out = $("out");
      const err = $("err");
      const totalsBox = $("totals");
      const specBox = $("spec");
      const explain = $("explain");
      const fallbackBox = $("fallback");
      const fallbackMsg = $("fallback-msg");
      const acceptFallbackBtn = $("accept-fallback");
      let lastFallbackResults = null;
      let lastFallbackProduct = null;

      function escapeHtml(s) {
        return (s ?? "").toString()
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#039;");
      }

      function renderTable(rows) {
        if (!rows || rows.length === 0) {
          out.innerHTML = "<div class='muted'>No results for query.</div>";
          return;
        }
        const cols = ["id", "event_name", "country", "league", "type", "value", "start_time", "end_time"];
        const head = cols.map(c => `<th>${escapeHtml(c)}</th>`).join("");
        const body = rows.map(r => {
          return "<tr>" + cols.map(c => `<td>${escapeHtml(r[c])}</td>`).join("") + "</tr>";
        }).join("");
        out.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
      }

      qform.addEventListener("submit", async (e) => {
        e.preventDefault();
        const text = q.value.trim();
        err.textContent = "";
        err.style.display = "none";
        totalsBox.textContent = "";
        totalsBox.style.display = "none";
        status.textContent = "";
        out.innerHTML = "";
        specBox.style.display = "none";
        specBox.textContent = "";
        fallbackBox.style.display = "none";
        lastFallbackResults = null;
        lastFallbackProduct = null;

        if (!text) {
          err.textContent = "Please enter some text.";
          err.style.display = "block";
          return;
        }

        status.textContent = "Querying…";
        try {
          const res = await fetch("/api/query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ text, explain: explain.checked }),
          });
          const data = await res.json();
          if (!res.ok) {
            throw new Error(data?.detail || "Request failed");
          }
          if (data.spec) {
            specBox.style.display = "block";
            specBox.textContent = JSON.stringify(data.spec, null, 2);
          }
          renderTable(data.results);
          const interp = data.interpreter ? ` (interpreter: ${data.interpreter})` : "";
          if (data.fallback_message && data.fallback_results && data.fallback_results.length > 0) {
            lastFallbackResults = data.fallback_results;
            lastFallbackProduct = data.fallback_totals_product || null;
            fallbackMsg.textContent = data.fallback_message;
            acceptFallbackBtn.textContent = "Accept " + data.fallback_results.length + " matches";
            fallbackBox.style.display = "block";
          } else {
            fallbackBox.style.display = "none";
            lastFallbackResults = null;
            lastFallbackProduct = null;
          }
          if (!data.results || data.results.length === 0) {
            status.textContent = (data.message || "No results for query.") + interp;
            return;
          }
          if (data.totals && data.totals.product !== null && data.totals.product !== undefined) {
            totalsBox.style.display = "block";
            totalsBox.textContent = `Total odds (product): ${data.totals.product}`;
            status.textContent = `Matched ${data.results.length} row(s). Total odds = ${data.totals.product}` +
              interp +
              (data.totals.skipped_non_numeric ? ` (skipped ${data.totals.skipped_non_numeric} non-numeric value(s))` : "");
          } else {
            status.textContent = `Matched ${data.results.length} row(s).` + interp;
          }
        } catch (e2) {
          err.textContent = e2?.message || String(e2);
          err.style.display = "block";
          status.textContent = "";
        }
      });
      acceptFallbackBtn.addEventListener("click", () => {
        if (lastFallbackResults && lastFallbackResults.length > 0) {
          renderTable(lastFallbackResults);
          if (lastFallbackProduct != null) {
            totalsBox.style.display = "block";
            totalsBox.textContent = "Total odds (product): " + lastFallbackProduct;
          }
          status.textContent = "Showing " + lastFallbackResults.length + " match(es). Total odds = " + (lastFallbackProduct || "—");
          fallbackBox.style.display = "none";
        }
      });
    </script>
  </body>
</html>
"""


@app.post("/api/query", response_model=QueryResponse)
async def api_query(req: QueryRequest, principal: Principal = Depends(require_principal)) -> QueryResponse:
    cfg = _cfg_from_env_or_default(req.db_path)
    init_db(cfg)
    ensure_seeded(cfg)  # seed if DB is empty so e.g. "3 matches from England, total odd 3-6" returns results
    if (os.getenv("DAMIN_GAMBIT_REQUIRE_AUTH") or "").strip() in {"1", "true", "TRUE"} and not principal.tenant_id:
        raise HTTPException(status_code=403, detail="Token is missing tenant_id claim")

    try:
        spec, interpreter = await interpret_with_source(req.text)
    except LlmRequiredError as e:
        # In "LLM required" mode, nonsense text should still get a friendly "didn't understand" message
        # rather than an infrastructure-flavored error.
        try:
            rule_spec = rule_parse(req.text)
        except Exception:
            rule_spec = QuerySpec(intent="list")
        if not _spec_is_meaningful(rule_spec):
            raise HTTPException(status_code=422, detail=_friendly_unrecognized_query_message()) from e

        # Otherwise, keep a clear service error for real queries.
        friendly = (
            "I couldn’t process your query because the local LLM (Ollama) is required but not available.\n"
            "Please make sure Ollama is running and the configured model is installed, then retry."
        )
        raise HTTPException(status_code=503, detail=friendly) from e
    if spec.intent == "empty":
        return QueryResponse(
            results=[],
            spec=_spec_to_json(spec) if req.explain else None,
            totals=None,
            interpreter=interpreter,
            message="Please enter a query.",
        )

    # If LLM returned a non-meaningful spec (e.g. didn't understand Serbian), try rules parser (ticket/parova/kvotama etc.)
    if not _spec_is_meaningful(spec):
        try:
            rule_spec = rule_parse(req.text)
            if _spec_is_meaningful(rule_spec) and rule_spec.intent != "empty":
                spec = rule_spec
                interpreter = "rules"
        except Exception:
            pass
    if not _spec_is_meaningful(spec):
        raise HTTPException(status_code=422, detail=_friendly_unrecognized_query_message())

    now = _effective_now(spec)

    events = query_events(
        cfg,
        tenant_id=principal.tenant_id,
        event_name=spec.event_name,
        group_name=spec.group_name,
        group_names=spec.group_names,
        country_limits=spec.country_limits,
        league=spec.league,
        type_=spec.type_,
        value_min=spec.value_min,
        value_min_inclusive=spec.value_min_inclusive,
        value_max=spec.value_max,
        value_max_inclusive=spec.value_max_inclusive,
        total_product_min=spec.total_product_min,
        total_product_min_inclusive=spec.total_product_min_inclusive,
        total_product_max=spec.total_product_max,
        total_product_max_inclusive=spec.total_product_max_inclusive,
        start_time_from=spec.start_time_from,
        start_time_to=spec.start_time_to,
        end_time_from=now,
        limit=spec.limit,
        distinct_matches=spec.distinct_matches,
        min_hours_between=spec.min_hours_between,
        round_num=getattr(spec, "round_num", None),
        sport_limits=getattr(spec, "sport_limits", None),
        include_teams=getattr(spec, "include_teams", None),
    )
    # If sport_limits + total_product returned nothing (DFS may not find a combo), retry without total product to still return quota matches
    if not events and getattr(spec, "sport_limits", None) and (spec.total_product_min is not None or spec.total_product_max is not None):
        events = query_events(
            cfg,
            tenant_id=principal.tenant_id,
            event_name=spec.event_name,
            group_name=spec.group_name,
            group_names=spec.group_names,
            country_limits=spec.country_limits,
            league=spec.league,
            type_=spec.type_,
            value_min=spec.value_min,
            value_min_inclusive=spec.value_min_inclusive,
            value_max=spec.value_max,
            value_max_inclusive=spec.value_max_inclusive,
            total_product_min=None,
            total_product_min_inclusive=True,
            total_product_max=None,
            total_product_max_inclusive=True,
            start_time_from=spec.start_time_from,
            start_time_to=spec.start_time_to,
            end_time_from=now,
            limit=spec.limit,
            distinct_matches=spec.distinct_matches,
            min_hours_between=spec.min_hours_between,
            round_num=getattr(spec, "round_num", None),
            sport_limits=getattr(spec, "sport_limits", None),
            include_teams=getattr(spec, "include_teams", None),
        )
    # If "za danas" (today) query returned nothing, ensure we have upcoming today fixtures and retry once
    if not events and spec.start_time_from is not None and spec.start_time_to is not None:
        ensure_today_fixtures(cfg)
        events = query_events(
            cfg,
            tenant_id=principal.tenant_id,
            event_name=spec.event_name,
            group_name=spec.group_name,
            group_names=spec.group_names,
            country_limits=spec.country_limits,
            league=spec.league,
            type_=spec.type_,
            value_min=spec.value_min,
            value_min_inclusive=spec.value_min_inclusive,
            value_max=spec.value_max,
            value_max_inclusive=spec.value_max_inclusive,
            total_product_min=spec.total_product_min,
            total_product_min_inclusive=spec.total_product_min_inclusive,
            total_product_max=spec.total_product_max,
            total_product_max_inclusive=spec.total_product_max_inclusive,
            start_time_from=spec.start_time_from,
            start_time_to=spec.start_time_to,
            end_time_from=now,
            limit=spec.limit,
            distinct_matches=spec.distinct_matches,
            min_hours_between=spec.min_hours_between,
            round_num=getattr(spec, "round_num", None),
            sport_limits=getattr(spec, "sport_limits", None),
            include_teams=getattr(spec, "include_teams", None),
        )

    # Optional: append a specifically requested extra match row (e.g. "... and one more match Anand vs Radjabov won").
    if spec.include_event_name:
        extra = query_events(
            cfg,
            tenant_id=principal.tenant_id,
            event_name=spec.include_event_name,
            type_=spec.include_type_,
            end_time_from=now,
            limit=1,
            distinct_matches=False,
        )
        if extra:
            extra_ev = extra[0]
            if all(e.id != extra_ev.id for e in events):
                events = list(events) + [extra_ev]

    results: list[dict[str, Any]] = []
    product = Decimal("1")
    multiplied_count = 0
    skipped_non_numeric = 0
    for e in events:
        try:
            v = Decimal(str(e.value).strip())
            product *= v
            multiplied_count += 1
        except (InvalidOperation, ValueError):
            skipped_non_numeric += 1

        results.append(
            {
                "id": e.id,
                "event_name": e.event_name,
                "country": e.group_name,
                "league": e.league,
                "sport": getattr(e, "sport", None),
                "type": e.event_type.name,
                "value": e.value,
                "start_time": _json_dt(e.start_time),
                "end_time": _json_dt(e.end_time),
            }
        )

    totals = {
        "product": str(product) if multiplied_count > 0 else None,
        "multiplied_count": multiplied_count,
        "skipped_non_numeric": skipped_non_numeric,
    }

    fallback_message: Optional[str] = None
    fallback_results: Optional[list[dict[str, Any]]] = None
    fallback_totals_product: Optional[str] = None
    if not results and (spec.total_product_min is not None or spec.total_product_max is not None) and spec.limit:
        fallback_events = query_events(
            cfg,
            tenant_id=principal.tenant_id,
            event_name=spec.event_name,
            group_name=spec.group_name,
            group_names=spec.group_names,
            country_limits=spec.country_limits,
            league=spec.league,
            type_=spec.type_,
            value_min=spec.value_min,
            value_min_inclusive=spec.value_min_inclusive,
            value_max=spec.value_max,
            value_max_inclusive=spec.value_max_inclusive,
            total_product_min=None,
            total_product_min_inclusive=True,
            total_product_max=None,
            total_product_max_inclusive=True,
            start_time_from=spec.start_time_from,
            start_time_to=spec.start_time_to,
            end_time_from=now,
            limit=spec.limit,
            distinct_matches=spec.distinct_matches,
            min_hours_between=spec.min_hours_between,
            round_num=getattr(spec, "round_num", None),
            sport_limits=getattr(spec, "sport_limits", None),
            include_teams=getattr(spec, "include_teams", None),
        )
        if fallback_events:
            fb_product = Decimal("1")
            fallback_results = []
            for e in fallback_events:
                try:
                    v = Decimal(str(e.value).strip())
                    fb_product *= v
                except (InvalidOperation, ValueError):
                    pass
                fallback_results.append(
                    {
                        "id": e.id,
                        "event_name": e.event_name,
                        "country": e.group_name,
                        "league": e.league,
                        "sport": getattr(e, "sport", None),
                        "type": e.event_type.name,
                        "value": e.value,
                        "start_time": _json_dt(e.start_time),
                        "end_time": _json_dt(e.end_time),
                    }
                )
            n = len(fallback_results)
            total_str = f"{fb_product:.2f}".rstrip("0").rstrip(".")
            fallback_message = f"Only {n} match{'es' if n != 1 else ''} available with total odds {total_str}."
            fallback_totals_product = str(fb_product)

    msg = "No results for query." if not results else None
    return QueryResponse(
        results=results,
        spec=_spec_to_json(spec) if req.explain else None,
        totals=totals,
        interpreter=interpreter,
        message=msg,
        tenant_id=principal.tenant_id,
        fallback_message=fallback_message,
        fallback_results=fallback_results,
        fallback_totals_product=fallback_totals_product,
    )

