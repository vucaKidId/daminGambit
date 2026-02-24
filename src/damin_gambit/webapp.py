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
from .db import DbConfig, init_db, query_events
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
    <title>damin-gambit</title>
    <style>
      :root { color-scheme: light dark; }
      body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; margin: 0; padding: 24px; }
      .wrap { max-width: 860px; margin: 0 auto; }
      h1 { margin: 0 0 12px; font-size: 20px; }
      p { margin: 0 0 16px; opacity: 0.85; }
      form { display: flex; gap: 10px; align-items: center; margin-bottom: 14px; }
      input[type="text"] { flex: 1; padding: 12px 12px; font-size: 14px; border-radius: 10px; border: 1px solid rgba(127,127,127,0.35); }
      button { padding: 12px 14px; font-size: 14px; border-radius: 10px; border: 1px solid rgba(127,127,127,0.35); cursor: pointer; }
      .row { display: flex; gap: 14px; align-items: center; margin-bottom: 12px; }
      label { font-size: 13px; opacity: 0.85; }
      table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 13px; }
      th, td { text-align: left; padding: 10px; border-bottom: 1px solid rgba(127,127,127,0.25); }
      th { opacity: 0.9; }
      .muted { opacity: 0.75; font-size: 12px; }
      .error { color: #b91c1c; }
      .totals {
        display: none;
        border: 1px solid rgba(127,127,127,0.25);
        border-radius: 10px;
        padding: 10px 12px;
        margin-top: 10px;
        font-size: 13px;
      }
      .totals strong { font-weight: 600; }
      .errorbox {
        display: none;
        white-space: pre-wrap;
        border: 1px solid rgba(185, 28, 28, 0.35);
        background: rgba(185, 28, 28, 0.08);
        border-radius: 10px;
        padding: 12px;
        margin: 10px 0 0;
        font-size: 13px;
        line-height: 1.35;
      }
      .spec { white-space: pre-wrap; border: 1px solid rgba(127,127,127,0.25); border-radius: 10px; padding: 10px; font-size: 12px; margin-top: 10px; }
    </style>
  </head>
  <body>
    <div class="wrap">
      <h1>damin-gambit</h1>
      <p>Describe what matches you want (country, time, odds, totals). The app will understand your text and pick the matches automatically.</p>

      <form id="qform">
        <input id="q" type="text" placeholder='Try: "I need 2 matches from Bulgaria with total odds less than 2"' autocomplete="off" />
        <button id="go" type="submit">Query</button>
      </form>

      <div class="row">
        <label><input id="explain" type="checkbox" /> explain interpretation</label>
        <span id="status" class="muted"></span>
      </div>

      <div id="err" class="error errorbox"></div>
      <div id="totals" class="totals"></div>
      <div id="spec" class="spec" style="display:none"></div>
      <div id="out"></div>
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
    </script>
  </body>
</html>
"""


@app.post("/api/query", response_model=QueryResponse)
async def api_query(req: QueryRequest, principal: Principal = Depends(require_principal)) -> QueryResponse:
    cfg = _cfg_from_env_or_default(req.db_path)
    init_db(cfg)
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

    msg = "No results for query." if not results else None
    return QueryResponse(
        results=results,
        spec=_spec_to_json(spec) if req.explain else None,
        totals=totals,
        interpreter=interpreter,
        message=msg,
        tenant_id=principal.tenant_id,
    )

