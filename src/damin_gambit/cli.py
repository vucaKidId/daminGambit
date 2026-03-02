from __future__ import annotations

import asyncio
from datetime import datetime
import os
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from . import __version__
from .db import DbConfig, query_events, reset_db, seed_db, seed_sports_db, init_db
from .llm import LlmRequiredError, interpret

app = typer.Typer(add_completion=False, no_args_is_help=True)
console = Console()


def _default_db_path() -> Path:
    return Path.cwd() / "data" / "events.sqlite3"


def _cfg_from_env_or_default(db: Optional[Path]) -> DbConfig:
    if db:
        return DbConfig(path=db)
    return DbConfig.from_env(default_path=_default_db_path())


def _fmt_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    try:
        return dt.isoformat(sep=" ", timespec="seconds")
    except Exception:
        return str(dt)


def _now_from_env() -> Optional[datetime]:
    raw = (os.getenv("DAMIN_GAMBIT_NOW") or "").strip()
    if not raw:
        return None
    try:
        from dateutil import parser as date_parser

        return date_parser.parse(raw)
    except Exception:
        return None


def _effective_now(spec) -> datetime:
    return getattr(spec, "submitted_at", None) or _now_from_env() or datetime.now()


def _print_events(events) -> None:
    table = Table(title=f"Events ({len(events)})")
    table.add_column("id", justify="right")
    table.add_column("event_name")
    table.add_column("type")
    table.add_column("value")
    table.add_column("start_time")
    table.add_column("end_time")

    for e in events:
        table.add_row(
            str(e.id),
            e.event_name,
            e.event_type.name,
            e.value,
            _fmt_dt(e.start_time),
            _fmt_dt(e.end_time),
        )
    console.print(table)


@app.command()
def version() -> None:
    """Show the current version."""
    console.print(__version__)


@app.command("init-db")
def init_db_cmd(
    db: Path = typer.Option(None, "--db", help="Path to SQLite DB file."),
) -> None:
    """Create the local SQLite DB + schema."""
    cfg = _cfg_from_env_or_default(db)
    init_db(cfg)
    console.print(f"Initialized DB at: {cfg.url}")

@app.command("reset-db")
def reset_db_cmd(
    db: Path = typer.Option(None, "--db", help="Path to SQLite DB file."),
) -> None:
    """Drop & recreate the schema (useful after schema changes)."""
    cfg = _cfg_from_env_or_default(db)
    reset_db(cfg)
    console.print(f"Reset DB at: {cfg.url}")


@app.command()
def seed(
    db: Path = typer.Option(None, "--db", help="Path to SQLite DB file."),
    rows: int = typer.Option(200, "--rows", min=1, max=50000, help="How many rows to generate (approx)."),
    replace: bool = typer.Option(
        True, "--replace/--append", help="Replace existing events (recommended for seeding)."
    ),
) -> None:
    """Insert a generated sample dataset."""
    cfg = _cfg_from_env_or_default(db)
    n = seed_db(cfg, rows=rows, replace=replace)
    console.print(f"Inserted {n} example rows into: {cfg.url}")


@app.command(name="seed-sports")
def seed_sports(
    db: Path = typer.Option(None, "--db", help="Path to SQLite DB file."),
    replace: bool = typer.Option(
        False, "--replace/--append", help="Replace all events with only sports data. Default: append."
    ),
) -> None:
    """Insert football (won/draw/lost + goals 0-2 / 3+), basketball and handball (won/draw/lost) sample data."""
    cfg = _cfg_from_env_or_default(db)
    n = seed_sports_db(cfg, replace=replace)
    console.print(f"Inserted {n} sports event rows into: {cfg.url}")


@app.command()
def ask(
    text: str = typer.Argument(..., help="Natural language query text."),
    db: Path = typer.Option(None, "--db", help="Path to SQLite DB file."),
    explain: bool = typer.Option(False, "--explain", help="Print parsed query details."),
) -> None:
    """Translate user text to a DB query and print results."""
    cfg = _cfg_from_env_or_default(db)

    try:
        spec = asyncio.run(interpret(text))
    except LlmRequiredError as e:
        console.print(f"[red]{e}[/red]")
        raise typer.Exit(code=3) from e
    if spec.intent == "empty":
        console.print("Empty input.")
        raise typer.Exit(code=2)

    now = _effective_now(spec)
    events = query_events(
        cfg,
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

    if explain:
        console.print(
            "QuerySpec:",
            {
                "intent": spec.intent,
                "event_name": spec.event_name,
                "group_name": spec.group_name,
                "league": spec.league,
                "type": spec.type_,
                "value_min": spec.value_min,
                "value_max": spec.value_max,
                "start_time_from": _fmt_dt(spec.start_time_from),
                "start_time_to": _fmt_dt(spec.start_time_to),
                "limit": spec.limit,
                "distinct_matches": spec.distinct_matches,
            },
        )

    if not events:
        console.print("No rows matched.")
        return

    if spec.intent == "get_value":
        first = events[0]
        console.print(
            f"value = {first.value} (event={first.event_name}, type={first.event_type.name}, start={_fmt_dt(first.start_time)}, end={_fmt_dt(first.end_time)})"
        )
        if len(events) > 1:
            console.print(f"(Matched {len(events)} rows; showing the most recent.)")
        return

    _print_events(events)


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Host to bind."),
    port: int = typer.Option(8000, "--port", help="Port to listen on."),
) -> None:
    """Run the web UI so you can type queries and see results."""
    import uvicorn
    from .webapp import app as web_app
    console.print(f"Opening at [bold]http://{host}:{port}[/] — type in the box to test queries.")
    uvicorn.run(web_app, host=host, port=port)


@app.command()
def repl(
    db: Path = typer.Option(None, "--db", help="Path to SQLite DB file."),
) -> None:
    """Interactive mode."""
    db = db or _default_db_path()
    console.print("Type a question (or 'quit'). Example: show events for group ops since 2026-01-01")
    while True:
        try:
            text = console.input("[bold]damin-gambit>[/] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print()
            return
        if not text:
            continue
        if text.lower() in {"q", "quit", "exit"}:
            return
        ask(text=text, db=db)

