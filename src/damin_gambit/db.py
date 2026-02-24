from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import random
import os
from typing import Iterable, Optional

from decimal import Decimal, InvalidOperation
from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, cast, create_engine, delete, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, selectinload


class Base(DeclarativeBase):
    pass


STATIC_EVENT_TYPES: tuple[str, ...] = ("won", "draw", "lost")

PLAYER_POOL: tuple[str, ...] = (
    "Kasparov",
    "Carlsen",
    "Fischer",
    "Spassky",
    "Karpov",
    "Anand",
    "Topalov",
    "Kramnik",
    "Ivanchuk",
    "Aronian",
    "Nakamura",
    "Caruana",
    "Nepomniachtchi",
    "Ding",
    "Giri",
    "So",
    "Polgar",
    "Tal",
    "Botvinnik",
    "Smyslov",
    "Petrosian",
    "Capablanca",
    "Alekhine",
    "Lasker",
    "Steinitz",
    "Morphy",
    "Bronstein",
    "Short",
    "Svidler",
    "Grischuk",
    "Radjabov",
    "Shirov",
    "Karjakin",
    "Mamedyarov",
    "Rapport",
    "Firouzja",
    "Praggnanandhaa",
    "Gukesh",
)

COUNTRY_POOL: tuple[str, ...] = (
    "Norway",
    "Iceland",
    "Bulgaria",
    "Hungary",
    "India",
    "USA",
    "Russia",
    "France",
    "Germany",
    "Spain",
    "Italy",
    "China",
    "Japan",
    "Brazil",
    "Argentina",
    "Serbia",
    "Croatia",
    "Poland",
    "Ukraine",
    "Netherlands",
    "Sweden",
    "Finland",
    "Denmark",
    "UK",
    "Ireland",
    "Portugal",
    "Romania",
    "Greece",
    "Turkey",
    "Israel",
    "Iran",
    "Egypt",
    "Morocco",
    "South Africa",
    "Nigeria",
    "Kenya",
    "Mexico",
    "Canada",
    "Australia",
    "New Zealand",
)


class EventType(Base):
    __tablename__ = "event_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, unique=True, index=True)

    events: Mapped[list["Event"]] = relationship(back_populates="event_type")


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String, index=True, default="default")
    event_name: Mapped[str] = mapped_column(String, index=True)
    group_name: Mapped[str] = mapped_column(String, index=True)
    league: Mapped[Optional[str]] = mapped_column(String, index=True, nullable=True)
    value: Mapped[str] = mapped_column(String)
    event_type_id: Mapped[int] = mapped_column(ForeignKey("event_types.id"), index=True)
    start_time: Mapped[datetime] = mapped_column(DateTime, index=True)
    end_time: Mapped[Optional[datetime]] = mapped_column(DateTime, index=True, nullable=True)

    event_type: Mapped[EventType] = relationship(back_populates="events")


@dataclass(frozen=True)
class DbConfig:
    path: Optional[Path] = None
    database_url: Optional[str] = None

    @classmethod
    def from_env(cls, *, default_path: Path) -> "DbConfig":
        url = (os.getenv("DAMIN_GAMBIT_DATABASE_URL") or "").strip()
        if url:
            return cls(database_url=url)
        return cls(path=default_path)

    @property
    def url(self) -> str:
        if self.database_url:
            return self.database_url
        if not self.path:
            raise ValueError("DbConfig requires either database_url or path")
        return f"sqlite:///{self.path}"


def ensure_parent_dir(path: Optional[Path]) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)


def get_engine(cfg: DbConfig):
    # Only SQLite file paths need a local parent directory.
    if cfg.path and cfg.url.startswith("sqlite:///"):
        ensure_parent_dir(cfg.path)
    return create_engine(cfg.url, future=True)


def init_db(cfg: DbConfig) -> None:
    engine = get_engine(cfg)
    Base.metadata.create_all(engine)
    ensure_event_types(cfg)


def open_session(cfg: DbConfig) -> Session:
    engine = get_engine(cfg)
    return Session(engine)

def ensure_event_types(cfg: DbConfig) -> int:
    with open_session(cfg) as s:
        existing = set(s.scalars(select(EventType.name)).all())
        missing = [t for t in STATIC_EVENT_TYPES if t not in existing]
        if missing:
            s.add_all([EventType(name=t) for t in missing])
            s.commit()
        return len(missing)


def reset_db(cfg: DbConfig) -> None:
    engine = get_engine(cfg)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    ensure_event_types(cfg)


def seed_db(cfg: DbConfig, *, rows: int = 200, replace: bool = False) -> int:
    init_db(cfg)
    ensure_event_types(cfg)

    with open_session(cfg) as s:
        types = {t.name: t for t in s.scalars(select(EventType)).all()}

        if replace:
            s.execute(delete(Event))
            s.commit()

        rng = random.Random(42)
        leagues = [
            "World Championship",
            "Candidates",
            "Grand Prix",
            "Rapid",
            "Blitz",
            "Online",
            "Classical",
        ]

        def odds_for(t: str, *, fixed: Optional[dict[str, str]] = None) -> str:
            # Keep the same “shape” as your example but allow variety per match.
            if fixed and t in fixed:
                return fixed[t]
            if t == "won":
                return f"{rng.uniform(1.4, 2.6):.2f}"
            if t == "lost":
                return f"{rng.uniform(1.4, 2.6):.2f}"
            # draw
            return f"{rng.uniform(6.0, 15.0):.2f}"

        # We generate rows in triplets (won/draw/lost) for each match.
        matches = max(1, (rows + 2) // 3)

        base = datetime(2026, 2, 24, 12, 0, 0)
        created: list[Event] = []

        used: set[tuple[str, str]] = set()

        featured: list[tuple[str, str]] = [
            ("Kasparov vs Carlsen", "Norway"),
            ("Fischer vs Spassky", "Iceland"),
            ("Anand vs Topalov", "Bulgaria"),
            ("Polgar vs Kasparov", "Hungary"),
            ("Caruana vs Nakamura", "USA"),
            ("Ding vs Nepomniachtchi", "China"),
        ]
        featured_odds = {"won": "1.85", "lost": "1.95", "draw": "10"}
        featured_low_norway: list[tuple[str, str, dict[str, str]]] = [
            ("Karpov vs Tal", "Norway", {"won": "1.55", "lost": "1.59", "draw": "12"}),
            ("Anand vs Carlsen", "Norway", {"won": "1.48", "lost": "1.52", "draw": "11"}),
            ("Fischer vs Kasparov", "Norway", {"won": "1.58", "lost": "1.57", "draw": "13"}),
        ]
        featured_low_bulgaria: list[tuple[str, str, dict[str, str]]] = [
            ("Topalov vs Anand", "Bulgaria", {"won": "1.45", "lost": "1.72", "draw": "10"}),
            ("Kramnik vs Topalov", "Bulgaria", {"won": "1.33", "lost": "1.66", "draw": "10"}),
            ("Ivanchuk vs Topalov", "Bulgaria", {"won": "1.61", "lost": "1.79", "draw": "10"}),
        ]
        featured_low_romania: list[tuple[str, str, dict[str, str]]] = [
            ("Anand vs Radjabov", "Romania", {"won": "1.44", "lost": "1.73", "draw": "10"}),
            ("Carlsen vs Giri", "Romania", {"won": "1.35", "lost": "1.69", "draw": "10"}),
            ("Nakamura vs So", "Romania", {"won": "1.62", "lost": "1.78", "draw": "10"}),
        ]
        featured_won_over_bg: list[tuple[str, str, dict[str, str]]] = [
            ("Botvinnik vs Kramnik", "Bulgaria", {"won": "1.70", "lost": "2.10", "draw": "10"}),
            ("Tal vs Petrosian", "Bulgaria", {"won": "1.80", "lost": "2.20", "draw": "10"}),
            ("Smyslov vs Topalov", "Bulgaria", {"won": "1.65", "lost": "2.05", "draw": "10"}),
        ]
        featured_won_over_ro: list[tuple[str, str, dict[str, str]]] = [
            ("Capablanca vs Alekhine", "Romania", {"won": "1.70", "lost": "2.10", "draw": "10"}),
            ("Lasker vs Steinitz", "Romania", {"won": "1.80", "lost": "2.20", "draw": "10"}),
            ("Morphy vs Bronstein", "Romania", {"won": "1.65", "lost": "2.05", "draw": "10"}),
        ]

        def add_match(event_name: str, country: str, *, fixed: Optional[dict[str, str]] = None) -> None:
            key = (event_name.lower(), country.lower())
            if key in used:
                return
            used.add(key)

            start = base - timedelta(days=rng.randint(0, 120), hours=rng.randint(0, 23), minutes=rng.randint(0, 59))
            end = start + timedelta(minutes=rng.randint(25, 110))
            league = rng.choice(leagues)

            for t in STATIC_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=event_name,
                        group_name=country.lower(),
                        league=league,
                        value=odds_for(t, fixed=fixed),
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                    )
                )

        def add_match_at(
            event_name: str,
            country: str,
            *,
            start: datetime,
            end: datetime,
            league: str,
            fixed: dict[str, str],
        ) -> None:
            key = (event_name.lower(), country.lower())
            if key in used:
                return
            used.add(key)
            for t in STATIC_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=event_name,
                        group_name=country.lower(),
                        league=league,
                        value=fixed[t],
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                    )
                )

        for ev, c in featured:
            add_match(ev, c, fixed=featured_odds)
        for ev, c, odds in featured_low_norway:
            add_match(ev, c, fixed=odds)
        for ev, c, odds in featured_low_bulgaria:
            add_match(ev, c, fixed=odds)
        for ev, c, odds in featured_low_romania:
            add_match(ev, c, fixed=odds)
        for ev, c, odds in featured_won_over_bg:
            add_match(ev, c, fixed=odds)
        for ev, c, odds in featured_won_over_ro:
            add_match(ev, c, fixed=odds)

        # Deterministic fixtures for date/time window prompts (25.02.2025, Bulgaria 12:00–17:00)
        add_match_at(
            "Kasparov vs Karpov",
            "Bulgaria",
            start=datetime(2025, 2, 25, 12, 30, 0),
            end=datetime(2025, 2, 25, 13, 20, 0),
            league="Classical",
            fixed={"won": "1.33", "lost": "2.10", "draw": "10"},
        )
        add_match_at(
            "Carlsen vs Anand",
            "Bulgaria",
            start=datetime(2025, 2, 25, 16, 0, 0),
            end=datetime(2025, 2, 25, 16, 55, 0),
            league="Classical",
            fixed={"won": "1.35", "lost": "2.20", "draw": "10"},
        )

        # Deterministic fixtures for: "after 12:00 28.02.2025 from romania with total odd less then 2"
        # We seed 6 Romania matches with very low "won" odds so a 6-pick product can still be < 2.
        ro_fixture_odds = {"won": "1.12", "lost": "2.40", "draw": "10"}
        ro_day = datetime(2025, 2, 28)
        add_match_at(
            "Tal vs Korchnoi",
            "Romania",
            start=ro_day.replace(hour=12, minute=10, second=0),
            end=ro_day.replace(hour=12, minute=55, second=0),
            league="Classical",
            fixed=ro_fixture_odds,
        )
        add_match_at(
            "Caruana vs Giri",
            "Romania",
            start=ro_day.replace(hour=13, minute=5, second=0),
            end=ro_day.replace(hour=13, minute=50, second=0),
            league="Classical",
            fixed=ro_fixture_odds,
        )
        add_match_at(
            "Fischer vs Tal",
            "Romania",
            start=ro_day.replace(hour=14, minute=20, second=0),
            end=ro_day.replace(hour=15, minute=5, second=0),
            league="Classical",
            fixed=ro_fixture_odds,
        )
        add_match_at(
            "Karpov vs Petrosian",
            "Romania",
            start=ro_day.replace(hour=15, minute=30, second=0),
            end=ro_day.replace(hour=16, minute=10, second=0),
            league="Classical",
            fixed=ro_fixture_odds,
        )
        add_match_at(
            "Kasparov vs Ivanchuk",
            "Romania",
            start=ro_day.replace(hour=16, minute=40, second=0),
            end=ro_day.replace(hour=17, minute=25, second=0),
            league="Classical",
            fixed=ro_fixture_odds,
        )
        add_match_at(
            "Anand vs Ding",
            "Romania",
            start=ro_day.replace(hour=17, minute=10, second=0),
            end=ro_day.replace(hour=17, minute=55, second=0),
            league="Classical",
            fixed=ro_fixture_odds,
        )

        # Deterministic fixtures for "value between 3 and 5" prompts (need at least 4 distinct matches).
        vb_day = datetime(2025, 3, 2)
        add_match_at(
            "Spassky vs Keres",
            "USA",
            start=vb_day.replace(hour=12, minute=0, second=0),
            end=vb_day.replace(hour=12, minute=45, second=0),
            league="Grand Prix",
            fixed={"won": "3.20", "lost": "1.90", "draw": "10"},
        )
        add_match_at(
            "Polgar vs Gelfand",
            "France",
            start=vb_day.replace(hour=13, minute=10, second=0),
            end=vb_day.replace(hour=13, minute=55, second=0),
            league="Grand Prix",
            fixed={"won": "3.40", "lost": "1.95", "draw": "10"},
        )
        add_match_at(
            "Kramnik vs Korchnoi",
            "Germany",
            start=vb_day.replace(hour=14, minute=20, second=0),
            end=vb_day.replace(hour=15, minute=5, second=0),
            league="Grand Prix",
            fixed={"won": "4.10", "lost": "1.85", "draw": "10"},
        )
        add_match_at(
            "Caruana vs Grischuk",
            "Spain",
            start=vb_day.replace(hour=15, minute=30, second=0),
            end=vb_day.replace(hour=16, minute=15, second=0),
            league="Grand Prix",
            fixed={"won": "4.80", "lost": "1.80", "draw": "10"},
        )

        # Deterministic fixtures for "submitted at 2026-02-20 11:55:00" constraints:
        # we create many matches that are ONGOING at that moment (start <= 11:55 <= end),
        # with low odds so that a 19-pick total product can be <= 1200.
        submit_dt = datetime(2026, 2, 20, 11, 55, 0)
        ongoing_odds = {"won": "1.25", "lost": "1.28", "draw": "10"}
        ongoing_matches: list[tuple[str, str, int, int]] = [
            ("Live01 vs Live02", "Romania", 10, 70),
            ("Live03 vs Live04", "Bulgaria", 12, 72),
            ("Live05 vs Live06", "Norway", 14, 74),
            ("Live07 vs Live08", "France", 16, 76),
            ("Live09 vs Live10", "Germany", 18, 78),
            ("Live11 vs Live12", "Spain", 20, 80),
            ("Live13 vs Live14", "USA", 22, 82),
            ("Live15 vs Live16", "China", 24, 84),
            ("Live17 vs Live18", "Hungary", 26, 86),
            ("Live19 vs Live20", "Ukraine", 28, 88),
            ("Live21 vs Live22", "Russia", 30, 90),
            ("Live23 vs Live24", "Switzerland", 32, 92),
            ("Live25 vs Live26", "Netherlands", 34, 94),
            ("Live27 vs Live28", "Iceland", 36, 96),
            ("Live29 vs Live30", "Austria", 38, 98),
            ("Live31 vs Live32", "Israel", 40, 100),
            ("Live33 vs Live34", "Estonia", 42, 102),
            ("Live35 vs Live36", "Serbia", 44, 104),
            ("Live37 vs Live38", "Croatia", 46, 106),
            ("Live39 vs Live40", "Italy", 48, 108),
            ("Live41 vs Live42", "Morocco", 50, 110),
            ("Live43 vs Live44", "Argentina", 52, 112),
            ("Live45 vs Live46", "Japan", 54, 114),
            ("Live47 vs Live48", "Brazil", 56, 116),
            ("Live49 vs Live50", "Canada", 58, 118),
        ]
        for ev, c, start_offset_min, duration_min in ongoing_matches:
            start = submit_dt.replace(hour=10, minute=0, second=0) + timedelta(minutes=int(start_offset_min))
            end = start + timedelta(minutes=int(duration_min))
            # Ensure end is after submit_dt
            if end <= submit_dt:
                end = submit_dt + timedelta(minutes=30)
            add_match_at(
                ev,
                c,
                start=start,
                end=end,
                league="Ongoing",
                fixed=ongoing_odds,
            )

        # Always include a pool of upcoming low-odds matches so "give me N events ..."
        # can return results even when run much later than our fixed base dates.
        future_base = datetime(2026, 12, 1, 12, 0, 0)
        future_odds = {"won": "1.20", "lost": "1.22", "draw": "10"}
        for i in range(1, 31):
            ev = f"Future{i:02d} vs Future{i+30:02d}"
            start = future_base + timedelta(minutes=i * 7)
            end = start + timedelta(days=7)
            add_match_at(
                ev,
                "Bulgaria" if i % 2 == 0 else "Romania",
                start=start,
                end=end,
                league="Upcoming",
                fixed=future_odds,
            )

        remaining_matches = max(0, matches - len(used))
        for i in range(remaining_matches):
            # Ensure unique (event_name, country) pairs.
            for _ in range(50):
                a, b = rng.sample(list(PLAYER_POOL), 2)
                event_name = f"{a} vs {b}"
                country = rng.choice(COUNTRY_POOL)
                key = (event_name.lower(), country.lower())
                if key not in used:
                    break
            else:
                event_name = f"Match {i+1}"
                country = rng.choice(COUNTRY_POOL)

            add_match(event_name, country, fixed=None)

        s.add_all(created)
        s.commit()
        return len(created)


def query_events(
    cfg: DbConfig,
    *,
    tenant_id: Optional[str] = None,
    event_name: Optional[str] = None,
    group_name: Optional[str] = None,
    group_names: Optional[list[str]] = None,
    country_limits: Optional[dict[str, int]] = None,
    league: Optional[str] = None,
    type_: Optional[str] = None,
    value_min: Optional[float] = None,
    value_min_inclusive: bool = True,
    value_max: Optional[float] = None,
    value_max_inclusive: bool = True,
    total_product_min: Optional[float] = None,
    total_product_min_inclusive: bool = True,
    total_product_max: Optional[float] = None,
    total_product_max_inclusive: bool = True,
    start_time_from: Optional[datetime] = None,
    start_time_to: Optional[datetime] = None,
    end_time_from: Optional[datetime] = None,
    end_time_to: Optional[datetime] = None,
    limit: int = 50,
    distinct_matches: bool = False,
) -> list[Event]:
    stmt = select(Event).options(selectinload(Event.event_type)).order_by(Event.start_time.desc(), Event.id.asc())
    if tenant_id:
        stmt = stmt.where(func.lower(Event.tenant_id) == tenant_id.lower())
    if event_name:
        stmt = stmt.where(func.lower(Event.event_name) == event_name.lower())
    merged_group_names: list[str] = []
    if group_name:
        merged_group_names.append(group_name.lower())
    if group_names:
        merged_group_names.extend([g.lower() for g in group_names if g])
    if country_limits:
        merged_group_names.extend([k.lower() for k in country_limits.keys()])
    merged_group_names = list(dict.fromkeys([g.strip() for g in merged_group_names if g.strip()]))
    if merged_group_names:
        if len(merged_group_names) == 1:
            stmt = stmt.where(func.lower(Event.group_name) == merged_group_names[0])
        else:
            stmt = stmt.where(func.lower(Event.group_name).in_(merged_group_names))
    if league:
        stmt = stmt.where(func.lower(Event.league) == league.lower())
    if type_:
        stmt = stmt.join(EventType).where(func.lower(EventType.name) == type_.lower())
    if value_min is not None:
        if value_min_inclusive:
            stmt = stmt.where(cast(Event.value, Float) >= value_min)
        else:
            stmt = stmt.where(cast(Event.value, Float) > value_min)
    if value_max is not None:
        if value_max_inclusive:
            stmt = stmt.where(cast(Event.value, Float) <= value_max)
        else:
            stmt = stmt.where(cast(Event.value, Float) < value_max)
    if start_time_from:
        stmt = stmt.where(Event.start_time >= start_time_from)
    if start_time_to:
        stmt = stmt.where(Event.start_time <= start_time_to)
    if end_time_from:
        stmt = stmt.where(Event.end_time >= end_time_from)
    if end_time_to:
        stmt = stmt.where(Event.end_time <= end_time_to)
    if country_limits or total_product_min is not None or total_product_max is not None:
        # Quotas / total-product constraints imply one row per match.
        distinct_matches = True

    if distinct_matches:
        # Over-fetch and dedupe in Python by match name (event_name).
        # The requested `limit` is interpreted as “matches”, not “rows”.
        overfetch = max(200, limit * 50)
        if country_limits:
            overfetch = max(overfetch, sum(max(0, int(v)) for v in country_limits.values()) * 100)
        stmt = stmt.limit(min(5000, overfetch))
    else:
        stmt = stmt.limit(limit)

    with open_session(cfg) as s:
        rows = list(s.scalars(stmt).all())

    if not distinct_matches and not country_limits and total_product_min is None and total_product_max is None:
        return rows

    remaining: Optional[dict[str, int]] = None
    desired_total = limit
    if country_limits:
        remaining = {str(k).strip().lower(): int(v) for k, v in country_limits.items() if int(v) > 0}
        desired_total = min(limit, sum(remaining.values())) if remaining else limit

    total_min = Decimal(str(total_product_min)) if total_product_min is not None else None
    total_max = Decimal(str(total_product_max)) if total_product_max is not None else None

    def in_total_range(p: Decimal) -> bool:
        if total_min is not None:
            if total_product_min_inclusive:
                if p < total_min:
                    return False
            else:
                if p <= total_min:
                    return False
        if total_max is not None:
            if total_product_max_inclusive:
                if p > total_max:
                    return False
            else:
                if p >= total_max:
                    return False
        return True

    # If no total-product constraint, keep previous “first match per name” behavior (with quotas if any).
    if total_min is None and total_max is None:
        seen: set[str] = set()
        out: list[Event] = []
        for r in rows:
            match_key = (r.event_name or "").strip().lower()
            if not match_key or match_key in seen:
                continue
            if remaining is not None:
                c = (r.group_name or "").strip().lower()
                if c not in remaining or remaining[c] <= 0:
                    continue
                remaining[c] -= 1
            seen.add(match_key)
            out.append(r)
            if len(out) >= desired_total:
                break
        return out

    # Total-product constrained selection: choose exactly `desired_total` rows,
    # each from a different match (event_name), satisfying optional quotas.
    # Note: `rows` may contain multiple outcomes per match; selection enforces uniqueness.
    candidates: list[tuple[str, str, Decimal, Event]] = []
    for r in rows:
        match_key = (r.event_name or "").strip().lower()
        if not match_key:
            continue
        country = (r.group_name or "").strip().lower()
        try:
            v = Decimal(str(r.value).strip())
        except (InvalidOperation, ValueError):
            continue
        if v <= 0:
            continue
        candidates.append((match_key, country, v, r))

    if desired_total <= 0:
        return []

    # Fast pruning: any single value that can't possibly fit given remaining slots.
    min_val = min((c[2] for c in candidates), default=None)
    max_val = max((c[2] for c in candidates), default=None)
    if min_val is None or max_val is None:
        return []

    # Additional upper-bound pruning: if total_max exists, discard candidates that are too large to ever fit.
    if total_max is not None and desired_total >= 2:
        # If one value > total_max / (min_val^(k-1)), it can never be in a valid product.
        try:
            per_item_max = total_max / (min_val ** (desired_total - 1))
            candidates = [c for c in candidates if c[2] <= per_item_max]
        except Exception:
            pass

    # Group by match to avoid choosing multiple outcomes of same match in recursion.
    match_groups: dict[str, dict[str, object]] = {}
    for match_key, country, v, ev in candidates:
        g = match_groups.get(match_key)
        if g is None:
            match_groups[match_key] = {"country": country, "items": [(v, ev)]}
        else:
            g_items = g["items"]  # type: ignore[assignment]
            g_items.append((v, ev))  # type: ignore[attr-defined]

    # Build deterministic list of groups by recency (first appearance in `rows`).
    ordered_match_keys: list[str] = []
    seen_m: set[str] = set()
    for r in rows:
        mk = (r.event_name or "").strip().lower()
        if mk and mk in match_groups and mk not in seen_m:
            ordered_match_keys.append(mk)
            seen_m.add(mk)

    groups: list[tuple[str, str, list[tuple[Decimal, Event]]]] = []
    for mk in ordered_match_keys:
        g = match_groups[mk]
        country = g["country"]  # type: ignore[index]
        items = g["items"]  # type: ignore[index]
        # Sort outcomes by value ascending to help find solutions under upper bound.
        items_sorted = sorted(items, key=lambda t: (t[0], t[1].id))
        groups.append((mk, country, items_sorted))

    # Country quotas, if any.
    remaining_counts = remaining.copy() if remaining is not None else None

    # Precompute global bounds for pruning.
    all_vals = [v for _, _, items in groups for v, _ in items]
    global_min = min(all_vals) if all_vals else min_val
    global_max = max(all_vals) if all_vals else max_val

    def dfs(start_idx: int, k: int, prod: Decimal, rem: Optional[dict[str, int]], chosen: list[Event]) -> Optional[list[Event]]:
        if k == 0:
            return chosen if in_total_range(prod) else None

        # Prune by theoretical min/max reachability.
        if total_max is not None and prod * (global_min**k) > total_max:
            return None
        if total_min is not None and prod * (global_max**k) < total_min:
            return None

        for i in range(start_idx, len(groups)):
            mk, country, items = groups[i]
            if rem is not None:
                if country not in rem or rem[country] <= 0:
                    continue

            for v, ev in items:
                new_prod = prod * v
                if total_max is not None and new_prod > total_max and global_min >= 1:
                    continue

                new_rem = rem
                if rem is not None:
                    new_rem = dict(rem)
                    new_rem[country] -= 1

                res = dfs(i + 1, k - 1, new_prod, new_rem, chosen + [ev])
                if res is not None:
                    return res

        return None

    selected = dfs(0, desired_total, Decimal("1"), remaining_counts, [])
    return selected or []


def distinct_event_names(cfg: DbConfig) -> Iterable[str]:
    stmt = select(Event.event_name).distinct().order_by(Event.event_name.asc())
    with open_session(cfg) as s:
        return [row[0] for row in s.execute(stmt).all()]

