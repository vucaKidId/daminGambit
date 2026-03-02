from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
import random
import os
from typing import Iterable, Optional

from decimal import Decimal, InvalidOperation
from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, cast, create_engine, delete, func, or_, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, selectinload


class Base(DeclarativeBase):
    pass


# Outcome types for match result (used by chess, basketball, handball).
STATIC_EVENT_TYPES: tuple[str, ...] = ("won", "draw", "lost")
# Extra types for football only (total goals bands).
FOOTBALL_GOALS_EVENT_TYPES: tuple[str, ...] = ("goals_0_2", "goals_3_plus")
# All event type names that must exist in DB.
ALL_EVENT_TYPES: tuple[str, ...] = STATIC_EVENT_TYPES + FOOTBALL_GOALS_EVENT_TYPES

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

# Real match names for "upcoming" and "ongoing" seed data (no placeholders like Future01 vs Future31).
UPCOMING_MATCH_NAMES: tuple[str, ...] = (
    "Manchester United vs Liverpool",
    "Real Madrid vs Barcelona",
    "Bayern Munich vs Borussia Dortmund",
    "Inter vs AC Milan",
    "Paris Saint-Germain vs Marseille",
    "Ajax vs PSV Eindhoven",
    "Benfica vs Porto",
    "Chelsea vs Arsenal",
    "Atletico Madrid vs Sevilla",
    "RB Leipzig vs Bayer Leverkusen",
    "Juventus vs Napoli",
    "Lyon vs Monaco",
    "Feyenoord vs AZ Alkmaar",
    "Sporting CP vs Braga",
    "Manchester City vs Tottenham",
    "Real Sociedad vs Villarreal",
    "Eintracht Frankfurt vs Wolfsburg",
    "Roma vs Lazio",
    "Lille vs Nice",
    "Twente vs Utrecht",
    "Vitoria Guimaraes vs Boavista",
    "Newcastle vs Aston Villa",
    "Athletic Bilbao vs Betis",
    "Freiburg vs Union Berlin",
    "Atalanta vs Fiorentina",
    "Rennes vs Lens",
    "Sparta Rotterdam vs Heerenveen",
    "Famalicao vs Gil Vicente",
    "West Ham vs Brighton",
    "Valencia vs Getafe",
)
ONGOING_MATCH_NAMES: tuple[tuple[str, str], ...] = (
    ("Lakers", "Celtics"),
    ("Warriors", "Bucks"),
    ("Real Madrid", "Barcelona"),
    ("Bayern Munich", "Alba Berlin"),
    ("Panathinaikos", "Olympiacos"),
    ("THW Kiel", "SG Flensburg-Handewitt"),
    ("Paris Saint-Germain", "Montpellier"),
    ("Kielce", "Wisla Plock"),
    ("Dinamo București", "CSM București"),
    ("Vardar", "Zagreb"),
    ("Liverpool", "Manchester City"),
    ("Barcelona", "Atletico Madrid"),
    ("Dortmund", "Leipzig"),
    ("Milan", "Juventus"),
    ("Monaco", "Lyon"),
    ("PSV", "Ajax"),
    ("Porto", "Benfica"),
    ("Aston Villa", "Chelsea"),
    ("Sevilla", "Villarreal"),
    ("Leverkusen", "Freiburg"),
    ("Napoli", "Roma"),
    ("Nice", "Lille"),
    ("AZ Alkmaar", "Twente"),
    ("Braga", "Vitoria Guimaraes"),
    ("Brighton", "Fulham"),
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
    round_num: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True, default=None)
    sport: Mapped[Optional[str]] = mapped_column(String, nullable=True, index=True, default=None)

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
    # Migration: add round_num if missing (existing DB from before round_num existed)
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE events ADD COLUMN round_num INTEGER"))
            conn.commit()
    except Exception:
        pass
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE events ADD COLUMN sport VARCHAR"))
            conn.commit()
    except Exception:
        pass
    ensure_event_types(cfg)


def open_session(cfg: DbConfig) -> Session:
    engine = get_engine(cfg)
    return Session(engine)

def ensure_event_types(cfg: DbConfig) -> int:
    with open_session(cfg) as s:
        existing = set(s.scalars(select(EventType.name)).all())
        missing = [t for t in ALL_EVENT_TYPES if t not in existing]
        if missing:
            s.add_all([EventType(name=t) for t in missing])
            s.commit()
        return len(missing)


def reset_db(cfg: DbConfig) -> None:
    engine = get_engine(cfg)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    ensure_event_types(cfg)


def event_count(cfg: DbConfig) -> int:
    """Return the number of events in the database."""
    init_db(cfg)
    with open_session(cfg) as s:
        return s.scalar(select(func.count(Event.id))) or 0


def _count_upcoming_today_matches(cfg: DbConfig) -> int:
    """Count distinct event_name (matches) that are today and end_time is still in the future."""
    init_db(cfg)
    now = datetime.now()
    today_str = now.date().isoformat()
    with open_session(cfg) as s:
        stmt = (
            select(Event.event_name)
            .where(func.date(Event.start_time) == today_str)
            .where(Event.end_time > now)
            .distinct()
        )
        names = list(s.scalars(stmt).all())
        return len(names)


def ensure_today_fixtures(cfg: DbConfig) -> int:
    """If there are fewer than 5 upcoming matches today, add 8 low-odds fixtures so 'za danas Max 5 parova' returns results."""
    if _count_upcoming_today_matches(cfg) >= 5:
        return 0
    init_db(cfg)
    ensure_event_types(cfg)
    with open_session(cfg) as s:
        types = {t.name: t for t in s.scalars(select(EventType)).all()}
        created: list[Event] = []
        # Schedule events in the future so end_time >= now always (webapp filters by end_time_from=now)
        now = datetime.now()
        # 8 real Serbian league / European matches, "won" odds 1.20–1.38 so any 5 have product in range
        danas_odds: list[tuple[str, str, str]] = [
            ("Partizan vs Red Star Belgrade", "1.20", "3.50", "2.50"),
            ("Cukaricki vs Vojvodina", "1.22", "3.60", "2.45"),
            ("Napredak vs Radnicki Nis", "1.25", "3.70", "2.40"),
            ("Spartak Subotica vs Radnik Surdulica", "1.28", "3.80", "2.35"),
            ("Mladost Lucani vs Backa Palanka", "1.30", "3.90", "2.30"),
            ("Javor vs Vozdovac", "1.32", "4.00", "2.25"),
            ("Kolubara vs Radnicki 1923", "1.35", "4.10", "2.20"),
            ("Proleter vs Metalac", "1.38", "4.20", "2.15"),
        ]
        today = now.date()
        end_of_today = datetime.combine(today, datetime.max.time().replace(microsecond=0))
        # 8 start times in the future but still today (so "za danas" and end_time >= now both match)
        first_start = now + timedelta(minutes=30)
        if first_start.date() > today or first_start > end_of_today:
            first_start = now + timedelta(minutes=1)
        window = (end_of_today - first_start).total_seconds()
        if window <= 0:
            starts = [min(now + timedelta(minutes=i + 1), end_of_today) for i in range(8)]
            starts = [s for s in starts if s.date() == today]
        else:
            step_sec = max(60, window / 7)
            starts = [min(first_start + timedelta(seconds=int(step_sec * i)), end_of_today) for i in range(8)]
        while len(starts) < 8 and starts:
            nxt = min(starts[-1] + timedelta(minutes=1), end_of_today)
            if nxt <= starts[-1]:
                break
            starts.append(nxt)
        if not starts:
            starts = [now + timedelta(hours=i + 1) for i in range(8)]
        starts = starts[:8]
        for i, (ev_name, won, draw, lost) in enumerate(danas_odds):
            start = starts[i] if i < len(starts) else (now + timedelta(hours=i + 1))
            end = start + timedelta(hours=2, minutes=5)
            fixed = {"won": won, "draw": draw, "lost": lost}
            for t in STATIC_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=ev_name,
                        group_name="serbia",
                        league="SuperLiga",
                        value=fixed[t],
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                    )
                )
        s.add_all(created)
        s.commit()
        return len(created)


def _count_upcoming_three_days_in_range(cfg: DbConfig, value_min: float = 1.5, value_max: float = 1.8) -> int:
    """Count distinct matches in next 3 days with end_time > now and value in [value_min, value_max]."""
    init_db(cfg)
    now = datetime.now()
    today_str = now.date().isoformat()
    to_date = (now.date() + timedelta(days=3)).isoformat()
    with open_session(cfg) as s:
        stmt = (
            select(Event.event_name)
            .where(func.date(Event.start_time) >= today_str)
            .where(func.date(Event.start_time) <= to_date)
            .where(Event.end_time > now)
            .where(cast(Event.value, Float) >= value_min)
            .where(cast(Event.value, Float) <= value_max)
            .distinct()
        )
        names = list(s.scalars(stmt).all())
        return len(names)


def ensure_upcoming_three_days_fixtures(cfg: DbConfig) -> int:
    """If there are fewer than 10 matches in the next 3 days with odds in 1.5–1.8, add 15 so 'naredna tri dana' + 2h spacing returns 10."""
    if _count_upcoming_three_days_in_range(cfg, 1.5, 1.8) >= 15:
        return 0
    init_db(cfg)
    ensure_event_types(cfg)
    with open_session(cfg) as s:
        types = {t.name: t for t in s.scalars(select(EventType)).all()}
        created: list[Event] = []
        now = datetime.now()
        # 15 real matches, odds 1.52–1.78; 5 start slots per day (12:00, 14:00, 16:00, 18:00, 20:00) so when run in evening we still have 10 slots (day+1 and day+2)
        matches_3d: list[tuple[str, str, str, str]] = [
            ("Real Madrid vs Barcelona", "1.52", "3.80", "2.20"),
            ("Bayern Munich vs Borussia Dortmund", "1.55", "3.90", "2.15"),
            ("Inter vs AC Milan", "1.58", "3.70", "2.25"),
            ("Paris Saint-Germain vs Marseille", "1.60", "3.60", "2.30"),
            ("Manchester City vs Liverpool", "1.62", "3.75", "2.20"),
            ("Ajax vs Feyenoord", "1.65", "3.85", "2.10"),
            ("Benfica vs Sporting CP", "1.68", "3.50", "2.35"),
            ("Atletico Madrid vs Sevilla", "1.70", "3.55", "2.28"),
            ("Juventus vs Napoli", "1.72", "3.45", "2.32"),
            ("RB Leipzig vs Bayer Leverkusen", "1.75", "3.65", "2.18"),
            ("Lyon vs Monaco", "1.78", "3.40", "2.40"),
            ("Porto vs Braga", "1.78", "3.55", "2.25"),
            ("Chelsea vs Arsenal", "1.56", "3.75", "2.22"),
            ("Napoli vs Roma", "1.64", "3.60", "2.28"),
            ("Lille vs Nice", "1.66", "3.70", "2.20"),
        ]
        slots_per_day = 5
        for i, (ev_name, won, draw, lost) in enumerate(matches_3d):
            day_offset = i // slots_per_day
            hour_offset = (i % slots_per_day) * 2
            start = datetime.combine(now.date() + timedelta(days=day_offset), datetime.min.time()) + timedelta(hours=12 + hour_offset, minutes=0)
            if start <= now:
                start = now + timedelta(hours=1 + (i % 8))
            end = start + timedelta(hours=2, minutes=5)
            fixed = {"won": won, "draw": draw, "lost": lost}
            for t in STATIC_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=ev_name,
                        group_name="europe",
                        league="Champions League",
                        value=fixed[t],
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                    )
                )
        s.add_all(created)
        s.commit()
        return len(created)


def ensure_seeded(cfg: DbConfig) -> None:
    """If the database has no events, seed it so the web app returns results."""
    if event_count(cfg) == 0:
        seed_db(cfg, rows=200, replace=False)
    ensure_today_fixtures(cfg)
    ensure_upcoming_three_days_fixtures(cfg)


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

        # England Premier League: "3 matches from England, 1 league, total odd between 3-6"
        england_day = datetime(2026, 3, 15, 15, 0, 0)
        england_fixed: list[tuple[str, str, str, str]] = [
            ("Manchester United vs Liverpool", "1.45", "3.80", "2.30"),
            ("Chelsea vs Arsenal", "1.50", "3.80", "2.10"),
            ("Manchester City vs Tottenham", "1.55", "4.00", "2.05"),
            ("Newcastle vs Aston Villa", "1.60", "3.90", "2.00"),
            ("West Ham vs Brighton", "1.65", "3.70", "1.95"),
        ]
        for i, (ev_name, won_odds, draw_odds, lost_odds) in enumerate(england_fixed):
            start = england_day + timedelta(days=i // 2, hours=(i % 2) * 4)
            add_match_at(
                ev_name,
                "England",
                start=start,
                end=start + timedelta(hours=2, minutes=5),
                league="Premier League",
                fixed={"won": won_odds, "draw": draw_odds, "lost": lost_odds},
            )

        # England Premier League round 24: "all matches for england league in 24 round to finish 0-2 goals"
        r24_day = datetime(2026, 4, 5, 15, 0, 0)
        r24_matches: list[tuple[str, str, str, str, str, str]] = [
            ("Manchester United vs Liverpool", "1.45", "3.80", "2.30", "2.10", "1.75"),
            ("Chelsea vs Arsenal", "1.50", "3.80", "2.10", "2.05", "1.80"),
            ("Manchester City vs Tottenham", "1.55", "4.00", "2.05", "2.00", "1.82"),
            ("Newcastle vs Aston Villa", "1.60", "3.90", "2.00", "1.95", "1.85"),
            ("West Ham vs Brighton", "1.65", "3.70", "1.95", "1.92", "1.88"),
            ("Fulham vs Brentford", "1.70", "3.50", "1.90", "1.90", "1.90"),
            ("Crystal Palace vs Wolves", "1.75", "3.40", "1.85", "1.88", "1.92"),
        ]
        for i, (ev_name, won, draw, lost, g02, g3p) in enumerate(r24_matches):
            start = r24_day + timedelta(days=i // 3, hours=(i % 3) * 3)
            end = start + timedelta(hours=2, minutes=5)
            for t in STATIC_EVENT_TYPES:
                val = {"won": won, "draw": draw, "lost": lost}[t]
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=ev_name,
                        group_name="england",
                        league="Premier League",
                        value=val,
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                        round_num=24,
                    )
                )
            if "goals_0_2" in types and "goals_3_plus" in types:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=ev_name,
                        group_name="england",
                        league="Premier League",
                        value=g02,
                        event_type=types["goals_0_2"],
                        start_time=start,
                        end_time=end,
                        round_num=24,
                    )
                )
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=ev_name,
                        group_name="england",
                        league="Premier League",
                        value=g3p,
                        event_type=types["goals_3_plus"],
                        start_time=start,
                        end_time=end,
                        round_num=24,
                    )
                )

        # Deterministic fixtures for "submitted at 2026-02-20 11:55:00" constraints:
        # we create many matches that are ONGOING at that moment (start <= 11:55 <= end),
        # with low odds so that a 19-pick total product can be <= 1200.
        submit_dt = datetime(2026, 2, 20, 11, 55, 0)
        ongoing_odds = {"won": "1.25", "lost": "1.28", "draw": "10"}
        ongoing_countries = ("Romania", "Bulgaria", "Norway", "France", "Germany", "Spain", "USA", "China", "Hungary", "Ukraine", "Russia", "Switzerland", "Netherlands", "Iceland", "Austria", "Israel", "Estonia", "Serbia", "Croatia", "Italy", "Morocco", "Argentina", "Japan", "Brazil", "Canada")
        for idx, (home, away) in enumerate(ONGOING_MATCH_NAMES):
            ev = f"{home} vs {away}"
            c = ongoing_countries[idx % len(ongoing_countries)]
            start_offset_min = 10 + idx * 2
            duration_min = 70 + idx * 2
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

        # Always include a pool of upcoming low-odds matches (real names) so "give me N events ..."
        # can return results even when run much later than our fixed base dates.
        future_base = datetime(2026, 12, 1, 12, 0, 0)
        future_odds = {"won": "1.20", "lost": "1.22", "draw": "10"}
        for i in range(len(UPCOMING_MATCH_NAMES)):
            ev = UPCOMING_MATCH_NAMES[i]
            start = future_base + timedelta(minutes=(i + 1) * 7)
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


# Top 7 leagues per sport with example teams (country = league country for group_name).
FOOTBALL_LEAGUES: list[tuple[str, str, list[tuple[str, str]]]] = [
    ("Premier League", "England", [("Manchester United", "Liverpool"), ("Chelsea", "Arsenal"), ("Manchester City", "Tottenham"), ("Newcastle", "Aston Villa"), ("West Ham", "Brighton"), ("Fulham", "Brentford"), ("Crystal Palace", "Wolves")]),
    ("La Liga", "Spain", [("Real Madrid", "Barcelona"), ("Atletico Madrid", "Sevilla"), ("Real Sociedad", "Villarreal"), ("Athletic Bilbao", "Betis"), ("Valencia", "Getafe"), ("Osasuna", "Mallorca"), ("Celta Vigo", "Girona")]),
    ("Serie A", "Italy", [("Inter", "AC Milan"), ("Juventus", "Napoli"), ("Roma", "Lazio"), ("Atalanta", "Fiorentina"), ("Torino", "Bologna"), ("Udinese", "Sassuolo"), ("Monza", "Empoli")]),
    ("Bundesliga", "Germany", [("Bayern Munich", "Borussia Dortmund"), ("RB Leipzig", "Bayer Leverkusen"), ("Eintracht Frankfurt", "Wolfsburg"), ("Freiburg", "Union Berlin"), ("Hoffenheim", "Borussia Mönchengladbach"), ("Werder Bremen", "Augsburg"), ("Mainz", "Bochum")]),
    ("Ligue 1", "France", [("Paris Saint-Germain", "Marseille"), ("Lyon", "Monaco"), ("Lille", "Nice"), ("Rennes", "Lens"), ("Strasbourg", "Montpellier"), ("Nantes", "Toulouse"), ("Reims", "Brest")]),
    ("Eredivisie", "Netherlands", [("Ajax", "PSV Eindhoven"), ("Feyenoord", "AZ Alkmaar"), ("Twente", "Utrecht"), ("Sparta Rotterdam", "Heerenveen"), ("Vitesse", "Groningen"), ("Fortuna Sittard", "RKC Waalwijk"), ("Cambuur", "Volendam")]),
    ("Primeira Liga", "Portugal", [("Benfica", "Porto"), ("Sporting CP", "Braga"), ("Vitoria Guimaraes", "Boavista"), ("Famalicao", "Gil Vicente"), ("Rio Ave", "Santa Clara"), ("Maritimo", "Portimonense"), ("Casa Pia", "Estoril")]),
]
BASKETBALL_LEAGUES: list[tuple[str, str, list[tuple[str, str]]]] = [
    ("NBA", "USA", [("Lakers", "Celtics"), ("Warriors", "Bucks"), ("Heat", "Nuggets"), ("Suns", "76ers"), ("Mavericks", "Clippers"), ("Grizzlies", "Cavaliers"), ("Knicks", "Nets")]),
    ("EuroLeague", "Europe", [("Real Madrid", "Barcelona"), ("CSKA Moscow", "Fenerbahce"), ("Maccabi Tel Aviv", "Olympiacos"), ("Panathinaikos", "Anadolu Efes"), ("Zalgiris", "Baskonia"), ("Monaco", "Virtus Bologna"), ("Partizan", "Bayern Munich")]),
    ("Liga ACB", "Spain", [("Real Madrid", "Barcelona"), ("Baskonia", "Valencia"), ("Unicaja", "Gran Canaria"), ("Joventut", "Murcia"), ("Bilbao", "Manresa"), ("Breogan", "Obradoiro"), ("Tenerife", "Saragossa")]),
    ("Legabasket Serie A", "Italy", [("Olimpia Milano", "Virtus Bologna"), ("Reggio Emilia", "Tortona"), ("Brescia", "Brindisi"), ("Trieste", "Venezia"), ("Napoli", "Cremona"), ("Pesaro", "Treviso"), ("Varese", "Scafati")]),
    ("Betclic Elite", "France", [("Monaco", "ASVEL"), ("Paris", "Bourg-en-Bresse"), ("Strasbourg", "Le Mans"), ("Nancy", "Dijon"), ("Limonoges", "Cholet"), ("Gravelines", "Nanterre"), ("Pau-Lacq-Orthez", "Roanne")]),
    ("BBL", "Germany", [("Bayern Munich", "Alba Berlin"), ("Bonn", "Ulm"), ("Ludwigsburg", "Bamberg"), ("Oldenburg", "Braunschweig"), ("Göttingen", "Crailsheim"), ("Rasta Vechta", "Chemnitz"), ("Würzburg", "Rostock")]),
    ("Greek Basket League", "Greece", [("Panathinaikos", "Olympiacos"), ("AEK Athens", "PAOK"), ("Aris", "Promitheas"), ("Lavrio", "Ionikos"), ("Kolossos", "Peristeri"), ("Larissa", "Karditsa"), ("Iraklis", "Apollon Patras")]),
]
HANDBALL_LEAGUES: list[tuple[str, str, list[tuple[str, str]]]] = [
    ("Bundesliga", "Germany", [("THW Kiel", "SG Flensburg-Handewitt"), ("SC Magdeburg", "Rhein-Neckar Löwen"), ("SC DHfK Leipzig", "Füchse Berlin"), ("MT Melsungen", "HBW Balingen-Weilstetten"), ("Frisch Auf Göppingen", "TVB Stuttgart"), ("TSV Hannover-Burgdorf", "HSG Wetzlar"), ("VfL Gummersbach", "TBV Lemgo")]),
    ("Lidl Starligue", "France", [("Paris Saint-Germain", "Montpellier"), ("Nantes", "Dunkerque"), ("Toulouse", "Saint-Raphaël"), ("Chambéry", "Aix"), ("Cesson-Rennes", "Ivry"), ("Limoges", "Nancy"), ("Dijon", "Pontarlier")]),
    ("Liga ASOBAL", "Spain", [("Barcelona", "Bidasoa"), ("Naturhouse La Rioja", "Granollers"), ("Benidorm", "Ademar León"), ("Puerto Sagunto", "Cuenca"), ("Antequera", "Logroño"), ("Huesca", "Guadalajara"), ("Villa de Aranda", "Nava")]),
    ("Polish Superliga", "Poland", [("Kielce", "Wisla Plock"), ("Górnik Zabrze", "Azoty-Pulawy"), ("Piotrkowianin", "MKS Poznan"), ("Słupsk", "Gwardia Opole"), ("MMTS Kwidzyn", "Warmia Olsztyn"), ("Sparta Katowice", "Siól Oswiecim"), ("Chrobry Glogow", "Ostrovia")]),
    ("Liga Națională", "Romania", [("Dinamo București", "CSM București"), ("CS Minaur Baia Mare", "CSU Suceava"), ("Bistrița", "Constanța"), ("Odorheiu Secuiesc", "Poli Timișoara"), ("Dobrogea Sud", "Dunărea Călărași"), ("Craiova", "Braila"), ("Buzău", "Pitești")]),
    ("Premier Handball League", "UK", [("London GD", "NEM Hawks"), ("Warrington Wolves", "Olympia Liverpool"), ("Nottingham", "Cambridge"), ("Ruislip", "Brighton"), ("Manchester", "Oxford"), ("Cardiff", "Swansea"), ("Edinburgh", "Glasgow")]),
    ("SEHA League", "Regional", [("Vardar", "Zagreb"), ("Meshkov Brest", "Veszprém"), ("Celje", "Metalurg"), ("Nexe", "Tatran Presov"), ("PPD Zagreb", "Borac Banja Luka"), ("Vojvodina", "Spartak Vojput"), ("Lovcen", "Dinamo Pancevo")]),
]


def seed_sports_db(cfg: DbConfig, *, replace: bool = False) -> int:
    """Seed football (won/draw/lost + goals_0_2, goals_3_plus), basketball and handball (won/draw/lost only)."""
    init_db(cfg)
    ensure_event_types(cfg)

    with open_session(cfg) as s:
        types = {t.name: t for t in s.scalars(select(EventType)).all()}
        if replace:
            s.execute(delete(Event))
            s.commit()

        rng = random.Random(43)
        created: list[Event] = []
        base = datetime(2026, 3, 1, 15, 0, 0)

        def odds_outcome(rng: random.Random) -> dict[str, str]:
            return {
                "won": f"{rng.uniform(1.4, 2.8):.2f}",
                "draw": f"{rng.uniform(3.0, 4.5):.2f}",
                "lost": f"{rng.uniform(1.4, 2.8):.2f}",
            }

        def odds_goals(rng: random.Random) -> dict[str, str]:
            return {
                "goals_0_2": f"{rng.uniform(1.9, 2.8):.2f}",
                "goals_3_plus": f"{rng.uniform(1.5, 2.2):.2f}",
            }

        used_sports: set[tuple[str, str, str]] = set()

        def add_football_match(league_name: str, country: str, home: str, away: str, round_num: int, match_num: int) -> None:
            key = ("football", league_name, f"{home} vs {away}")
            if key in used_sports:
                return
            used_sports.add(key)
            start = base + timedelta(days=round_num * 7 + match_num, hours=rng.randint(12, 20), minutes=rng.choice([0, 15, 30, 45]))
            end = start + timedelta(hours=2, minutes=5)
            event_name = f"{home} vs {away}"
            group_name = country.lower()
            outcome = odds_outcome(rng)
            goals = odds_goals(rng)
            for t in STATIC_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=event_name,
                        group_name=group_name,
                        league=league_name,
                        value=outcome[t],
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                        sport="football",
                    )
                )
            for t in FOOTBALL_GOALS_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=event_name,
                        group_name=group_name,
                        league=league_name,
                        value=goals[t],
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                        sport="football",
                    )
                )

        def add_basketball_match(league_name: str, country: str, home: str, away: str, round_num: int, match_num: int) -> None:
            key = ("basketball", league_name, f"{home} vs {away}")
            if key in used_sports:
                return
            used_sports.add(key)
            start = base + timedelta(days=round_num * 5 + match_num + 30, hours=rng.randint(18, 21), minutes=rng.choice([0, 30]))
            end = start + timedelta(hours=2, minutes=15)
            event_name = f"{home} vs {away}"
            group_name = country.lower()
            outcome = odds_outcome(rng)
            for t in STATIC_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=event_name,
                        group_name=group_name,
                        league=league_name,
                        value=outcome[t],
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                        sport="basketball",
                    )
                )

        def add_handball_match(league_name: str, country: str, home: str, away: str, round_num: int, match_num: int) -> None:
            key = ("handball", league_name, f"{home} vs {away}")
            if key in used_sports:
                return
            used_sports.add(key)
            start = base + timedelta(days=round_num * 5 + match_num + 60, hours=rng.randint(17, 20), minutes=rng.choice([0, 30]))
            end = start + timedelta(hours=1, minutes=30)
            event_name = f"{home} vs {away}"
            group_name = country.lower()
            outcome = odds_outcome(rng)
            for t in STATIC_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=event_name,
                        group_name=group_name,
                        league=league_name,
                        value=outcome[t],
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                        sport="handball",
                    )
                )

        # Guaranteed England Premier League fixtures so "3 matches from England, total odd 3-6" returns results.
        # Won odds chosen so product of any 3 is in [3, 6]: e.g. 1.45*1.55*1.50=3.37, 1.60*1.65*1.70=4.49.
        england_base = datetime(2026, 3, 15, 15, 0, 0)
        england_fixtures: list[tuple[str, str, str, str]] = [
            ("Manchester United vs Liverpool", "1.45", "3.80", "2.30"),
            ("Chelsea vs Arsenal", "1.50", "3.80", "2.10"),
            ("Manchester City vs Tottenham", "1.55", "4.00", "2.05"),
            ("Newcastle vs Aston Villa", "1.60", "3.90", "2.00"),
            ("West Ham vs Brighton", "1.65", "3.70", "1.95"),
            ("Fulham vs Brentford", "1.70", "3.50", "1.90"),
            ("Crystal Palace vs Wolves", "1.75", "3.40", "1.85"),
            ("Everton vs Bournemouth", "1.80", "3.30", "1.80"),
            ("Nottingham Forest vs Leicester", "1.85", "3.25", "1.75"),
            ("Ipswich vs Southampton", "1.90", "3.20", "1.70"),
        ]
        for i, (event_name, won_odds, draw_odds, lost_odds) in enumerate(england_fixtures):
            start = england_base + timedelta(days=i // 3, hours=(i % 3) * 4)
            end = start + timedelta(hours=2, minutes=5)
            fixed: dict[str, str] = {"won": won_odds, "draw": draw_odds, "lost": lost_odds}
            goals = {"goals_0_2": "2.10", "goals_3_plus": "1.75"}
            key = ("football", "Premier League", event_name.lower())
            if key in used_sports:
                continue
            used_sports.add(key)
            group_name = "england"
            for t in STATIC_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=event_name,
                        group_name=group_name,
                        league="Premier League",
                        value=fixed[t],
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                        sport="football",
                    )
                )
            for t in FOOTBALL_GOALS_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=event_name,
                        group_name=group_name,
                        league="Premier League",
                        value=goals[t],
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                        sport="football",
                    )
                )

        # Guaranteed 3 football + 3 basketball with won=1.5 so "3 fudbal, 3 kosarka, ukupna kvota 11" has a solution (1.5^6 ≈ 11.39).
        # Schedule early (March 1) so they appear first when ordered by start_time and DFS finds them.
        sport_combo_base = datetime(2026, 3, 1, 10, 0, 0)
        for i, (ev_name, grp, lig, sp) in enumerate([
            ("Team A vs Team B", "england", "Premier League", "football"),
            ("Team C vs Team D", "spain", "La Liga", "football"),
            ("Team E vs Team F", "germany", "Bundesliga", "football"),
            ("Alpha vs Beta", "usa", "NBA", "basketball"),
            ("Gamma vs Delta", "europe", "EuroLeague", "basketball"),
            ("Epsilon vs Zeta", "spain", "Liga ACB", "basketball"),
        ]):
            start = sport_combo_base + timedelta(days=i, hours=i)
            end = start + timedelta(hours=2)
            for t in STATIC_EVENT_TYPES:
                created.append(
                    Event(
                        tenant_id="default",
                        event_name=ev_name,
                        group_name=grp,
                        league=lig,
                        value="1.50" if t == "won" else "3.00",
                        event_type=types[t],
                        start_time=start,
                        end_time=end,
                        sport=sp,
                    )
                )

        # Football: 7 leagues × 7 matches × 2 rounds = 98 matches, 98×5 = 490 rows
        for league_name, country, teams in FOOTBALL_LEAGUES:
            for round_num in range(2):
                for match_num, (home, away) in enumerate(teams):
                    add_football_match(league_name, country, home, away, round_num, match_num)
        # Basketball: 7 leagues × 7 matches × 2 rounds = 98 matches, 98×3 = 294 rows
        for league_name, country, teams in BASKETBALL_LEAGUES:
            for round_num in range(2):
                for match_num, (home, away) in enumerate(teams):
                    add_basketball_match(league_name, country, home, away, round_num, match_num)
        # Handball: 7 leagues × 7 matches × 2 rounds = 98 matches, 98×3 = 294 rows
        for league_name, country, teams in HANDBALL_LEAGUES:
            for round_num in range(2):
                for match_num, (home, away) in enumerate(teams):
                    add_handball_match(league_name, country, home, away, round_num, match_num)

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
    min_hours_between: Optional[float] = None,
    round_num: Optional[int] = None,
    sport_limits: Optional[dict[str, int]] = None,
    include_teams: Optional[list[str]] = None,
) -> list[Event]:
    def _apply_min_hours_between(events: list[Event], min_hours: float, cap: int) -> list[Event]:
        if not events or min_hours <= 0:
            return events[:cap]
        sorted_events = sorted(events, key=lambda e: e.start_time or datetime.min)
        result: list[Event] = []
        for ev in sorted_events:
            if not result:
                result.append(ev)
                continue
            delta = (ev.start_time - result[-1].start_time).total_seconds() if ev.start_time and result[-1].start_time else 0
            if delta >= min_hours * 3600:
                result.append(ev)
                if len(result) >= cap:
                    break
        return result

    stmt = select(Event).options(selectinload(Event.event_type)).order_by(Event.start_time.asc(), Event.id.asc())
    if tenant_id:
        stmt = stmt.where(func.lower(Event.tenant_id) == tenant_id.lower())
    if event_name:
        stmt = stmt.where(func.lower(Event.event_name) == event_name.lower())
    if include_teams:
        stmt = stmt.where(
            or_(*[func.lower(Event.event_name).like("%" + t.lower() + "%") for t in include_teams])
        )
        distinct_matches = True
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
    if round_num is not None:
        stmt = stmt.where(Event.round_num == round_num)
    if end_time_from:
        stmt = stmt.where(Event.end_time >= end_time_from)
    if end_time_to:
        stmt = stmt.where(Event.end_time <= end_time_to)
    if sport_limits:
        stmt = stmt.where(Event.sport.in_([s for s in sport_limits if sport_limits[s] > 0]))
    if country_limits or sport_limits or total_product_min is not None or total_product_max is not None:
        # Quotas / total-product constraints imply one row per match.
        distinct_matches = True

    if distinct_matches:
        # Over-fetch and dedupe in Python by match name (event_name).
        # The requested `limit` is interpreted as “matches”, not “rows”.
        overfetch = max(200, limit * 50)
        if country_limits:
            overfetch = max(overfetch, sum(max(0, int(v)) for v in country_limits.values()) * 100)
        if sport_limits:
            overfetch = max(overfetch, sum(max(0, int(v)) for v in sport_limits.values()) * 100)
        if min_hours_between is not None and min_hours_between > 0:
            overfetch = max(overfetch, limit * 15)
        stmt = stmt.limit(min(5000, overfetch))
    else:
        stmt = stmt.limit(limit)

    with open_session(cfg) as s:
        rows = list(s.scalars(stmt).all())

    if not distinct_matches and not country_limits and not sport_limits and total_product_min is None and total_product_max is None:
        if min_hours_between is not None and min_hours_between > 0:
            return _apply_min_hours_between(rows, min_hours_between, limit)
        return rows

    remaining: Optional[dict[str, int]] = None
    desired_total = limit
    if country_limits:
        remaining = {str(k).strip().lower(): int(v) for k, v in country_limits.items() if int(v) > 0}
        desired_total = min(limit, sum(remaining.values())) if remaining else limit
    if sport_limits:
        remaining = {str(k).strip().lower(): int(v) for k, v in sport_limits.items() if int(v) > 0}
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
        # When min_hours_between is set, collect more candidates so 2h spacing can still yield desired_total
        cap = desired_total * 5 if (min_hours_between and min_hours_between > 0) else desired_total
        cap = min(cap, len(rows)) if rows else desired_total
        for r in rows:
            match_key = (r.event_name or "").strip().lower()
            if not match_key or match_key in seen:
                continue
            if remaining is not None:
                c = (r.sport or "").strip().lower() if sport_limits else (r.group_name or "").strip().lower()
                if c not in remaining or remaining[c] <= 0:
                    continue
                remaining[c] -= 1
            seen.add(match_key)
            out.append(r)
            if len(out) >= cap:
                break
        if min_hours_between is not None and min_hours_between > 0:
            out = _apply_min_hours_between(out, min_hours_between, desired_total)
        return out

    # Total-product constrained selection: choose exactly `desired_total` rows,
    # each from a different match (event_name), satisfying optional quotas.
    # Note: `rows` may contain multiple outcomes per match; selection enforces uniqueness.
    candidates: list[tuple[str, str, Decimal, Event]] = []
    for r in rows:
        match_key = (r.event_name or "").strip().lower()
        if not match_key:
            continue
        country = (r.sport or "").strip().lower() if sport_limits else (r.group_name or "").strip().lower()
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
    if selected and min_hours_between is not None and min_hours_between > 0:
        selected = _apply_min_hours_between(selected, min_hours_between, desired_total)
    return selected or []


def distinct_event_names(cfg: DbConfig) -> Iterable[str]:
    stmt = select(Event.event_name).distinct().order_by(Event.event_name.asc())
    with open_session(cfg) as s:
        return [row[0] for row in s.execute(stmt).all()]

