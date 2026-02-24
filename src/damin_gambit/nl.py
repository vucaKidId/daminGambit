from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Dict, List, Optional

from dateutil import parser as date_parser


@dataclass(frozen=True)
class QuerySpec:
    event_name: Optional[str] = None
    group_name: Optional[str] = None
    group_names: Optional[List[str]] = None
    country_limits: Optional[Dict[str, int]] = None
    league: Optional[str] = None
    type_: Optional[str] = None
    value_min: Optional[float] = None
    value_min_inclusive: bool = True
    value_max: Optional[float] = None
    value_max_inclusive: bool = True
    total_product_min: Optional[float] = None
    total_product_min_inclusive: bool = True
    total_product_max: Optional[float] = None
    total_product_max_inclusive: bool = True
    start_time_from: Optional[datetime] = None
    start_time_to: Optional[datetime] = None
    submitted_at: Optional[datetime] = None
    include_event_name: Optional[str] = None
    include_type_: Optional[str] = None
    limit: int = 50
    distinct_matches: bool = False
    intent: str = "list"


_QUOTED_RE = re.compile(r"(?P<q>['\"])(?P<val>.*?)(?P=q)")
_VS_RE = re.compile(r"\b(?P<a>[A-Za-z][A-Za-z .'-]{1,60}?)\s+vs\s+(?P<b>[A-Za-z][A-Za-z .'-]{1,60}?)\b", re.I)
_NUM_RE = re.compile(r"\b(?P<n>\d+(?:\.\d+)?)\b")

_SR_NUM_WORDS: dict[str, int] = {
    "jedan": 1,
    "jedna": 1,
    "jedno": 1,
    "dva": 2,
    "dve": 2,
    "dvije": 2,
    "tri": 3,
    "cetiri": 4,
    "četiri": 4,
    "pet": 5,
    "sest": 6,
    "šest": 6,
    "sedam": 7,
    "osam": 8,
    "devet": 9,
    "deset": 10,
}

_SR_COUNTRY_FORMS: dict[str, str] = {
    "bugarska": "bulgaria",
    "bugarske": "bulgaria",
    "bugarskoj": "bulgaria",
    "norveska": "norway",
    "norveška": "norway",
    "norveske": "norway",
    "norveške": "norway",
    "norveskoj": "norway",
    "norveškoj": "norway",
    "rumunija": "romania",
    "rumunije": "romania",
    "rumuniji": "romania",
}

_BG_COUNTRY_FORMS: dict[str, str] = {
    "българия": "bulgaria",
    "румъния": "romania",
    "норвегия": "norway",
}


def _parse_dt(s: str) -> Optional[datetime]:
    s = s.strip()
    if not s:
        return None
    try:
        return date_parser.parse(s)
    except Exception:
        return None


def _extract_after_keyword(text: str, keyword: str) -> Optional[str]:
    # Example: "event deploy", "event: deploy", "event=deploy"
    m = re.search(rf"\b{re.escape(keyword)}\b\s*(?:name\s*)?(?::|=)?\s*(.+)$", text, re.I)
    if not m:
        return None

    tail = m.group(1).strip()
    # Stop at next keyword-ish boundary to avoid swallowing dates etc.
    stop = re.split(
        r"\b(group|country|league|tournament|type|since|after|before|until|between|and|limit)\b",
        tail,
        maxsplit=1,
        flags=re.I,
    )[
        0
    ].strip()
    if not stop:
        return None
    val = stop.strip(" ,.")
    val = val.strip("\"'")
    return val or None


def parse(text: str) -> QuerySpec:
    raw = (text or "").strip()
    if not raw:
        return QuerySpec(intent="empty")

    lowered = raw.lower()

    # Support compound queries like:
    # "give me 4 matches with value between 3 and 5 and one more match Anand vs Radjabov won"
    # We treat the "one more match ..." clause as an extra include and remove it from the base query text.
    include_event_name: Optional[str] = None
    include_type_: Optional[str] = None
    m_one_more = re.search(r"\b(?:and\s+)?one\s+more\s+match\b(?P<rest>.+)$", raw, re.I)
    if m_one_more:
        rest = m_one_more.group("rest") or ""
        m_vs_extra = _VS_RE.search(rest)
        if m_vs_extra:
            include_event_name = f"{m_vs_extra.group('a').strip()} vs {m_vs_extra.group('b').strip()}".lower()
            m_t = re.search(r"\b(won|draw|lost|win|lose|loss)\b", rest, re.I)
            if m_t:
                t = m_t.group(1).lower()
                include_type_ = {"win": "won", "lose": "lost", "loss": "lost"}.get(t, t)
        raw = raw[: m_one_more.start()].strip()
        lowered = raw.lower()
    intent = "list"
    if any(k in lowered for k in ["value for", "what is the value", "get value", "show value"]):
        intent = "get_value"

    event_name = _extract_after_keyword(raw, "event")
    group_name = _extract_after_keyword(raw, "group") or _extract_after_keyword(raw, "country")
    league = _extract_after_keyword(raw, "league") or _extract_after_keyword(raw, "tournament")
    type_ = _extract_after_keyword(raw, "type")

    # If "type" was used in a generic phrase like "event type values ...",
    # don't treat the tail as an outcome type filter.
    if type_:
        t_low = type_.strip().lower()
        if t_low not in {"won", "draw", "lost"} and re.search(r"\bvalue|values|lower|less|greater|then|than\b", t_low):
            type_ = None

    # Free-form type mention (won/draw/lost) even if "type" keyword wasn't used.
    if not type_:
        m_type = re.search(r"\b(won|draw|lost|win|lose|loss)\b", lowered, re.I)
        if m_type:
            t = m_type.group(1).lower()
            type_ = {"win": "won", "lose": "lost", "loss": "lost"}.get(t, t)
    elif type_:
        t = type_.strip().lower()
        type_ = {"win": "won", "lose": "lost", "loss": "lost"}.get(t, t)

    # Free-form country mention: "in Norway"
    if not group_name:
        m_in = re.search(
            r"\bin\s+(?P<c>[A-Za-z][A-Za-z .'-]{1,60}?)(?=\s+\b(won|draw|lost|type|since|after|before|until|between|limit)\b|$)",
            raw,
            re.I,
        )
        if m_in:
            group_name = m_in.group("c").strip()

    # Free-form country mention: "from Norway"
    if not group_name:
        m_from = re.search(
            r"\bfrom\s+(?P<c>[A-Za-z][A-Za-z .'-]{1,60}?)(?=\s+\b(won|draw|lost|type|since|after|before|until|between|limit|with)\b|$)",
            raw,
            re.I,
        )
        if m_from:
            group_name = m_from.group("c").strip()

    # Serbian: "iz Rumunije"
    if not group_name:
        m_iz = re.search(r"\biz\s+(?P<c>[a-zčćšđž]+)\b", lowered, re.I)
        if m_iz:
            c_raw = (m_iz.group("c") or "").strip().lower()
            group_name = _SR_COUNTRY_FORMS.get(c_raw, c_raw)

    # Bulgarian: "от България"
    if not group_name:
        m_ot = re.search(r"\bот\s+(?P<c>[a-zа-яёіїєъь]+)\b", lowered, re.I)
        if m_ot:
            c_raw = (m_ot.group("c") or "").strip().lower()
            group_name = _BG_COUNTRY_FORMS.get(c_raw, c_raw)

    # Free-form match name: "A vs B"
    if not event_name:
        m_vs = _VS_RE.search(raw)
        if m_vs:
            event_name = f"{m_vs.group('a').strip()} vs {m_vs.group('b').strip()}"

    if event_name:
        event_name = event_name.strip().lower()
    if group_name:
        group_name = group_name.strip().lower()
    if league:
        league = league.strip().lower()
    if type_:
        type_ = type_.strip().lower()

    # If user gave a single quoted token and no explicit event/group/type, treat it as an event name.
    quoted = [m.group("val") for m in _QUOTED_RE.finditer(raw)]
    if quoted and not (event_name or group_name or type_):
        event_name = quoted[0].strip()

    start_time_from: Optional[datetime] = None
    start_time_to: Optional[datetime] = None
    submitted_at: Optional[datetime] = None

    # Value (odds) constraints
    value_min: Optional[float] = None
    value_max: Optional[float] = None
    value_min_inclusive = True
    value_max_inclusive = True

    # Explicit window: "starts between 12:00 and 17:00 on 25.02.2025"
    m_start_window = re.search(
        r"\bstarts?\b.*?\bbetween\b\s+(?P<t1>\d{1,2}:\d{2})\s+\band\b\s+(?P<t2>\d{1,2}:\d{2})\s+\bon\b\s+(?P<d>\d{1,2}[./-]\d{1,2}[./-]\d{2,4}|\d{4}-\d{2}-\d{2})\b",
        lowered,
        re.I,
    )
    if m_start_window:
        d = m_start_window.group("d")
        t1 = m_start_window.group("t1")
        t2 = m_start_window.group("t2")
        # dateutil handles both dd.mm.yyyy and yyyy-mm-dd reasonably well.
        start_time_from = _parse_dt(f"{d} {t1}")
        start_time_to = _parse_dt(f"{d} {t2}")

    # Explicit lower bound: "starts after 12:00 28.02.2025" (also allow "on")
    m_start_after = re.search(
        r"\bstarts?\b.*?\bafter\b\s+(?P<t>\d{1,2}:\d{2})\s+(?:on\s+)?(?P<d>\d{1,2}[./-]\d{1,2}[./-]\d{2,4}|\d{4}-\d{2}-\d{2})\b",
        lowered,
        re.I,
    )
    if m_start_after and start_time_from is None:
        d = m_start_after.group("d")
        t = m_start_after.group("t")
        start_time_from = _parse_dt(f"{d} {t}")

    # Submission time (used as "now" for filtering): e.g. a timestamp line after "submitovanja"
    if re.search(r"\bsubmit\w*\b", lowered):
        dt_matches = re.findall(r"\b\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\b", raw)
        if dt_matches:
            submitted_at = _parse_dt(dt_matches[-1])

    handled_between_as_value = False
    m_value_between = re.search(
        r"\b(?:value|values|odds)\b.*?\bbetween\b\s+(?P<a>\d+(?:\.\d+)?)\s+\band\b\s+(?P<b>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_value_between and not re.search(r"\btotal\s+(?:value|product)\b", lowered):
        try:
            value_min = float(m_value_between.group("a"))
            value_max = float(m_value_between.group("b"))
            value_min_inclusive = True
            value_max_inclusive = True
            handled_between_as_value = True
        except Exception:
            pass

    m_between = None if handled_between_as_value else re.search(r"\bbetween\b\s+(?P<a>.+?)\s+\band\b\s+(?P<b>.+)$", raw, re.I)
    if m_between:
        a = m_between.group("a").strip()
        b = m_between.group("b").strip()
        # Avoid treating numeric ranges (e.g. "total value between 5 and 9") as date ranges.
        a_is_num = bool(re.fullmatch(r"\d+(?:\.\d+)?", a))
        b_is_num = bool(re.fullmatch(r"\d+(?:\.\d+)?", b))
        looks_like_value_range = a_is_num and b_is_num and re.search(r"\b(total|value|values|product|kvot|kvota|kvotom)\b", lowered)
        if not looks_like_value_range and start_time_from is None and start_time_to is None:
            start_time_from = _parse_dt(a)
            start_time_to = _parse_dt(b)
    else:
        m_since = re.search(r"\b(?:since|after)\b\s+(?P<d>.+)$", raw, re.I)
        if m_since:
            start_time_from = _parse_dt(m_since.group("d"))

        m_before = re.search(r"\b(?:before|until)\b\s+(?P<d>.+)$", raw, re.I)
        if m_before:
            start_time_to = _parse_dt(m_before.group("d"))

    limit = 50
    m_limit = re.search(r"\blimit\b\s+(?P<n>\d+)\b", raw, re.I)
    if m_limit:
        try:
            limit = max(1, min(500, int(m_limit.group("n"))))
        except Exception:
            pass

    # "Give me 3 matches/events/games ..." (no explicit "limit")
    if limit == 50:
        # Serbian: "daj mi dvije utakmice"
        m_sr_need = re.search(
            r"\b(?:daj\s+mi|daj)\s+(?P<n>\d+|[a-zčćšđž]+)\s+(?P<what>utakmic\w*|me[cč]ev\w*|partij\w*|dogadjaj\w*|događaj\w*)\b",
            lowered,
            re.I,
        )
        if m_sr_need:
            n_raw = (m_sr_need.group("n") or "").strip().lower()
            try:
                n = int(n_raw)
            except Exception:
                n = _SR_NUM_WORDS.get(n_raw, 0)
            if n > 0:
                limit = max(1, min(500, int(n)))

        # Bulgarian: "Трябват ми 2 мача ..."
        m_bg_need = re.search(
            r"\b(?:трябват\s+ми|искам|дай\s+ми)\s+(?P<n>\d+)\s+(?P<what>мач\w*|срещ\w*)\b",
            lowered,
            re.I,
        )
        if m_bg_need:
            try:
                limit = max(1, min(500, int(m_bg_need.group("n"))))
            except Exception:
                pass

        # Range: "I need 4 to 6 matches"
        m_range = re.search(
            r"\b(?:give me|show me|get me|list|i need|need)\s+(?P<a>\d+)\s*(?:to|-)\s*(?P<b>\d+)\s+(?P<what>matches?|matchs?|games?|events?)\b",
            raw,
            re.I,
        )
        if m_range:
            try:
                a = int(m_range.group("a"))
                b = int(m_range.group("b"))
                if a > b:
                    a, b = b, a
                limit = max(1, min(500, b))
            except Exception:
                pass

        m_n_items = re.search(
            r"\b(?:give me|show me|get me|list|i need|need)\s+(?P<n>\d+)\s+(?P<what>matches?|matchs?|games?|events?)\b",
            raw,
            re.I,
        )
        if not m_n_items:
            m_n_items = re.search(r"\b(?P<n>\d+)\s+(?P<what>matches?|matchs?|games?|events?)\b", raw, re.I)
        if m_n_items:
            try:
                limit = max(1, min(500, int(m_n_items.group("n"))))
            except Exception:
                pass

    distinct_matches = bool(
        re.search(r"\b(matches?|matchs?|games?|utakmic\w*|me[cč]ev\w*|partij\w*|мач\w*|срещ\w*)\b", raw, re.I)
    )

    # Numeric comparisons on value (odds): "lower than 1.6", "greater than 2.0", etc.
    total_product_min: Optional[float] = None
    total_product_max: Optional[float] = None
    total_product_min_inclusive = True
    total_product_max_inclusive = True

    # English: "total value between 5 and 9"
    m_total_between = re.search(
        r"\btotal\s+(?:value|product)\b.*?\bbetween\b\s+(?P<a>\d+(?:\.\d+)?)\s+\band\b\s+(?P<b>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_total_between:
        try:
            total_product_min = float(m_total_between.group("a"))
            total_product_max = float(m_total_between.group("b"))
            total_product_min_inclusive = True
            total_product_max_inclusive = True
            distinct_matches = True
        except Exception:
            pass

    # English: "total odd less than 2" / "total odds under 2"
    m_total_odd_lt = re.search(
        r"\btotal\s+odd[s]?\b.*?\b(?:lower|less|under|below)\b(?:\s+than|\s+then)?\s+(?P<n>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_total_odd_lt:
        try:
            total_product_max = float(m_total_odd_lt.group("n"))
            total_product_max_inclusive = False
            distinct_matches = True
        except Exception:
            pass

    # Serbian: "ukupnom kvotom/koeficijentom ... ne vec(om) od 1200"  => total product <= 1200
    m_sr_total_lte = re.search(
        r"\bukupn\w*\s+(?:kvot\w*|kovt\w*|koeficijent\w*)\b.*?\bne\s+vec\w*\s+od\s+(?P<n>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_sr_total_lte:
        try:
            total_product_max = float(m_sr_total_lte.group("n"))
            total_product_max_inclusive = True
            distinct_matches = True
        except Exception:
            pass

    # If user says matches didn't start after submission time => start_time_to = submitted_at
    if submitted_at and re.search(r"\bnisu\s+startov\w*\s+nakon\s+submit\w*\b", lowered, re.I):
        if start_time_to is None or start_time_to > submitted_at:
            start_time_to = submitted_at

    # Bulgarian: "с общ коефициент по-малък от 2" (total odds < 2)
    m_bg_total_lt = re.search(
        r"\bобщ\w*\s+коефициент\w*\b.*?\bпо[-\s]?малък\w*\s+от\s+(?P<n>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_bg_total_lt:
        try:
            total_product_max = float(m_bg_total_lt.group("n"))
            total_product_max_inclusive = False
            distinct_matches = True
        except Exception:
            pass

    # Serbian: "sa ukupnom vrijednoscu vecom od 3" (total product > 3)
    m_sr_total_gt = re.search(
        r"\bukupn\w*\s+(?:vrijednos\w*|vrednos\w*)\b.*?\b(?:vec\w*|već\w*|iznad|preko)\b(?:\s+od)?\s+(?P<n>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_sr_total_gt:
        try:
            total_product_min = float(m_sr_total_gt.group("n"))
            total_product_min_inclusive = False
            distinct_matches = True
        except Exception:
            pass

    m_sr_total_lt = re.search(
        r"\bukupn\w*\s+(?:vrijednos\w*|vrednos\w*)\b.*?\b(?:manje|ispod)\b(?:\s+od)?\s+(?P<n>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_sr_total_lt:
        try:
            total_product_max = float(m_sr_total_lt.group("n"))
            total_product_max_inclusive = False
            distinct_matches = True
        except Exception:
            pass

    # Serbian range: "izmedju 1.3 i 1.8" (also accept "između")
    m_sr_between = re.search(
        r"\bizmedju\b\s+(?P<a>\d+(?:\.\d+)?)\s+i\s+(?P<b>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    ) or re.search(
        r"\bizmeđu\b\s+(?P<a>\d+(?:\.\d+)?)\s+i\s+(?P<b>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_sr_between:
        try:
            value_min = float(m_sr_between.group("a"))
            value_max = float(m_sr_between.group("b"))
            value_min_inclusive = True
            value_max_inclusive = True
        except Exception:
            pass

    # Serbian quotas: "2 iz bugarske i dva iz rumunije"
    country_limits: Optional[Dict[str, int]] = None
    for m in re.finditer(r"\b(?P<n>\d+|[a-zčćšđž]+)\b\s+iz\s+(?P<c>[a-zčćšđž]+)\b", lowered, re.I):
        n_raw = m.group("n").strip().lower()
        c_raw = m.group("c").strip().lower()
        try:
            n = int(n_raw)
        except Exception:
            n = _SR_NUM_WORDS.get(n_raw, 0)
        if n <= 0:
            continue
        c = _SR_COUNTRY_FORMS.get(c_raw, c_raw)
        if not c:
            continue
        if country_limits is None:
            country_limits = {}
        country_limits[c] = country_limits.get(c, 0) + n

    group_names: Optional[List[str]] = None
    if country_limits:
        group_names = list(country_limits.keys())
        distinct_matches = True

    # English quotas: "3 matches from Bulgaria and 3 from Romania"
    for m in re.finditer(
        r"\b(?P<n>\d+)\s+(?:matches?|matchs?)\s+from\s+(?P<c>[A-Za-z]+)\b",
        raw,
        re.I,
    ):
        n = int(m.group("n"))
        c = m.group("c").strip().lower()
        if n <= 0 or not c:
            continue
        if country_limits is None:
            country_limits = {}
        country_limits[c] = country_limits.get(c, 0) + n

    # Follow-up quota segment: "... and 3 from Romania" (avoid matching dates like 25.02.2025 from ...)
    for m in re.finditer(r"\b(?:and|,)\s*(?P<n>\d+)\s+from\s+(?P<c>[A-Za-z]+)\b", raw, re.I):
        n = int(m.group("n"))
        c = m.group("c").strip().lower()
        if n <= 0 or not c:
            continue
        if country_limits is None:
            country_limits = {}
        country_limits[c] = country_limits.get(c, 0) + n

    if country_limits:
        group_names = list(country_limits.keys())
        distinct_matches = True
        # If the prompt gives per-country quotas, interpret total rows as their sum.
        quota_total = sum(country_limits.values())
        if quota_total > 0:
            limit = max(limit, quota_total)

    # tolerate "then" typo
    m_lt = re.search(r"\b(?:value|values)\b.*?\b(?:lower|less|under|below)\b(?:\s+than|\s+then)?\s+(?P<n>\d+(?:\.\d+)?)\b", raw, re.I)
    if not m_lt:
        m_lt = re.search(r"\b(?:lower|less|under|below)\b(?:\s+than|\s+then)?\s+(?P<n>\d+(?:\.\d+)?)\b", raw, re.I)
    if m_lt:
        try:
            value_max = float(m_lt.group("n"))
            value_max_inclusive = False
        except Exception:
            pass

    m_gt = re.search(r"\b(?:value|values)\b.*?\b(?:higher|greater|more|over|above)\b(?:\s+than|\s+then)?\s+(?P<n>\d+(?:\.\d+)?)\b", raw, re.I)
    if not m_gt:
        m_gt = re.search(r"\b(?:higher|greater|more|over|above)\b(?:\s+than|\s+then)?\s+(?P<n>\d+(?:\.\d+)?)\b", raw, re.I)
    if m_gt:
        try:
            value_min = float(m_gt.group("n"))
            value_min_inclusive = False
        except Exception:
            pass

    # If prompt says "total odd", treat the bound as total-product, not per-row value.
    if re.search(r"\btotal\s+odd[s]?\b", lowered):
        if total_product_max is None and value_max is not None:
            total_product_max = value_max
            total_product_max_inclusive = value_max_inclusive
        # Prefer total-product constraint; clear per-row max if it was picked up.
        if value_max is not None:
            value_max = None
            value_max_inclusive = True

    # Fallback: if text is a single word and no other filters, treat it as event name.
    if not any([event_name, group_name, type_, start_time_from, start_time_to]) and re.fullmatch(r"[\w\-]+", raw):
        event_name = raw

    return QuerySpec(
        event_name=event_name,
        group_name=group_name,
        group_names=group_names,
        country_limits=country_limits,
        league=league,
        type_=type_,
        value_min=value_min,
        value_min_inclusive=value_min_inclusive,
        value_max=value_max,
        value_max_inclusive=value_max_inclusive,
        total_product_min=total_product_min,
        total_product_min_inclusive=total_product_min_inclusive,
        total_product_max=total_product_max,
        total_product_max_inclusive=total_product_max_inclusive,
        start_time_from=start_time_from,
        start_time_to=start_time_to,
        submitted_at=submitted_at,
        include_event_name=include_event_name,
        include_type_=include_type_,
        limit=limit,
        distinct_matches=distinct_matches,
        intent=intent,
    )

