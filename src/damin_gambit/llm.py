from __future__ import annotations

import json
import os
import re
from dataclasses import asdict
from typing import Any, Dict, List, Literal, Optional, Tuple

import httpx
from pydantic import BaseModel, Field, ValidationError
from pydantic.config import ConfigDict
from dateutil import parser as date_parser

from .nl import QuerySpec, parse as rule_parse


InterpreterSource = Literal["ollama", "rules"]

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


def _normalize_country(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    x = str(s).strip().lower()
    if not x:
        return None
    return _SR_COUNTRY_FORMS.get(x, x)


class LlmQuerySpec(BaseModel):
    model_config = ConfigDict(extra="ignore")
    # Keep this schema intentionally small and safe:
    # only fields that we can translate into whitelisted SQLAlchemy filters.
    event_name: Optional[str] = None
    country: Optional[str] = None
    countries: Optional[List[str]] = None
    country_limits: Optional[Dict[str, int]] = None
    league: Optional[str] = None
    type: Optional[Literal["won", "draw", "lost"]] = None

    start_time_from: Optional[str] = None
    start_time_to: Optional[str] = None

    value_gt: Optional[float] = None
    value_gte: Optional[float] = None
    value_lt: Optional[float] = None
    value_lte: Optional[float] = None

    total_product_gt: Optional[float] = None
    total_product_gte: Optional[float] = None
    total_product_lt: Optional[float] = None
    total_product_lte: Optional[float] = None

    limit: int = Field(50, ge=1, le=500)
    distinct_matches: bool = False


class LlmRequiredError(RuntimeError):
    pass


def _env_truthy(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() not in {"0", "false", "no", "off"}


def _normalize(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if not s:
        return None
    return s


def _extract_json_object(text: str) -> Optional[dict[str, Any]]:
    objs = _extract_json_objects(text)
    return objs[0] if objs else None


def _extract_json_objects(text: str) -> list[dict[str, Any]]:
    """
    Extract one or more JSON objects from an LLM response.
    Handles cases where the model prints multiple JSON objects back-to-back.
    """
    text = (text or "").strip()
    if not text:
        return []

    # If it's a single dict JSON, return it.
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return [obj]
    except Exception:
        pass

    results: list[dict[str, Any]] = []

    # Scan for balanced {...} regions and try to parse each.
    depth = 0
    start: Optional[int] = None
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start is not None:
                    candidate = text[start : i + 1]
                    start = None
                    try:
                        obj = json.loads(candidate)
                        if isinstance(obj, dict):
                            results.append(obj)
                    except Exception:
                        continue

    return results


def _system_prompt() -> str:
    return (
        "You convert user text (any language, including Serbian) into a JSON object that matches this schema.\n"
        "Return ONLY valid JSON. Do not include explanations.\n\n"
        "Schema (all fields optional unless stated):\n"
        "{\n"
        '  "event_name": string | null,   // match name like "Kasparov vs Carlsen"\n'
        '  "country": string | null,      // single country name\n'
        '  "countries": [string] | null,  // multiple countries\n'
        '  "country_limits": { "country": integer } | null, // per-country quotas, e.g. {"Bulgaria":2,"Norway":2}\n'
        '  "league": string | null,       // tournament/league name\n'
        '  "type": "won" | "draw" | "lost" | null,\n'
        '  "start_time_from": string | null, // datetime lower bound\n'
        '  "start_time_to": string | null,   // datetime upper bound\n'
        '  "value_gt": number | null,\n'
        '  "value_gte": number | null,\n'
        '  "value_lt": number | null,\n'
        '  "value_lte": number | null,\n'
        '  "total_product_gt": number | null,\n'
        '  "total_product_gte": number | null,\n'
        '  "total_product_lt": number | null,\n'
        '  "total_product_lte": number | null,\n'
        '  "limit": integer (1..500),\n'
        '  "distinct_matches": boolean\n'
        "}\n\n"
        "Rules:\n"
        "- If user asks for N matches/games/events (e.g. 'Give me 4 ...'), set limit=N.\n"
        "- If user asks for a range like '4 to 6 matches', set limit=6.\n"
        "- If user says 'matches'/'games'/'mečeva'/'partija', set distinct_matches=true.\n"
        "- If user does NOT specify an outcome type (won/draw/lost), default distinct_matches=true (one row per match).\n"
        "- Map synonyms: 'win'/'won' => type='won', 'loss'/'lose'/'lost' => type='lost', 'draw' => type='draw'.\n"
        "- Serbian range: 'izmedju X i Y' => value_gte=X and value_lte=Y.\n"
        "- Serbian quotas: '2 iz Bugarske i 2 iz Norveske' => country_limits={\"Bulgaria\":2,\"Norway\":2}.\n"
        "- English: 'give me 3 matches with total value between 5 and 9' => total_product_gte=5 and total_product_lte=9.\n"
        "- English: 'give me 4 matches with value between 3 and 5' => value_gte=3 and value_lte=5.\n"
        "- Date/time: 'starts between 12:00 and 17:00 on 25.02.2025' => start_time_from='2025-02-25 12:00', start_time_to='2025-02-25 17:00'.\n"
        "- Date/time: 'starts after 12:00 28.02.2025' => start_time_from='2025-02-28 12:00'.\n"
        "- Map Serbian: 'iz'/'u' + country, 'veće od'/'iznad' => value_gt, 'manje od'/'ispod' => value_lt.\n"
        "- Do not invent fields.\n"
    )


async def interpret_with_ollama(text: str) -> Optional[QuerySpec]:
    host = os.getenv("DAMIN_GAMBIT_OLLAMA_HOST", "http://127.0.0.1:11434")
    model = os.getenv("DAMIN_GAMBIT_OLLAMA_MODEL", "qwen2.5:7b")
    # First request often includes model load; keep a generous default.
    timeout_s = float(os.getenv("DAMIN_GAMBIT_OLLAMA_TIMEOUT_S", "60"))

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _system_prompt()},
            {"role": "user", "content": text},
        ],
        "stream": False,
        "options": {"temperature": 0},
    }

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            r = await client.post(f"{host}/api/chat", json=payload)
            r.raise_for_status()
            data = r.json()
    except Exception:
        return None

    content = (((data or {}).get("message") or {}).get("content")) or ""
    objs = _extract_json_objects(content)
    if not objs:
        return None

    specs: list[LlmQuerySpec] = []
    for obj in objs:
        try:
            specs.append(LlmQuerySpec.model_validate(obj))
        except ValidationError:
            continue
    if not specs:
        return None

    # Merge multiple objects if the model returned them.
    merged = specs[0]
    if len(specs) > 1:
        # Merge quotas/countries
        cl: Dict[str, int] = {}
        countries: list[str] = []
        for s in specs:
            if s.countries:
                countries.extend(s.countries)
            if s.country_limits:
                for k, v in s.country_limits.items():
                    cl[str(k)] = cl.get(str(k), 0) + int(v)
            elif s.country and s.limit:
                # Treat "country + limit" as a quota line.
                cl[str(s.country)] = cl.get(str(s.country), 0) + int(s.limit)

        # Merge numeric constraints conservatively:
        def max_opt(*vals):
            xs = [v for v in vals if v is not None]
            return max(xs) if xs else None

        def min_opt(*vals):
            xs = [v for v in vals if v is not None]
            return min(xs) if xs else None

        merged = merged.model_copy(
            update={
                "country_limits": cl or None,
                "countries": list(dict.fromkeys([c for c in countries if c])) or None,
                "country": None,
                "limit": sum(cl.values()) if cl else max(s.limit for s in specs),
                "type": next((s.type for s in specs if s.type is not None), None),
                "league": next((s.league for s in specs if s.league is not None), None),
                "event_name": next((s.event_name for s in specs if s.event_name is not None), None),
                "distinct_matches": True if any(s.distinct_matches for s in specs) else merged.distinct_matches,
                "start_time_from": next((s.start_time_from for s in specs if s.start_time_from), None),
                "start_time_to": next((s.start_time_to for s in specs if s.start_time_to), None),
                "value_gt": max_opt(*(s.value_gt for s in specs)),
                "value_gte": max_opt(*(s.value_gte for s in specs)),
                "value_lt": min_opt(*(s.value_lt for s in specs)),
                "value_lte": min_opt(*(s.value_lte for s in specs)),
                "total_product_gt": max_opt(*(s.total_product_gt for s in specs)),
                "total_product_gte": max_opt(*(s.total_product_gte for s in specs)),
                "total_product_lt": min_opt(*(s.total_product_lt for s in specs)),
                "total_product_lte": min_opt(*(s.total_product_lte for s in specs)),
            }
        )

    spec = merged

    full_text = text or ""
    m_one_more_clause = re.search(r"\b(?:and\s+)?one\s+more\s+match\b", full_text, re.I)
    base_text = full_text
    extra_text = ""
    if m_one_more_clause:
        base_text = full_text[: m_one_more_clause.start()].strip()
        extra_text = full_text[m_one_more_clause.end() :]

    submitted_at_txt: Optional[datetime] = None
    if re.search(r"\bsubmit\w*\b", full_text, re.I):
        dt_matches = re.findall(r"\b\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\b", full_text)
        if dt_matches:
            try:
                submitted_at_txt = date_parser.parse(dt_matches[-1])
            except Exception:
                submitted_at_txt = None

    # If the user explicitly mentioned an outcome but the model omitted `type`,
    # apply a safe hint from the base query text.
    outcome_hint: Optional[str] = None
    lowered = base_text.lower()
    if re.search(r"\b(win|won)\b", lowered):
        outcome_hint = "won"
    elif re.search(r"\b(draw)\b", lowered):
        outcome_hint = "draw"
    elif re.search(r"\b(loss|lose|lost)\b", lowered):
        outcome_hint = "lost"

    if spec.type is None and outcome_hint in {"won", "draw", "lost"}:
        spec = spec.model_copy(update={"type": outcome_hint})

    # Deterministic extraction for explicit date+time windows in the text.
    start_time_from_txt: Optional[datetime] = None
    start_time_to_txt: Optional[datetime] = None
    m_window = re.search(
        r"\bstarts?\b.*?\bbetween\b\s+(?P<t1>\d{1,2}:\d{2})\s+\band\b\s+(?P<t2>\d{1,2}:\d{2})\s+\bon\b\s+(?P<d>\d{1,2}[./-]\d{1,2}[./-]\d{2,4}|\d{4}-\d{2}-\d{2})\b",
        (text or "").lower(),
        re.I,
    )
    if m_window:
        d = m_window.group("d")
        t1 = m_window.group("t1")
        t2 = m_window.group("t2")
        try:
            start_time_from_txt = date_parser.parse(f"{d} {t1}")
            start_time_to_txt = date_parser.parse(f"{d} {t2}")
        except Exception:
            start_time_from_txt = None
            start_time_to_txt = None

    m_after = re.search(
        r"\bstarts?\b.*?\bafter\b\s+(?P<t>\d{1,2}:\d{2})\s+(?:on\s+)?(?P<d>\d{1,2}[./-]\d{1,2}[./-]\d{2,4}|\d{4}-\d{2}-\d{2})\b",
        (text or "").lower(),
        re.I,
    )
    if m_after and start_time_from_txt is None:
        d = m_after.group("d")
        t = m_after.group("t")
        try:
            start_time_from_txt = date_parser.parse(f"{d} {t}")
        except Exception:
            start_time_from_txt = None

    if submitted_at_txt and re.search(r"\bnisu\s+startov\w*\s+nakon\s+submit\w*\b", full_text, re.I):
        if start_time_to_txt is None or start_time_to_txt > submitted_at_txt:
            start_time_to_txt = submitted_at_txt

    # If the model provided start_time_from/to, parse them.
    if start_time_from_txt is None and spec.start_time_from:
        try:
            start_time_from_txt = date_parser.parse(spec.start_time_from)
        except Exception:
            start_time_from_txt = None
    if start_time_to_txt is None and spec.start_time_to:
        try:
            start_time_to_txt = date_parser.parse(spec.start_time_to)
        except Exception:
            start_time_to_txt = None

    # If user says "total odd/odds", interpret bound as total-product.
    if re.search(r"\btotal\s+odd[s]?\b", (text or "").lower()):
        if spec.total_product_lt is None and spec.total_product_lte is None and (spec.value_lt is not None or spec.value_lte is not None):
            spec = spec.model_copy(
                update={
                    "total_product_lt": spec.value_lt,
                    "total_product_lte": spec.value_lte,
                    "value_lt": None,
                    "value_lte": None,
                }
            )

    # Serbian: "ukupnom/ukupna vrijednoscu/vrednoscu ... vec/vecom od N" -> total-product constraint.
    m_sr_total_gt = re.search(
        r"\bukupn\w*\s+(?:vrijednos\w*|vrednos\w*)\b.*?\b(?:vec\w*|već\w*|iznad|preko)\b(?:\s+od)?\s+(?P<n>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_sr_total_gt:
        try:
            n = float(m_sr_total_gt.group("n"))
            if spec.total_product_gt is None and spec.total_product_gte is None:
                spec = spec.model_copy(update={"total_product_gt": n})
            # Avoid accidental per-row filtering from model hallucination.
            if spec.value_gt is not None or spec.value_gte is not None:
                spec = spec.model_copy(update={"value_gt": None, "value_gte": None})
        except Exception:
            pass

    # Serbian: "ukupnom kvotom/koeficijentom ... ne vec(om) od N" -> total-product <= N
    m_sr_total_lte = re.search(
        r"\bukupn\w*\s+(?:kvot\w*|kovt\w*|koeficijent\w*)\b.*?\bne\s+vec\w*\s+od\s+(?P<n>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_sr_total_lte:
        try:
            n = float(m_sr_total_lte.group("n"))
            if spec.total_product_lte is None and spec.total_product_lt is None:
                spec = spec.model_copy(update={"total_product_lte": n, "total_product_lt": None})
            if spec.value_lte is not None or spec.value_lt is not None:
                spec = spec.model_copy(update={"value_lte": None, "value_lt": None})
        except Exception:
            pass

    # English: "value between X and Y" should be a per-row value range.
    m_value_between = re.search(
        r"\bvalue\b.*?\bbetween\b\s+(?P<a>\d+(?:\.\d+)?)\s+\band\b\s+(?P<b>\d+(?:\.\d+)?)\b",
        lowered,
        re.I,
    )
    if m_value_between and not re.search(r"\btotal\s+(?:value|product)\b", lowered):
        try:
            a = float(m_value_between.group("a"))
            b = float(m_value_between.group("b"))
            lo, hi = (a, b) if a <= b else (b, a)
            spec = spec.model_copy(
                update={
                    "value_gte": lo,
                    "value_lte": hi,
                    "value_gt": None,
                    "value_lt": None,
                }
            )
        except Exception:
            pass

    # Convert to internal QuerySpec (dataclass)
    value_min = spec.value_gt if spec.value_gt is not None else spec.value_gte
    value_min_inclusive = True if value_min is None else (spec.value_gte is not None and spec.value_gt is None)
    value_max = spec.value_lt if spec.value_lt is not None else spec.value_lte
    value_max_inclusive = True if value_max is None else (spec.value_lte is not None and spec.value_lt is None)

    total_product_min = spec.total_product_gt if spec.total_product_gt is not None else spec.total_product_gte
    total_product_min_inclusive = (
        True if total_product_min is None else (spec.total_product_gte is not None and spec.total_product_gt is None)
    )
    total_product_max = spec.total_product_lt if spec.total_product_lt is not None else spec.total_product_lte
    total_product_max_inclusive = (
        True if total_product_max is None else (spec.total_product_lte is not None and spec.total_product_lt is None)
    )

    country_limits = None
    if spec.country_limits:
        country_limits = {
            (_normalize_country(str(k)) or str(k).strip().lower()): int(v)
            for k, v in spec.country_limits.items()
            if int(v) > 0
        }

    group_names = None
    if spec.countries:
        group_names = [_normalize_country(str(c)) for c in spec.countries]
        group_names = [c for c in group_names if c]
    if _normalize(spec.country):
        group_names = (group_names or []) + [_normalize_country(_normalize(spec.country))]  # type: ignore[list-item]
        group_names = [c for c in group_names if c]
    if country_limits:
        group_names = list(country_limits.keys())

    distinct_matches = bool(spec.distinct_matches)
    if spec.type is None or total_product_min is not None or total_product_max is not None:
        distinct_matches = True

    limit = spec.limit
    if country_limits:
        quota_total = sum(country_limits.values())
        if quota_total > 0:
            limit = max(limit, quota_total)

    include_event_name: Optional[str] = None
    include_type_: Optional[str] = None
    if extra_text:
        m_vs = re.search(
            r"\b(?P<a>[A-Za-z][A-Za-z .'-]{1,60}?)\s+vs\s+(?P<b>[A-Za-z][A-Za-z .'-]{1,60}?)\b",
            extra_text,
            re.I,
        )
        if m_vs:
            include_event_name = f"{m_vs.group('a').strip()} vs {m_vs.group('b').strip()}".lower()
            m_t = re.search(r"\b(won|draw|lost|win|lose|loss)\b", extra_text, re.I)
            if m_t:
                t = m_t.group(1).lower()
                include_type_ = {"win": "won", "lose": "lost", "loss": "lost"}.get(t, t)

    event_name_out = _normalize(spec.event_name).lower() if _normalize(spec.event_name) else None
    if include_event_name and event_name_out == include_event_name:
        event_name_out = None

    type_out = spec.type
    if m_one_more_clause and type_out and include_type_ and type_out == include_type_:
        # If the only outcome mention is in the "one more match ..." clause,
        # don't apply it to the base query.
        if not re.search(r"\b(win|won|draw|loss|lose|lost)\b", lowered):
            type_out = None

    return QuerySpec(
        event_name=event_name_out,
        group_name=None,
        group_names=group_names or None,
        country_limits=country_limits,
        league=_normalize(spec.league).lower() if _normalize(spec.league) else None,
        type_=type_out,
        value_min=value_min,
        value_min_inclusive=value_min_inclusive,
        value_max=value_max,
        value_max_inclusive=value_max_inclusive,
        total_product_min=total_product_min,
        total_product_min_inclusive=total_product_min_inclusive,
        total_product_max=total_product_max,
        total_product_max_inclusive=total_product_max_inclusive,
        start_time_from=start_time_from_txt,
        start_time_to=start_time_to_txt,
        submitted_at=submitted_at_txt,
        include_event_name=include_event_name,
        include_type_=include_type_,
        limit=limit,
        distinct_matches=distinct_matches,
        intent="list",
    )


async def interpret(text: str) -> QuerySpec:
    """
    Best-effort: try LLM first (if available), fall back to rules.
    """
    spec, _source = await interpret_with_source(text)
    return spec


async def interpret_with_source(text: str) -> Tuple[QuerySpec, InterpreterSource]:
    """
    Like `interpret`, but also returns which interpreter produced the spec.
    """
    require_llm = _env_truthy("DAMIN_GAMBIT_REQUIRE_LLM", "0")
    use_llm = _env_truthy("DAMIN_GAMBIT_USE_LLM", "1")

    if not use_llm:
        if require_llm:
            raise LlmRequiredError("LLM is required but DAMIN_GAMBIT_USE_LLM is disabled.")
        return rule_parse(text), "rules"

    # LLM path enabled
    if use_llm:
        spec = await interpret_with_ollama(text)
        if spec:
            return spec, "ollama"

    if require_llm:
        raise LlmRequiredError(
            "LLM is required but Ollama did not return a valid structured query. "
            "Ensure Ollama is running and the configured model is available."
        )

    return rule_parse(text), "rules"


def explain_spec(spec: QuerySpec) -> dict[str, Any]:
    # Convenient for API responses.
    return asdict(spec)

