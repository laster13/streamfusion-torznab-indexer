from __future__ import annotations

import asyncio
import hmac
import time
import os
import re
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from email.utils import format_datetime
from typing import Any
from urllib.parse import quote
from xml.etree.ElementTree import Element, SubElement, register_namespace, tostring

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import create_async_engine
from app.alldebrid_global_quota import acquire_alldebrid_global_quota


MEILI_URL = os.getenv("MEILI_URL", "http://sfr-meilisearch-dev:7700").rstrip("/")
MEILI_INDEX = os.getenv("MEILI_INDEX", "torrents").strip() or "torrents"
MEILI_API_KEY = os.getenv("MEILI_API_KEY", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
INDEXER_NAME = os.getenv("INDEXER_NAME", "StreamFusion Meilisearch FR").strip()
API_KEY = os.getenv("INDEXER_API_KEY", "").strip()
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))
MAX_LIMIT = int(os.getenv("MAX_LIMIT", "200"))
MEILI_CANDIDATE_LIMIT = min(1000, max(100, int(os.getenv("MEILI_CANDIDATE_LIMIT", "600"))))
MEILI_TIMEOUT = float(os.getenv("MEILI_TIMEOUT", "10"))
PG_SEEDERS_REQUIRED = os.getenv("PG_SEEDERS_REQUIRED", "false").strip().lower() in {
    "1", "true", "yes", "on"
}
ACCEPT_MULTI_WITHOUT_LANG = os.getenv("ACCEPT_MULTI_WITHOUT_LANG", "true").strip().lower() in {
    "1", "true", "yes", "on"
}


ALLDEBRID_API_KEY = os.getenv(
    "ALLDEBRID_API_KEY",
    "",
).strip()


ALLDEBRID_BROKER_URL = os.getenv(
    "ALLDEBRID_BROKER_URL",
    "http://sf-alldebrid-broker:8080",
).strip().rstrip("/")

ALLDEBRID_BROKER_TOKEN = os.getenv(
    "ALLDEBRID_BROKER_TOKEN",
    "",
).strip()

ALLDEBRID_BROKER_TIMEOUT = max(
    5.0,
    float(
        os.getenv(
            "ALLDEBRID_BROKER_TIMEOUT",
            "45",
        )
    ),
)

ALLDEBRID_CACHE_ONLY = os.getenv(
    "ALLDEBRID_CACHE_ONLY",
    "false",
).strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

ALLDEBRID_CHECK_LIMIT = int(
    os.getenv(
        "ALLDEBRID_CHECK_LIMIT",
        "30",
    )
)

ALLDEBRID_BATCH_SIZE = int(
    os.getenv(
        "ALLDEBRID_BATCH_SIZE",
        "10",
    )
)

ALLDEBRID_MAX_CHECK = int(
    os.getenv(
        "ALLDEBRID_MAX_CHECK",
        "30",
    )
)

# Garde-fous identiques au backend PostgreSQL.
ALLDEBRID_SAFE_UPLOAD_BATCH = 10
ALLDEBRID_SAFE_MAX_CHECK = 30
ALLDEBRID_CAPACITY_GUARD = 950
ALLDEBRID_BACKOFF_SECONDS = 60

ALLDEBRID_BLOCKED_UNTIL = 0.0
ALLDEBRID_LOCK = asyncio.Lock()

# Marge sous la limite API AllDebrid.
ALLDEBRID_RATE_LOCK = asyncio.Lock()
ALLDEBRID_LAST_REQUEST_AT = 0.0
ALLDEBRID_MIN_REQUEST_INTERVAL = 0.25


if len(API_KEY) < 32:
    raise RuntimeError("INDEXER_API_KEY is required and must contain at least 32 characters")
if not MEILI_API_KEY:
    raise RuntimeError("MEILI_API_KEY is required")
if DEFAULT_LIMIT < 1 or MAX_LIMIT < 1 or DEFAULT_LIMIT > MAX_LIMIT:
    raise RuntimeError("Invalid DEFAULT_LIMIT/MAX_LIMIT")

engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
) if DATABASE_URL else None

app = FastAPI(title="StreamFusion Meilisearch Torznab Indexer", version="1.0.0")
TORZNAB_NS = "http://torznab.com/schemas/2015/feed"
register_namespace("torznab", TORZNAB_NS)

FR_MARKERS = re.compile(
    r"(?ix)(?:^|[^A-Z0-9])(?:"
    r"TRUE[ ._-]*FRENCH|FRENCH|VOSTFR|SUB[ ._-]*FRENCH|"
    r"VFF|VFQ|VFI|VF2|QCF|FRENCH[ ._-]*CANADIAN|CANADIAN[ ._-]*FRENCH"
    r")(?:$|[^A-Z0-9])"
)
MULTI_MARKER = re.compile(r"(?i)(?:^|[^A-Z0-9])MULTI(?:$|[^A-Z0-9])")
NON_FR_MARKERS = re.compile(
    r"(?ix)(?:^|[^A-Z0-9])(?:"
    r"VO|VOSTA|ENGLISH|ENG|SUB[ ._-]*ENG"
    r")(?:$|[^A-Z0-9])"
)


def xml_response(root: Element) -> Response:
    data = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="utf-8")
    return Response(data, media_type="application/xml; charset=utf-8")


def check_key(request: Request) -> None:
    supplied = request.query_params.get("apikey", "")
    if not hmac.compare_digest(supplied, API_KEY):
        raise HTTPException(status_code=401, detail="Invalid API key")


def caps() -> Response:
    root = Element("caps")
    SubElement(root, "server", version="1.0", title=INDEXER_NAME)
    SubElement(root, "limits", max=str(MAX_LIMIT), default=str(DEFAULT_LIMIT))
    searching = SubElement(root, "searching")
    SubElement(searching, "search", available="yes", supportedParams="q,cat")
    SubElement(searching, "movie-search", available="yes", supportedParams="q,imdbid,tmdbid,year,cat")
    SubElement(searching, "tv-search", available="yes", supportedParams="q,season,ep,imdbid,tmdbid,cat")
    cats = SubElement(root, "categories")
    movie = SubElement(cats, "category", id="2000", name="Movies")
    for cid, name in (("2030", "Movies/SD"), ("2040", "Movies/HD"), ("2045", "Movies/UHD")):
        SubElement(movie, "subcat", id=cid, name=name)
    tv = SubElement(cats, "category", id="5000", name="TV")
    for cid, name in (("5030", "TV/SD"), ("5040", "TV/HD"), ("5045", "TV/UHD"), ("5070", "TV/Anime")):
        SubElement(tv, "subcat", id=cid, name=name)
    return xml_response(root)


def normalize_imdb(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return "tt" + value
    return value.lower()


def normalize_title(value: Any) -> str:
    text_value = str(value or "")
    text_value = unicodedata.normalize("NFKD", text_value)
    text_value = "".join(ch for ch in text_value if not unicodedata.combining(ch))
    text_value = text_value.lower().replace("&", " and ")
    text_value = re.sub(r"[^a-z0-9]+", " ", text_value)
    return re.sub(r"\s+", " ", text_value).strip()


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, (tuple, set)):
        return list(value)
    return [value]


def is_french(row: dict[str, Any]) -> bool:
    title = str(row.get("raw_title") or "")

    # Explicit French/MULTI markers win. This includes Quebec variants.
    if FR_MARKERS.search(title):
        return True
    if ACCEPT_MULTI_WITHOUT_LANG and MULTI_MARKER.search(title):
        return True

    # The parsed language is useful, but some sources may default a language.
    # Do not trust fr when the release explicitly advertises VO/English only.
    languages = {
        str(x).strip().lower()
        for x in as_list(row.get("languages"))
        if str(x).strip()
    }
    has_french_language = bool(
        {"fr", "fra", "fre", "french"} & languages
    )
    if not has_french_language:
        return False
    if NON_FR_MARKERS.search(title):
        return False
    return True


def title_score(row: dict[str, Any], query: str | None) -> int:
    if not query or not query.strip():
        return 50
    qn = normalize_title(query)
    if not qn:
        return 50
    candidates = [
        normalize_title(row.get("normalized_title")),
        normalize_title(row.get("parsed_title")),
        normalize_title(row.get("raw_title")),
    ]
    if qn in candidates[:2]:
        return 100
    q_tokens = [t for t in qn.split() if t]
    if not q_tokens:
        return 50
    for candidate in candidates:
        if not candidate:
            continue
        if qn in candidate:
            return 95
        c_tokens = set(candidate.split())
        if all(token in c_tokens for token in q_tokens):
            return 90
    return 0


def matches_year(row: dict[str, Any], year: int | None) -> bool:
    if year is None:
        return True
    stored = row.get("year")
    if stored not in (None, ""):
        try:
            if int(stored) == year:
                return True
        except (TypeError, ValueError):
            pass
    return re.search(rf"(?<!\d){year}(?!\d)", str(row.get("raw_title") or "")) is not None


def matches_season_episode(row: dict[str, Any], season: int | None, episode: int | None) -> bool:
    title = str(row.get("raw_title") or "")
    seasons = set()
    episodes = set()
    for value in as_list(row.get("seasons")):
        try:
            seasons.add(int(value))
        except (TypeError, ValueError):
            pass
    for value in as_list(row.get("episodes")):
        try:
            episodes.add(int(value))
        except (TypeError, ValueError):
            pass
    if season is not None and season not in seasons:
        if not re.search(rf"(?i)(?:^|[^A-Z0-9])S0*{season}(?:[^0-9]|$)", title):
            return False
    if episode is not None and episode not in episodes:
        if not re.search(rf"(?i)(?:^|[^A-Z0-9])E0*{episode}(?:[^0-9]|$)", title):
            return False
    return True


def valid_hash(value: Any) -> str | None:
    h = str(value or "").strip().lower()
    if len(h) == 40 and all(c in "0123456789abcdef" for c in h):
        return h
    return None


def magnet(row: dict[str, Any]) -> str:
    h = valid_hash(row.get("hash") or row.get("info_hash"))
    if not h:
        return ""
    return f"magnet:?xt=urn:btih:{h}&dn={quote(str(row.get('raw_title') or ''))}"


def category(row: dict[str, Any]) -> str:
    typ = str(row.get("type") or "").lower()
    res = str(row.get("resolution") or "").lower()
    if typ == "series":
        if "2160" in res or "4k" in res:
            return "5045"
        if "1080" in res or "720" in res:
            return "5040"
        return "5000"
    if "2160" in res or "4k" in res:
        return "2045"
    if "1080" in res or "720" in res:
        return "2040"
    return "2000"


def pubdate(value: Any) -> str:
    try:
        dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)
    return format_datetime(dt)


def meili_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def type_filter(mode: str) -> str | None:
    if mode == "movie":
        return 'type = "movie"'
    if mode == "tvsearch":
        return 'type = "series"'
    return None


def parse_categories(value: str | None) -> set[int]:
    categories: set[int] = set()

    if not value:
        return categories

    for part in str(value).split(","):
        part = part.strip()

        if not part:
            continue

        try:
            categories.add(int(part))
        except ValueError:
            continue

    return categories


def mode_from_categories(mode: str, cat: str | None) -> str:
    """
    Prowlarr utilise souvent t=search + cat plutôt que t=movie/tvsearch.

    2xxx = Movies
    5xxx = TV
    """
    if mode != "search":
        return mode

    categories = parse_categories(cat)

    has_movie = any(
        2000 <= value < 3000
        for value in categories
    )

    has_tv = any(
        5000 <= value < 6000
        for value in categories
    )

    if has_movie and not has_tv:
        return "movie"

    if has_tv and not has_movie:
        return "tvsearch"

    return mode


def category_allowed(
    row: dict[str, Any],
    cat: str | None,
) -> bool:
    requested = parse_categories(cat)

    if not requested:
        return True

    try:
        actual = int(category(row))
    except (TypeError, ValueError):
        return False

    # Parent Movies : accepte tous les 2xxx.
    if 2000 <= actual < 3000:
        if 2000 in requested:
            return True

        return actual in requested

    # Parent TV : accepte tous les 5xxx.
    if 5000 <= actual < 6000:
        if 5000 in requested:
            return True

        return actual in requested

    return actual in requested


def id_filter(imdbid: str | None, tmdbid: int | None) -> str | None:
    bits: list[str] = []
    if tmdbid is not None:
        bits.append(f'(tmdb_id = {int(tmdbid)} OR tmdb_id = "{int(tmdbid)}")')
    normalized_imdb = normalize_imdb(imdbid)
    if normalized_imdb:
        safe = meili_escape(normalized_imdb)
        bits.append(f'imdb_id = "{safe}"')
    if not bits:
        return None
    return "(" + " OR ".join(bits) + ")"


def combine_filters(*filters: str | None) -> str | None:
    kept = [f"({f})" for f in filters if f]
    return " AND ".join(kept) if kept else None


ATTRIBUTES = [
    "hash", "type", "uuid", "raw_title", "parsed_title", "normalized_title",
    "added", "size", "year", "resolution", "seasons", "episodes", "languages",
    "quality", "codec", "audio", "hdr", "indexer", "imdb_id", "tmdb_id",
]


async def meili_search(
    *,
    q: str,
    filter_expr: str | None,
    limit: int,
    offset: int = 0,
    matching_strategy: str = "frequency",
    sort: list[str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "q": q,
        "limit": min(limit, 1000),
        "offset": max(offset, 0),
        "distinct": "hash",
        "matchingStrategy": matching_strategy,
        "attributesToRetrieve": ATTRIBUTES,
    }
    if filter_expr:
        payload["filter"] = filter_expr
    if sort:
        payload["sort"] = sort

    async with httpx.AsyncClient(timeout=httpx.Timeout(MEILI_TIMEOUT)) as client:
        response = await client.post(
            f"{MEILI_URL}/indexes/{MEILI_INDEX}/search",
            headers={"Authorization": f"Bearer {MEILI_API_KEY}"},
            json=payload,
        )
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Meilisearch error HTTP {response.status_code}")
    body = response.json()
    if not isinstance(body, dict) or not isinstance(body.get("hits", []), list):
        raise HTTPException(status_code=502, detail="Invalid Meilisearch response")
    return body


async def collect_text_candidates(mode: str, q: str, limit: int) -> list[dict[str, Any]]:
    base = type_filter(mode)
    french = combine_filters(base, 'languages = "fr"')
    candidates: dict[str, dict[str, Any]] = {}

    queries = [f'"{q}"', q]
    strategies = ["last", "frequency"]

    for query, strategy in zip(queries, strategies):
        for filter_expr, require_marker in ((french, False), (base, True)):
            body = await meili_search(
                q=query,
                filter_expr=filter_expr,
                limit=limit,
                matching_strategy=strategy,
            )
            for position, row in enumerate(body.get("hits", [])):
                h = valid_hash(row.get("hash"))
                if not h:
                    continue
                if require_marker and not is_french(row):
                    continue
                if not is_french(row):
                    continue
                score = title_score(row, q)
                if score <= 0:
                    continue
                candidate = dict(row)
                candidate["_match_rank"] = max(int(candidate.get("_match_rank") or 0), score)
                candidate["_meili_position"] = min(int(candidate.get("_meili_position") or 10**9), position)
                old = candidates.get(h)
                if old is None or int(candidate["_match_rank"]) > int(old.get("_match_rank") or 0):
                    candidates[h] = candidate

    return list(candidates.values())


async def collect_id_candidates(mode: str, imdbid: str | None, tmdbid: int | None, limit: int) -> list[dict[str, Any]]:
    ids = id_filter(imdbid, tmdbid)
    if not ids:
        return []
    base = combine_filters(type_filter(mode), ids)
    french = combine_filters(base, 'languages = "fr"')
    candidates: dict[str, dict[str, Any]] = {}

    for filter_expr, require_marker in ((french, False), (base, True)):
        body = await meili_search(q="", filter_expr=filter_expr, limit=limit, matching_strategy="last")
        for position, row in enumerate(body.get("hits", [])):
            h = valid_hash(row.get("hash"))
            if not h:
                continue
            if require_marker and not is_french(row):
                continue
            if not is_french(row):
                continue
            candidate = dict(row)
            candidate["_match_rank"] = 120
            candidate["_meili_position"] = position
            candidates[h] = candidate
    return list(candidates.values())


def resolved_title(rows: list[dict[str, Any]]) -> str | None:
    values: list[str] = []
    for row in rows:
        value = str(row.get("normalized_title") or row.get("parsed_title") or "").strip()
        if value:
            values.append(value)
    if not values:
        return None
    return Counter(values).most_common(1)[0][0]


async def resolve_title_from_postgres(
    mode: str,
    imdbid: str | None,
    tmdbid: int | None,
) -> str | None:
    if engine is None:
        return None

    conditions: list[str] = []
    params: dict[str, Any] = {}

    if tmdbid is not None:
        conditions.append("tmdb_id = :tmdbid")
        params["tmdbid"] = tmdbid

    normalized_imdb = normalize_imdb(imdbid)
    if normalized_imdb:
        conditions.append("imdb_id = :imdbid")
        params["imdbid"] = normalized_imdb

    if not conditions:
        return None

    type_clause = ""
    if mode == "movie":
        type_clause = "AND type = 'movie'"
    elif mode == "tvsearch":
        type_clause = "AND type = 'series'"

    stmt = text(
        f"""
        SELECT
            COALESCE(
                NULLIF(parsed_data::jsonb->>'normalized_title', ''),
                NULLIF(parsed_data::jsonb->>'parsed_title', '')
            ) AS resolved_title,
            COUNT(*) AS occurrences,
            MAX(COALESCE(seeders, 0)) AS max_seeders
        FROM torrent_items
        WHERE ({' OR '.join(conditions)})
          {type_clause}
          AND COALESCE(
                NULLIF(parsed_data::jsonb->>'normalized_title', ''),
                NULLIF(parsed_data::jsonb->>'parsed_title', '')
              ) IS NOT NULL
        GROUP BY resolved_title
        ORDER BY occurrences DESC, max_seeders DESC
        LIMIT 1
        """
    )

    try:
        async with engine.connect() as conn:
            result = await conn.execute(stmt, params)
            value = result.scalar_one_or_none()
    except Exception:
        return None

    return str(value).strip() if value else None




async def alldebrid_rate_wait() -> None:
    await acquire_alldebrid_global_quota()


async def alldebrid_status_snapshot(
    client: httpx.AsyncClient,
) -> list[dict[str, Any]]:
    started = time.perf_counter()

    await alldebrid_rate_wait()

    response = await client.post(
        "https://api.alldebrid.com/v4.1/magnet/status",
        headers={
            "Authorization":
                f"Bearer {ALLDEBRID_API_KEY}"
        },
    )

    response.raise_for_status()

    body = response.json()

    if body.get("status") != "success":
        raise RuntimeError(
            "Réponse magnet/status invalide"
        )

    magnets = (
        body
        .get("data", {})
        .get("magnets", [])
        or []
    )

    if not isinstance(magnets, list):
        raise RuntimeError(
            "magnet/status: magnets "
            "n'est pas une liste"
        )

    print(
        "[ALLDEBRID][TIMING] "
        f"status="
        f"{time.perf_counter() - started:.3f}s "
        f"magnets={len(magnets)}"
    )

    return magnets


async def alldebrid_delete_magnet(
    client: httpx.AsyncClient,
    magnet_id: int,
) -> None:
    await alldebrid_rate_wait()

    response = await client.post(
        "https://api.alldebrid.com/v4/magnet/delete",
        headers={
            "Authorization":
                f"Bearer {ALLDEBRID_API_KEY}"
        },
        data={
            "id": str(magnet_id)
        },
    )

    response.raise_for_status()

    body = response.json()

    if body.get("status") != "success":
        error = body.get("error") or {}
        if error.get("code") == "MAGNET_INVALID_ID":
            print(
                "[ALLDEBRID][DELETE-IDEMPOTENT] "
                f"id={magnet_id} already_absent"
            )
            return

        raise RuntimeError(
            f"Echec suppression magnet "
            f"{magnet_id}"
        )



async def alldebrid_cached_hashes(
    rows: list[dict[str, Any]],
) -> tuple[set[str], set[str]]:
    """
    Chemin principal : broker partagé.
    Repli : ancienne logique locale intacte.
    """

    if not rows:
        return set(), set()

    hard_limit = max(
        1,
        min(
            ALLDEBRID_CHECK_LIMIT,
            ALLDEBRID_MAX_CHECK,
            ALLDEBRID_SAFE_MAX_CHECK,
        ),
    )

    hashes: list[str] = []
    seen: set[str] = set()

    for row in rows:
        h = valid_hash(
            row.get("hash")
        )

        if not h:
            continue

        if h in seen:
            continue

        seen.add(h)
        hashes.append(h)

        if len(hashes) >= hard_limit:
            break

    if not hashes:
        return set(), set()

    if (
        not ALLDEBRID_BROKER_URL
        or not ALLDEBRID_BROKER_TOKEN
    ):
        print(
            "[ALLDEBRID]"
            "[BROKER-DISABLED] "
            "fallback local"
        )

        return (
            await
            alldebrid_cached_hashes_local(
                rows
            )
        )

    wanted = set(hashes)

    try:
        started = time.perf_counter()

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(
                ALLDEBRID_BROKER_TIMEOUT
            )
        ) as client:

            response = await client.post(
                ALLDEBRID_BROKER_URL
                + "/v1/check",
                headers={
                    "Authorization":
                        "Bearer "
                        + ALLDEBRID_BROKER_TOKEN
                },
                json={
                    "hashes": hashes,
                },
            )

        if response.status_code != 200:
            raise RuntimeError(
                "broker HTTP "
                f"{response.status_code}"
            )

        body = response.json()

        cached = {
            h
            for value in (
                body.get("cached")
                or []
            )
            if (
                h := valid_hash(value)
            )
        }

        uncached = {
            h
            for value in (
                body.get("uncached")
                or []
            )
            if (
                h := valid_hash(value)
            )
        }

        if cached & uncached:
            raise RuntimeError(
                "classification broker "
                "contradictoire"
            )

        classified = (
            cached
            | uncached
        )

        if classified != wanted:
            raise RuntimeError(
                "classification broker "
                "incomplète "
                f"{len(classified)}/"
                f"{len(wanted)}"
            )

        elapsed = (
            time.perf_counter()
            - started
        )

        print(
            "[ALLDEBRID][BROKER] "
            f"requested={len(wanted)} "
            f"cached={len(cached)} "
            f"uncached={len(uncached)} "
            f"time={elapsed:.3f}s"
        )

        return cached, uncached

    except Exception as exc:
        print(
            "[ALLDEBRID]"
            "[BROKER-FALLBACK] "
            f"{type(exc).__name__}: "
            f"{exc}"
        )

        return (
            await
            alldebrid_cached_hashes_local(
                rows
            )
        )


async def alldebrid_cached_hashes_local(
    rows: list[dict[str, Any]],
) -> tuple[set[str], set[str]]:
    global ALLDEBRID_BLOCKED_UNTIL

    if not rows:
        return set(), set()

    if not ALLDEBRID_API_KEY:
        raise HTTPException(
            status_code=503,
            detail=(
                "ALLDEBRID_CACHE_ONLY=true "
                "mais ALLDEBRID_API_KEY est vide"
            ),
        )

    hard_limit = max(
        1,
        min(
            ALLDEBRID_CHECK_LIMIT,
            ALLDEBRID_MAX_CHECK,
            ALLDEBRID_SAFE_MAX_CHECK,
        ),
    )

    hashes: list[str] = []
    seen: set[str] = set()

    for row in rows:
        h = valid_hash(
            row.get("hash")
        )

        if not h:
            continue

        if h in seen:
            continue

        seen.add(h)
        hashes.append(h)

        if len(hashes) >= hard_limit:
            break

    if not hashes:
        return set(), set()

    wanted = set(hashes)

    cached: set[str] = set()
    uncached: set[str] = set()

    async with ALLDEBRID_LOCK:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(20.0)
        ) as client:

            # Snapshot AVANT tout upload.
            try:
                snapshot = (
                    await alldebrid_status_snapshot(
                        client
                    )
                )
            except Exception as exc:
                print(
                    "[ALLDEBRID][FAIL-CLOSED] "
                    "status impossible: "
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )

                return set(), set()

            existing_ids: set[int] = set()

            for item in snapshot:
                magnet_id = item.get("id")

                if magnet_id is not None:
                    try:
                        existing_ids.add(
                            int(magnet_id)
                        )
                    except (
                        TypeError,
                        ValueError,
                    ):
                        pass

                h = valid_hash(
                    item.get("hash")
                )

                if (
                    not h
                    or h not in wanted
                ):
                    continue

                if item.get("statusCode") == 4:
                    cached.add(h)
                else:
                    uncached.add(h)

            unknown = [
                h
                for h in hashes
                if (
                    h not in cached
                    and h not in uncached
                )
            ]

            if not unknown:
                print(
                    "[ALLDEBRID][STATUS-HIT] "
                    f"cached={len(cached)} "
                    f"uncached={len(uncached)}"
                )

                return cached, uncached

            now = time.monotonic()

            if now < ALLDEBRID_BLOCKED_UNTIL:
                print(
                    "[ALLDEBRID][BACKOFF] "
                    f"cached_known="
                    f"{len(cached)} "
                    f"unknown={len(unknown)}"
                )

                return cached, uncached

            if (
                len(existing_ids)
                >= ALLDEBRID_CAPACITY_GUARD
            ):
                ALLDEBRID_BLOCKED_UNTIL = (
                    now
                    + ALLDEBRID_BACKOFF_SECONDS
                )

                print(
                    "[ALLDEBRID]"
                    "[CAPACITY-GUARD] "
                    f"existing="
                    f"{len(existing_ids)} "
                    f"threshold="
                    f"{ALLDEBRID_CAPACITY_GUARD} "
                    "aucun upload effectué"
                )

                return cached, uncached

            batch_size = max(
                1,
                min(
                    ALLDEBRID_BATCH_SIZE,
                    ALLDEBRID_SAFE_UPLOAD_BATCH,
                ),
            )

            for pos in range(
                0,
                len(unknown),
                batch_size,
            ):
                batch = unknown[
                    pos:pos + batch_size
                ]

                created_ids: list[int] = []

                upload_elapsed = 0.0
                cleanup_elapsed = 0.0

                try:
                    upload_started = (
                        time.perf_counter()
                    )

                    await alldebrid_rate_wait()

                    response = await client.post(
                        "https://api.alldebrid.com"
                        "/v4/magnet/upload",
                        headers={
                            "Authorization":
                                "Bearer "
                                f"{ALLDEBRID_API_KEY}"
                        },
                        data={
                            "magnets[]": batch
                        },
                    )

                    response.raise_for_status()

                    body = response.json()

                    if (
                        body.get("status")
                        != "success"
                    ):
                        error = (
                            body.get(
                                "error",
                                {},
                            )
                            or {}
                        )

                        code = str(
                            error.get("code")
                            or ""
                        )

                        if code in {
                            "MAGNET_TOO_MANY",
                            "MAGNET_TOO_MANY_ACTIVE",
                        }:
                            ALLDEBRID_BLOCKED_UNTIL = (
                                time.monotonic()
                                + ALLDEBRID_BACKOFF_SECONDS
                            )

                            print(
                                "[ALLDEBRID]"
                                "[LIMIT] "
                                f"{code}; "
                                "backoff activé"
                            )

                            return (
                                cached,
                                uncached,
                            )

                        raise RuntimeError(
                            "Réponse upload "
                            "invalide"
                        )

                    magnets = (
                        body
                        .get("data", {})
                        .get("magnets", [])
                        or []
                    )

                    upload_elapsed = (
                        time.perf_counter()
                        - upload_started
                    )

                    for item in magnets:
                        h = valid_hash(
                            item.get("hash")
                        )

                        magnet_id = (
                            item.get("id")
                        )

                        if (
                            magnet_id
                            is not None
                        ):
                            try:
                                numeric_id = int(
                                    magnet_id
                                )

                                if (
                                    numeric_id
                                    not in existing_ids
                                ):
                                    created_ids.append(
                                        numeric_id
                                    )
                            except (
                                TypeError,
                                ValueError,
                            ):
                                pass

                        if not h:
                            continue

                        if (
                            item.get("ready")
                            is True
                        ):
                            cached.add(h)

                        elif (
                            item.get("ready")
                            is False
                        ):
                            uncached.add(h)

                except HTTPException:
                    raise

                except Exception as exc:
                    print(
                        "[ALLDEBRID]"
                        "[FAIL-CLOSED] "
                        "upload: "
                        f"{type(exc).__name__}: "
                        f"{exc}"
                    )

                    return (
                        cached,
                        uncached,
                    )

                finally:
                    async def cleanup_one(
                        magnet_id: int,
                    ) -> str | None:
                        try:
                            await (
                                alldebrid_delete_magnet(
                                    client,
                                    magnet_id,
                                )
                            )

                            return None

                        except Exception as exc:
                            return (
                                f"{magnet_id}: "
                                f"{type(exc).__name__}: "
                                f"{exc}"
                            )

                    cleanup_started = (
                        time.perf_counter()
                    )

                    cleanup_results = (
                        await asyncio.gather(
                            *[
                                cleanup_one(
                                    magnet_id
                                )
                                for magnet_id
                                in created_ids
                            ]
                        )
                    )

                    cleanup_elapsed = (
                        time.perf_counter()
                        - cleanup_started
                    )

                    cleanup_errors = [
                        error
                        for error
                        in cleanup_results
                        if error is not None
                    ]

                    if cleanup_errors:
                        ALLDEBRID_BLOCKED_UNTIL = (
                            time.monotonic()
                            + ALLDEBRID_BACKOFF_SECONDS
                        )

                        print(
                            "[ALLDEBRID]"
                            "[CLEANUP-ERROR] "
                            + " | ".join(
                                cleanup_errors
                            )
                        )

                        raise HTTPException(
                            status_code=502,
                            detail=(
                                "Nettoyage AllDebrid "
                                "incomplet. Nouveaux "
                                "contrôles temporairement "
                                "bloqués."
                            ),
                        )

                print(
                    "[ALLDEBRID][BATCH] "
                    f"checked={len(batch)} "
                    f"cached={len(cached)} "
                    f"cleanup="
                    f"{len(created_ids)} "
                    f"upload_time="
                    f"{upload_elapsed:.3f}s "
                    f"cleanup_time="
                    f"{cleanup_elapsed:.3f}s"
                )

    return cached, uncached


async def filter_alldebrid_results(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:

    if not ALLDEBRID_CACHE_ONLY:
        return rows

    cached, _uncached = (
        await alldebrid_cached_hashes(
            rows
        )
    )

    return [
        row
        for row in rows
        if valid_hash(
            row.get("hash")
        ) in cached
    ]


async def enrich_seeders(rows: list[dict[str, Any]]) -> None:
    hashes = [valid_hash(row.get("hash")) for row in rows]
    hashes = sorted({h for h in hashes if h})
    if not hashes:
        return
    if engine is None:
        if PG_SEEDERS_REQUIRED:
            raise HTTPException(status_code=503, detail="PostgreSQL seeders enrichment unavailable")
        for row in rows:
            row["seeders"] = 0
        return

    stmt = text(
        """
        SELECT info_hash,
               MAX(COALESCE(seeders, 0))::bigint AS seeders
        FROM torrent_items
        WHERE info_hash IN :hashes
        GROUP BY info_hash
        """
    ).bindparams(bindparam("hashes", expanding=True))

    try:
        async with engine.connect() as conn:
            result = await conn.execute(stmt, {"hashes": hashes})
            mapping = {str(r.info_hash): int(r.seeders or 0) for r in result}
    except Exception as exc:
        if PG_SEEDERS_REQUIRED:
            raise HTTPException(status_code=503, detail="PostgreSQL seeders enrichment failed") from exc
        mapping = {}

    for row in rows:
        h = valid_hash(row.get("hash"))
        row["seeders"] = mapping.get(h or "", 0)


def feed(rows: list[dict[str, Any]], total: int, offset: int) -> Response:
    rss = Element("rss", version="2.0")
    ch = SubElement(rss, "channel")
    SubElement(ch, "title").text = INDEXER_NAME
    SubElement(ch, "description").text = "StreamFusion Meilisearch FR Torznab indexer"
    SubElement(ch, f"{{{TORZNAB_NS}}}response", offset=str(offset), total=str(total))

    for row in rows:
        h = valid_hash(row.get("hash"))
        if not h:
            continue
        title = str(row.get("raw_title") or "")
        m = magnet(row)
        size = int(row.get("size") or 0)
        seeders = int(row.get("seeders") or 0)
        cat = category(row)

        item = SubElement(ch, "item")
        SubElement(item, "title").text = title
        SubElement(item, "guid", isPermaLink="false").text = h
        SubElement(item, "link").text = m
        SubElement(item, "pubDate").text = pubdate(row.get("added"))
        SubElement(item, "category").text = cat
        SubElement(item, "enclosure", url=m, length=str(size), type="application/x-bittorrent")

        attrs = {
            "category": cat,
            "size": str(size),
            "seeders": str(seeders),
            "peers": str(seeders),
            "infohash": h,
            "downloadvolumefactor": "0",
            "uploadvolumefactor": "1",
        }
        imdb = row.get("imdb_id")
        tmdb = row.get("tmdb_id")
        source = row.get("indexer")
        if imdb not in (None, ""):
            attrs["imdb"] = str(imdb).removeprefix("tt")
        if tmdb not in (None, ""):
            attrs["tmdbid"] = str(tmdb)
        if source not in (None, ""):
            attrs["source"] = str(source)
        for key, value in attrs.items():
            SubElement(item, f"{{{TORZNAB_NS}}}attr", name=key, value=value)

    return xml_response(rss)


@app.get("/")
async def root() -> dict[str, Any]:
    return {"name": INDEXER_NAME, "status": "ok", "backend": "meilisearch", "torznab": "/torznab/api"}


@app.get("/health")
async def health() -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(MEILI_TIMEOUT)) as client:
        response = await client.get(
            f"{MEILI_URL}/health",
            headers={"Authorization": f"Bearer {MEILI_API_KEY}"},
        )
    if response.status_code != 200:
        raise HTTPException(status_code=503, detail="Meilisearch unavailable")
    result: dict[str, Any] = {"status": "ok", "meilisearch": "ok", "postgres_seeders": "disabled" if engine is None else "configured"}
    return result



def candidate_release_year(
    row: dict[str, Any],
) -> int | None:
    """
    Retourne l'année la plus fiable disponible
    pour un candidat Meili.

    Priorité :
    1. champ top-level `year`
    2. dernière année plausible trouvée
       dans raw_title
    """
    current_year = time.gmtime().tm_year
    maximum_year = current_year + 1

    value = row.get("year")

    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = None

    if (
        parsed is not None
        and 1900 <= parsed <= maximum_year
    ):
        return parsed

    title = str(
        row.get("raw_title")
        or ""
    )

    years = []

    for match in re.finditer(
        r"(?<!\d)(19\d{2}|20\d{2})(?!\d)",
        title,
    ):
        candidate = int(match.group(1))

        if 1900 <= candidate <= maximum_year:
            years.append(candidate)

    if not years:
        return None

    return years[-1]


def infer_consensus_movie_year(
    rows,
    query: str,
    cat: str | None,
) -> tuple[int | None, int, int]:
    """
    N'infère une année que si le consensus
    entre candidats est suffisamment fort.

    Protections :
    - minimum 5 candidats datés
    - au moins 80 % pour l'année dominante
    - année dominante >= 4x la seconde
    """
    years: list[int] = []

    for row in rows:
        if not is_french(row):
            continue

        if not category_allowed(row, cat):
            continue

        if (
            int(
                row.get("_match_rank")
                or 0
            ) < 90
            and title_score(
                row,
                query,
            ) <= 0
        ):
            continue

        candidate_year = (
            candidate_release_year(row)
        )

        if candidate_year is not None:
            years.append(candidate_year)

    if len(years) < 5:
        return None, 0, len(years)

    counts = Counter(years)
    ranked = counts.most_common()

    best_year, best_count = ranked[0]

    second_count = (
        ranked[1][1]
        if len(ranked) > 1
        else 0
    )

    share = best_count / len(years)

    if best_count < 5:
        return None, best_count, len(years)

    if share < 0.80:
        return None, best_count, len(years)

    if (
        second_count > 0
        and best_count < second_count * 4
    ):
        return None, best_count, len(years)

    return (
        best_year,
        best_count,
        len(years),
    )


@app.get("/torznab/api")
async def torznab(
    request: Request,
    t: str = Query("search"),
    q: str | None = None,
    imdbid: str | None = None,
    tmdbid: int | None = None,
    season: int | None = None,
    ep: int | None = None,
    year: int | None = None,
    cat: str | None = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(DEFAULT_LIMIT, ge=1),
) -> Response:
    check_key(request)
    mode = (t or "search").lower()
    if mode == "caps":
        return caps()
    if mode not in {"search", "movie", "tvsearch"}:
        raise HTTPException(status_code=400, detail=f"Unsupported search type: {mode}")

    effective_mode = mode_from_categories(
        mode,
        cat,
    )

    limit = min(limit, MAX_LIMIT)
    wanted = offset + limit
    candidate_limit = min(1000, max(MEILI_CANDIDATE_LIMIT, wanted * 3))

    # Generic Torznab probe / RSS without query: return a few recent French documents.
    if not (q and q.strip()) and not imdbid and tmdbid is None and season is None and ep is None and year is None:
        filter_expr = combine_filters(type_filter(effective_mode), 'languages = "fr"')
        body = await meili_search(q="", filter_expr=filter_expr, limit=min(5, limit), sort=["added:desc"])
        rows = [dict(x) for x in body.get("hits", []) if valid_hash(x.get("hash")) and is_french(x)]
        await enrich_seeders(rows)
        return feed(rows, len(rows), 0)

    candidates: dict[str, dict[str, Any]] = {}

    direct = await collect_id_candidates(effective_mode, imdbid, tmdbid, candidate_limit)
    for row in direct:
        h = valid_hash(row.get("hash"))
        if h:
            candidates[h] = row

    search_q = (q or "").strip()
    if not search_q and (imdbid or tmdbid is not None):
        search_q = (
            await resolve_title_from_postgres(mode, imdbid, tmdbid)
            or resolved_title(direct)
            or ""
        )

    if search_q:
        text_rows = await collect_text_candidates(effective_mode, search_q, candidate_limit)
        for row in text_rows:
            h = valid_hash(row.get("hash"))
            if not h:
                continue
            old = candidates.get(h)
            if old is None:
                candidates[h] = row
            else:
                old["_match_rank"] = max(int(old.get("_match_rank") or 0), int(row.get("_match_rank") or 0))

    inferred_year: int | None = None

    if (
        year is None
        and effective_mode == "movie"
        and bool(search_q)
        and not imdbid
        and tmdbid is None
    ):
        (
            inferred_year,
            consensus_votes,
            consensus_total,
        ) = infer_consensus_movie_year(
            candidates.values(),
            search_q,
            cat,
        )

        if inferred_year is not None:
            print(
                "[MEILI][YEAR-CONSENSUS] "
                f"q={search_q!r} "
                f"year={inferred_year} "
                f"votes={consensus_votes}/"
                f"{consensus_total}"
            )

    filtered: list[dict[str, Any]] = []
    for row in candidates.values():
        if not is_french(row):
            continue

        if not category_allowed(row, cat):
            continue

        if search_q and int(row.get("_match_rank") or 0) < 90 and title_score(row, search_q) <= 0:
            continue
        if year is not None:
            if not matches_year(row, year):
                continue

        elif inferred_year is not None:
            row_year = candidate_release_year(
                row
            )

            if (
                row_year is not None
                and row_year != inferred_year
            ):
                continue
        if not matches_season_episode(row, season, ep):
            continue
        filtered.append(row)

    await enrich_seeders(filtered)

    filtered.sort(
        key=lambda row: (
            int(row.get("_match_rank") or 0),
            int(row.get("seeders") or 0),
            int(row.get("added") or 0),
        ),
        reverse=True,
    )

    if ALLDEBRID_CACHE_ONLY:
        max_check = max(
            1,
            min(
                ALLDEBRID_CHECK_LIMIT,
                ALLDEBRID_MAX_CHECK,
                ALLDEBRID_SAFE_MAX_CHECK,
            ),
        )

        ad_input = filtered[:max_check]

        ad_started = time.perf_counter()

        filtered = await filter_alldebrid_results(
            ad_input
        )

        print(
            "[TORZNAB][ALLDEBRID] "
            f"candidates={len(ad_input)} "
            f"cached={len(filtered)} "
            f"time="
            f"{time.perf_counter() - ad_started:.3f}s"
        )

    rows = filtered[offset:offset + limit]
    total = len(filtered)
    return feed(rows, total, offset)
