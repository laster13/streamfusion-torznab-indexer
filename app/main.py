from __future__ import annotations

import os
import time
import asyncio
import httpx
from datetime import datetime, timezone
from email.utils import format_datetime
from urllib.parse import quote
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from xml.etree.ElementTree import Element, SubElement, tostring, register_namespace

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://streamfusion:CHANGE_ME@postgresql:5432/streamfusion",
)
INDEXER_NAME = os.getenv("INDEXER_NAME", "StreamFusion PostgreSQL")
API_KEY = os.getenv("INDEXER_API_KEY", "").strip()
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))
MAX_LIMIT = int(os.getenv("MAX_LIMIT", "200"))

ALLDEBRID_API_KEY = os.getenv(
    "ALLDEBRID_API_KEY",
    ""
).strip()

ALLDEBRID_CACHE_ONLY = os.getenv(
    "ALLDEBRID_CACHE_ONLY",
    "false"
).strip().lower() in {"1", "true", "yes", "on"}

ALLDEBRID_CHECK_LIMIT = int(
    os.getenv(
        "ALLDEBRID_CHECK_LIMIT",
        "30"
    )
)


ALLDEBRID_BATCH_SIZE = int(
    os.getenv(
        "ALLDEBRID_BATCH_SIZE",
        "10"
    )
)

ALLDEBRID_MAX_CHECK = int(
    os.getenv(
        "ALLDEBRID_MAX_CHECK",
        "30"
    )
)

engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
)

app = FastAPI(title="StreamFusion PostgreSQL Torznab Indexer", version="1.0.0")
TORZNAB_NS = "http://torznab.com/schemas/2015/feed"
register_namespace("torznab", TORZNAB_NS)


def xml_response(root: Element) -> Response:
    data = b'<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(root, encoding="utf-8")
    return Response(data, media_type="application/xml; charset=utf-8")


def check_key(request: Request) -> None:
    if API_KEY and request.query_params.get("apikey", "") != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


def effective_torznab_limits() -> tuple[int, int]:
    """
    Retourne :
        (default_limit, max_limit)

    En cache-only :
    - défaut = taille normale d'un lot AllDebrid ;
    - maximum = plafond absolu réellement supporté.

    Sans cache-only :
    - comportement Torznab normal.
    """
    if ALLDEBRID_CACHE_ONLY:
        max_limit = max(
            1,
            min(
                MAX_LIMIT,
                ALLDEBRID_CHECK_LIMIT,
                ALLDEBRID_MAX_CHECK,
                ALLDEBRID_SAFE_MAX_CHECK,
            ),
        )

        default_limit = max(
            1,
            min(
                DEFAULT_LIMIT,
                ALLDEBRID_BATCH_SIZE,
                max_limit,
            ),
        )

        return default_limit, max_limit

    max_limit = max(1, MAX_LIMIT)

    default_limit = max(
        1,
        min(
            DEFAULT_LIMIT,
            max_limit,
        ),
    )

    return default_limit, max_limit


def caps() -> Response:
    default_limit, max_limit = effective_torznab_limits()

    root = Element("caps")
    server = SubElement(root, "server", version="1.0", title=INDEXER_NAME)
    SubElement(
        root,
        "limits",
        max=str(max_limit),
        default=str(default_limit),
    )
    searching = SubElement(root, "searching")
    SubElement(searching, "search", available="yes", supportedParams="q")
    SubElement(searching, "movie-search", available="yes", supportedParams="q,imdbid,tmdbid,year")
    SubElement(searching, "tv-search", available="yes", supportedParams="q,season,ep,imdbid,tmdbid")
    cats = SubElement(root, "categories")
    movie = SubElement(cats, "category", id="2000", name="Movies")
    for cid, name in [("2030","Movies/SD"),("2040","Movies/HD"),("2045","Movies/UHD")]:
        SubElement(movie, "subcat", id=cid, name=name)
    tv = SubElement(cats, "category", id="5000", name="TV")
    for cid, name in [("5030","TV/SD"),("5040","TV/HD"),("5045","TV/UHD"),("5070","TV/Anime")]:
        SubElement(tv, "subcat", id=cid, name=name)
    return xml_response(root)


def normalize_imdb(v: str | None) -> str | None:
    if not v:
        return None
    v = v.strip()
    return "tt" + v if v.isdigit() else v


def mode_from_categories(
    mode: str,
    cat: str | None,
) -> str:
    """
    Convertit les catégories Torznab en restriction de type
    uniquement pour une recherche générique t=search.

    2xxx -> movie
    5xxx -> tvsearch

    Une requête mixte Movie + TV reste une recherche générale.
    Les modes explicites movie/tvsearch restent prioritaires.
    """
    if mode in {"movie", "tvsearch"}:
        return mode

    if mode != "search" or not cat:
        return mode

    categories: set[int] = set()

    for raw in cat.split(","):
        raw = raw.strip()

        if not raw:
            continue

        try:
            categories.add(int(raw))
        except ValueError:
            continue

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


def category(row: dict[str, Any]) -> str:
    typ = (row.get("type") or "").lower()
    parsed = row.get("parsed_data") or {}
    res = str(parsed.get("resolution") or "").lower()
    if typ == "series":
        if "2160" in res or "4k" in res: return "5045"
        if "1080" in res or "720" in res: return "5040"
        return "5000"
    if "2160" in res or "4k" in res: return "2045"
    if "1080" in res or "720" in res: return "2040"
    return "2000"


def pubdate(v: Any) -> str:
    try:
        dt = datetime.fromtimestamp(float(v), tz=timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)
    return format_datetime(dt)


def magnet(row: dict[str, Any]) -> str:
    m = (row.get("magnet") or "").strip()
    if m:
        return m
    h = (row.get("info_hash") or "").strip()
    if not h:
        return ""
    return f"magnet:?xt=urn:btih:{quote(h)}&dn={quote(str(row.get('raw_title') or ''))}"



ALLDEBRID_SAFE_UPLOAD_BATCH = 10
ALLDEBRID_SAFE_MAX_CHECK = 30
ALLDEBRID_CAPACITY_GUARD = 950
ALLDEBRID_BACKOFF_SECONDS = 900
ALLDEBRID_BLOCKED_UNTIL = 0.0
ALLDEBRID_LOCK = asyncio.Lock()

# L'API AllDebrid autorise actuellement 12 req/s.
# On garde une marge de sécurité à ~9 req/s.
ALLDEBRID_RATE_LOCK = asyncio.Lock()
ALLDEBRID_LAST_REQUEST_AT = 0.0
ALLDEBRID_MIN_REQUEST_INTERVAL = 0.115


async def alldebrid_rate_wait() -> None:
    global ALLDEBRID_LAST_REQUEST_AT

    async with ALLDEBRID_RATE_LOCK:
        now = time.monotonic()

        delay = (
            ALLDEBRID_MIN_REQUEST_INTERVAL
            - (now - ALLDEBRID_LAST_REQUEST_AT)
        )

        if delay > 0:
            await asyncio.sleep(delay)

        ALLDEBRID_LAST_REQUEST_AT = time.monotonic()


def _valid_btih(value: str) -> bool:
    value = value.strip().lower()
    return (
        len(value) == 40
        and all(c in "0123456789abcdef" for c in value)
    )


async def alldebrid_status_snapshot(
    client: httpx.AsyncClient,
) -> list[dict[str, Any]]:
    status_started = time.perf_counter()

    await alldebrid_rate_wait()

    response = await client.post(
        "https://api.alldebrid.com/v4.1/magnet/status",
        headers={
            "Authorization": f"Bearer {ALLDEBRID_API_KEY}"
        },
    )

    response.raise_for_status()
    body = response.json()

    if body.get("status") != "success":
        raise RuntimeError(
            f"Réponse magnet/status invalide: {body}"
        )

    magnets = body.get("data", {}).get("magnets", []) or []

    if not isinstance(magnets, list):
        raise RuntimeError(
            "Réponse magnet/status: magnets n'est pas une liste"
        )

    print(
        "[ALLDEBRID][TIMING] "
        f"status={time.perf_counter() - status_started:.3f}s "
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
            "Authorization": f"Bearer {ALLDEBRID_API_KEY}"
        },
        data={
            "id": str(magnet_id)
        },
    )

    response.raise_for_status()
    body = response.json()

    if body.get("status") != "success":
        raise RuntimeError(
            f"Echec suppression magnet {magnet_id}: {body}"
        )


async def alldebrid_cached_hashes(
    rows: list[dict[str, Any]],
) -> tuple[set[str], set[str]]:
    global ALLDEBRID_BLOCKED_UNTIL

    if not rows:
        return set(), set()

    if not ALLDEBRID_API_KEY:
        raise HTTPException(
            status_code=503,
            detail=(
                "ALLDEBRID_CACHE_ONLY=true mais "
                "ALLDEBRID_API_KEY est vide"
            ),
        )

    hashes: list[str] = []
    seen: set[str] = set()

    for row in rows:
        h = str(row.get("info_hash") or "").strip().lower()

        if not _valid_btih(h):
            continue

        if h in seen:
            continue

        seen.add(h)
        hashes.append(h)

        if len(hashes) >= ALLDEBRID_SAFE_MAX_CHECK:
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
            #
            # Deux objectifs :
            # 1. ne jamais supprimer un magnet qui existait déjà ;
            # 2. éviter tout nouvel upload si le compte est saturé.
            try:
                snapshot = await alldebrid_status_snapshot(client)
            except Exception as exc:
                print(
                    "[ALLDEBRID][FAIL-CLOSED] "
                    f"status impossible: {type(exc).__name__}: {exc}"
                )
                return set(), set()

            existing_ids: set[int] = set()

            for item in snapshot:
                magnet_id = item.get("id")

                if magnet_id is not None:
                    try:
                        existing_ids.add(int(magnet_id))
                    except (TypeError, ValueError):
                        pass

                # Certaines réponses AllDebrid exposent aussi le hash.
                # Si présent, on peut utiliser les torrents déjà dans
                # le compte sans aucun nouvel upload.
                h = str(item.get("hash") or "").strip().lower()

                if not _valid_btih(h) or h not in wanted:
                    continue

                if item.get("statusCode") == 4:
                    cached.add(h)
                else:
                    uncached.add(h)

            unknown = [
                h
                for h in hashes
                if h not in cached and h not in uncached
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
                    f"cached_known={len(cached)} "
                    f"unknown={len(unknown)}"
                )
                return cached, uncached

            # Garde volontairement conservatrice.
            # L'API annonce une limite de 1000 magnets.
            if len(existing_ids) >= ALLDEBRID_CAPACITY_GUARD:
                ALLDEBRID_BLOCKED_UNTIL = (
                    now + ALLDEBRID_BACKOFF_SECONDS
                )

                print(
                    "[ALLDEBRID][CAPACITY-GUARD] "
                    f"existing={len(existing_ids)} "
                    f"threshold={ALLDEBRID_CAPACITY_GUARD} "
                    "aucun upload effectué"
                )

                return cached, uncached

            for pos in range(
                0,
                len(unknown),
                ALLDEBRID_SAFE_UPLOAD_BATCH,
            ):
                batch = unknown[
                    pos:pos + ALLDEBRID_SAFE_UPLOAD_BATCH
                ]

                created_ids: list[int] = []

                try:
                    upload_started = time.perf_counter()

                    await alldebrid_rate_wait()

                    response = await client.post(
                        "https://api.alldebrid.com/v4/magnet/upload",
                        headers={
                            "Authorization":
                                f"Bearer {ALLDEBRID_API_KEY}"
                        },
                        data={
                            "magnets[]": batch
                        },
                    )

                    response.raise_for_status()
                    body = response.json()

                    if body.get("status") != "success":
                        error = body.get("error", {}) or {}
                        code = str(error.get("code") or "")

                        if code in {
                            "MAGNET_TOO_MANY",
                            "MAGNET_TOO_MANY_ACTIVE",
                        }:
                            ALLDEBRID_BLOCKED_UNTIL = (
                                time.monotonic()
                                + ALLDEBRID_BACKOFF_SECONDS
                            )

                            print(
                                "[ALLDEBRID][LIMIT] "
                                f"{code}; backoff activé"
                            )

                            return cached, uncached

                        raise RuntimeError(
                            f"Réponse upload invalide: {body}"
                        )

                    magnets = (
                        body.get("data", {})
                        .get("magnets", [])
                        or []
                    )

                    upload_elapsed = (
                        time.perf_counter() - upload_started
                    )

                    for item in magnets:
                        h = str(
                            item.get("hash") or ""
                        ).strip().lower()

                        magnet_id = item.get("id")

                        # On ne supprimera QUE les IDs qui n'existaient
                        # pas dans le snapshot pris avant l'upload.
                        if magnet_id is not None:
                            try:
                                numeric_id = int(magnet_id)

                                if numeric_id not in existing_ids:
                                    created_ids.append(numeric_id)
                            except (TypeError, ValueError):
                                pass

                        if not _valid_btih(h):
                            continue

                        if item.get("ready") is True:
                            cached.add(h)

                        elif item.get("ready") is False:
                            uncached.add(h)

                except HTTPException:
                    raise

                except Exception as exc:
                    print(
                        "[ALLDEBRID][FAIL-CLOSED] "
                        f"upload: {type(exc).__name__}: {exc}"
                    )
                    return cached, uncached

                finally:
                    async def cleanup_one(
                        magnet_id: int,
                    ) -> str | None:
                        try:
                            await alldebrid_delete_magnet(
                                client,
                                magnet_id,
                            )
                            return None
                        except Exception as exc:
                            return (
                                f"{magnet_id}: "
                                f"{type(exc).__name__}: {exc}"
                            )

                    cleanup_started = time.perf_counter()

                    cleanup_results = await asyncio.gather(
                        *[
                            cleanup_one(magnet_id)
                            for magnet_id in created_ids
                        ]
                    )

                    cleanup_elapsed = (
                        time.perf_counter() - cleanup_started
                    )

                    cleanup_errors = [
                        error
                        for error in cleanup_results
                        if error is not None
                    ]

                    if cleanup_errors:
                        ALLDEBRID_BLOCKED_UNTIL = (
                            time.monotonic()
                            + ALLDEBRID_BACKOFF_SECONDS
                        )

                        print(
                            "[ALLDEBRID][CLEANUP-ERROR] "
                            + " | ".join(cleanup_errors)
                        )

                        raise HTTPException(
                            status_code=502,
                            detail=(
                                "Nettoyage AllDebrid incomplet. "
                                "Nouveaux contrôles temporairement "
                                "bloqués."
                            ),
                        )

                print(
                    "[ALLDEBRID][BATCH] "
                    f"checked={len(batch)} "
                    f"cached={len(cached)} "
                    f"cleanup={len(created_ids)} "
                    f"upload_time={upload_elapsed:.3f}s "
                    f"cleanup_time={cleanup_elapsed:.3f}s"
                )

    return cached, uncached


async def filter_alldebrid_results(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    # Aucun appel AllDebrid lorsque le filtrage cache-only est désactivé.
    if not ALLDEBRID_CACHE_ONLY:
        return rows


    cached, _ = await alldebrid_cached_hashes(rows)

    # Cette fonction n'arrive ici qu'en mode cache-only :
    # lorsque CACHE_ONLY=false, elle a déjà retourné rows
    # quelques lignes plus haut.
    wanted = cached

    return [
        row
        for row in rows
        if str(
            row.get("info_hash") or ""
        ).strip().lower() in wanted
    ]

def feed(rows: list[dict[str, Any]], total: int, offset: int) -> Response:
    rss = Element("rss", version="2.0")
    ch = SubElement(rss, "channel")
    SubElement(ch, "title").text = INDEXER_NAME
    SubElement(ch, "description").text = "StreamFusion PostgreSQL Torznab indexer"
    SubElement(ch, f"{{{TORZNAB_NS}}}response", offset=str(offset), total=str(total))

    for row in rows:
        item = SubElement(ch, "item")
        title = str(row.get("raw_title") or "")
        h = str(row.get("info_hash") or row.get("id") or "")
        m = magnet(row)
        size = int(row.get("size") or 0)
        seeders = int(row.get("seeders") or 0)
        cat = category(row)

        SubElement(item, "title").text = title
        SubElement(item, "guid", isPermaLink="false").text = h
        SubElement(item, "link").text = m
        SubElement(item, "pubDate").text = pubdate(row.get("created_at"))
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
        if row.get("imdb_id"):
            attrs["imdb"] = str(row["imdb_id"]).removeprefix("tt")
        if row.get("tmdb_id"):
            attrs["tmdbid"] = str(row["tmdb_id"])
        if row.get("indexer"):
            attrs["source"] = str(row["indexer"])

        for k, v in attrs.items():
            SubElement(item, f"{{{TORZNAB_NS}}}attr", name=k, value=v)

    return xml_response(rss)


def build_sql(
    mode: str,
    q: str | None,
    imdbid: str | None,
    tmdbid: int | None,
    season: int | None,
    ep: int | None,
    year: int | None,
    offset: int,
    limit: int,
):
    params: dict[str, Any] = {
        "offset": offset,
        "limit": limit,
    }

    imdbid = normalize_imdb(imdbid)

    base_filters = [
        "t.info_hash IS NOT NULL",
    ]

    if mode == "movie":
        base_filters.append("t.type = 'movie'")
    elif mode == "tvsearch":
        base_filters.append("t.type = 'series'")

    extra_filters = []

    if year is not None:
        params["year"] = str(year)
        extra_filters.append(
            """(
                COALESCE(t.parsed_data::jsonb->>'year','') = :year
                OR t.raw_title ILIKE ('%' || :year || '%')
            )"""
        )

    if season is not None:
        params["season"] = season
        params["season_tag"] = f"%S{season:02d}%"
        extra_filters.append(
            """(
                COALESCE(
                    t.parsed_data::jsonb->'seasons',
                    '[]'::jsonb
                ) @> to_jsonb(ARRAY[:season]::int[])
                OR t.raw_title ILIKE :season_tag
            )"""
        )

    if ep is not None:
        params["ep"] = ep
        params["ep_tag"] = f"%E{ep:02d}%"
        extra_filters.append(
            """(
                COALESCE(
                    t.parsed_data::jsonb->'episodes',
                    '[]'::jsonb
                ) @> to_jsonb(ARRAY[:ep]::int[])
                OR t.raw_title ILIKE :ep_tag
            )"""
        )

    common = " AND ".join(
        base_filters + extra_filters
    )

    branches = []

    have_external_id = (
        tmdbid is not None
        or bool(imdbid)
    )

    if tmdbid is not None:
        params["tmdbid"] = tmdbid

        branches.append(
            f"""
            SELECT
                t.id,
                t.raw_title,
                t.size,
                t.magnet,
                t.info_hash,
                t.seeders,
                t.indexer,
                t.type,
                t.parsed_data,
                t.created_at,
                t.updated_at,
                t.tmdb_id,
                t.imdb_id,
                100 AS match_rank
            FROM torrent_items t
            WHERE {common}
              AND t.tmdb_id = :tmdbid
            """
        )

    if imdbid:
        params["imdbid"] = imdbid

        branches.append(
            f"""
            SELECT
                t.id,
                t.raw_title,
                t.size,
                t.magnet,
                t.info_hash,
                t.seeders,
                t.indexer,
                t.type,
                t.parsed_data,
                t.created_at,
                t.updated_at,
                t.tmdb_id,
                t.imdb_id,
                100 AS match_rank
            FROM torrent_items t
            WHERE {common}
              AND t.imdb_id = :imdbid
            """
        )

    if q and q.strip():
        clean_q = q.strip()

        params["q_exact"] = clean_q.lower()
        params["q_like"] = f"%{clean_q}%"

        # Prowlarr effectue aussi des recherches texte sous la forme :
        #
        #   "Le Grand Escogriffe 1976"
        #
        # alors que normalized_title / parsed_title contiennent souvent :
        #
        #   "le grand escogriffe"
        #
        # On conserve TOUJOURS la recherche originale et on ajoute
        # seulement un fallback titre + année lorsque le dernier token
        # est une année plausible.
        year_fallback_case = ""
        year_fallback_where = ""

        q_parts = clean_q.rsplit(maxsplit=1)

        if len(q_parts) == 2:
            possible_title = q_parts[0].strip()
            possible_year = q_parts[1].strip()

            current_year = datetime.now(
                timezone.utc
            ).year

            if (
                possible_title
                and len(possible_year) == 4
                and possible_year.isdigit()
                and 1900
                <= int(possible_year)
                <= current_year + 2
            ):
                params["q_title_exact"] = (
                    possible_title.lower()
                )

                params["q_title_like"] = (
                    f"%{possible_title}%"
                )

                # Pour raw_title :
                # "Le Grand Escogriffe" devient
                # "%Le%Grand%Escogriffe%"
                # afin de supporter les séparateurs . _ - etc.
                params["q_title_tokens_like"] = (
                    "%"
                    + "%".join(
                        possible_title.split()
                    )
                    + "%"
                )

                params["q_year"] = possible_year
                params["q_year_like"] = (
                    f"%{possible_year}%"
                )

                year_fallback_case = """
                    WHEN (
                        (
                            lower(
                                COALESCE(
                                    t.parsed_data::jsonb
                                    ->>'normalized_title',
                                    ''
                                )
                            ) = :q_title_exact

                            OR lower(
                                COALESCE(
                                    t.parsed_data::jsonb
                                    ->>'parsed_title',
                                    ''
                                )
                            ) = :q_title_exact
                        )
                        AND (
                            COALESCE(
                                t.parsed_data::jsonb->>'year',
                                ''
                            ) = :q_year

                            OR t.raw_title
                               ILIKE :q_year_like
                        )
                    )
                    THEN 80

                    WHEN (
                        (
                            (
                                t.parsed_data::jsonb
                                ->>'normalized_title'
                            ) ILIKE :q_title_like

                            OR (
                                t.parsed_data::jsonb
                                ->>'parsed_title'
                            ) ILIKE :q_title_like

                            OR t.raw_title
                               ILIKE :q_title_tokens_like
                        )
                        AND (
                            COALESCE(
                                t.parsed_data::jsonb->>'year',
                                ''
                            ) = :q_year

                            OR t.raw_title
                               ILIKE :q_year_like
                        )
                    )
                    THEN 55
                """

                year_fallback_where = """
                    OR (
                        (
                            (
                                t.parsed_data::jsonb
                                ->>'normalized_title'
                            ) ILIKE :q_title_like

                            OR (
                                t.parsed_data::jsonb
                                ->>'parsed_title'
                            ) ILIKE :q_title_like

                            OR t.raw_title
                               ILIKE :q_title_tokens_like
                        )
                        AND (
                            COALESCE(
                                t.parsed_data::jsonb->>'year',
                                ''
                            ) = :q_year

                            OR t.raw_title
                               ILIKE :q_year_like
                        )
                    )
                """

        no_id_filter = ""

        if have_external_id:
            no_id_filter = """
              AND t.tmdb_id IS NULL
              AND t.imdb_id IS NULL
            """

        branches.append(
            f"""
            SELECT
                t.id,
                t.raw_title,
                t.size,
                t.magnet,
                t.info_hash,
                t.seeders,
                t.indexer,
                t.type,
                t.parsed_data,
                t.created_at,
                t.updated_at,
                t.tmdb_id,
                t.imdb_id,

                CASE
                    WHEN lower(
                        COALESCE(
                            t.parsed_data::jsonb->>'normalized_title',
                            ''
                        )
                    ) = :q_exact
                    THEN 90

                    WHEN lower(
                        COALESCE(
                            t.parsed_data::jsonb->>'parsed_title',
                            ''
                        )
                    ) = :q_exact
                    THEN 85

                    {year_fallback_case}

                    WHEN (
                        t.parsed_data::jsonb
                        ->>'normalized_title'
                    ) ILIKE :q_like
                    THEN 70

                    WHEN (
                        t.parsed_data::jsonb
                        ->>'parsed_title'
                    ) ILIKE :q_like
                    THEN 60

                    ELSE 50
                END AS match_rank

            FROM torrent_items t

            WHERE {common}
              {no_id_filter}
              AND (
                    (
                        t.parsed_data::jsonb
                        ->>'normalized_title'
                    ) ILIKE :q_like

                    OR (
                        t.parsed_data::jsonb
                        ->>'parsed_title'
                    ) ILIKE :q_like

                    OR t.raw_title ILIKE :q_like

                    {year_fallback_where}
                  )
            """
        )

    if not branches:
        # Recherche générique sans titre/TMDB/IMDb.
        #
        # Ne surtout pas envoyer les centaines de milliers de lignes
        # correspondantes au CTE de déduplication : cela provoquait des
        # tris externes de plusieurs centaines de Mo sur disque.
        #
        # On sélectionne d'abord un ensemble largement supérieur à la
        # page réellement demandée, classé selon le même critère global.
        # La déduplication finale ne travaille ensuite que sur ce petit
        # ensemble.
        generic_candidate_limit = max(
            300,
            offset + (limit * 10),
        )

        params["generic_candidate_limit"] = (
            generic_candidate_limit
        )

        branches.append(
            f"""
            SELECT
                t.id,
                t.raw_title,
                t.size,
                t.magnet,
                t.info_hash,
                t.seeders,
                t.indexer,
                t.type,
                t.parsed_data,
                t.created_at,
                t.updated_at,
                t.tmdb_id,
                t.imdb_id,
                10 AS match_rank
            FROM torrent_items t
            WHERE {common}
            ORDER BY
                COALESCE(t.seeders, 0) DESC,
                t.created_at DESC NULLS LAST
            LIMIT :generic_candidate_limit
            """
        )

    union_sql = "\nUNION ALL\n".join(branches)

    sql = f"""
    WITH candidates AS MATERIALIZED (
        {union_sql}
    ),

    dedup AS (
        SELECT DISTINCT ON (info_hash)
            id,
            raw_title,
            size,
            magnet,
            info_hash,
            seeders,
            indexer,
            type,
            parsed_data,
            created_at,
            updated_at,
            tmdb_id,
            imdb_id,
            match_rank
        FROM candidates
        ORDER BY
            info_hash,
            match_rank DESC,
            COALESCE(seeders, 0) DESC,
            COALESCE(updated_at, 0) DESC
    )

    SELECT
        *,
        0::bigint AS total_count
    FROM dedup
    ORDER BY
        match_rank DESC,
        COALESCE(seeders, 0) DESC,
        created_at DESC NULLS LAST
    OFFSET :offset
    LIMIT :limit
    """

    return sql, params

@app.get("/")
async def root():
    return {"name": INDEXER_NAME, "status": "ok", "torznab": "/torznab/api"}


@app.get("/health")
async def health():
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.get("/torznab/api")
async def torznab(
    request: Request,
    t: str = Query("search"),
    q: str | None = None,
    cat: str | None = None,
    imdbid: str | None = None,
    tmdbid: int | None = None,
    season: int | None = None,
    ep: int | None = None,
    year: int | None = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(
        effective_torznab_limits()[0],
        ge=1,
    ),
):
    check_key(request)
    t = (t or "search").lower()

    if t == "caps":
        return caps()

    if t not in {"search", "movie", "tvsearch"}:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported search type: {t}",
        )

    sql_mode = mode_from_categories(
        t,
        cat,
    )

    # Fast-path pour le probe de validation Generic Torznab
    # envoyé par Prowlarr :
    #
    #   t=search&extended=1&apikey=...
    #
    # Ce probe a uniquement besoin de quelques résultats valides.
    # On évite donc la recherche générale coûteuse sur torrent_items.
    query_keys = set(request.query_params.keys())

    is_prowlarr_probe = (
        t == "search"
        and not (q and q.strip())
        and not imdbid
        and tmdbid is None
        and season is None
        and ep is None
        and year is None
        and cat is None
        and query_keys.issubset(
            {"t", "extended", "apikey"}
        )
    )

    if is_prowlarr_probe:
        probe_sql = text(
            """
            SELECT *
            FROM torrent_items
            WHERE raw_title IS NOT NULL
              AND btrim(raw_title) <> ''
              AND info_hash IS NOT NULL
              AND info_hash ~ '^[0-9A-Fa-f]{40}$'
              AND type IN ('movie', 'series')
            LIMIT 5
            """
        )

        async with engine.connect() as conn:
            result = await conn.execute(probe_sql)
            probe_rows = [
                dict(row)
                for row in result.mappings().all()
            ]

        print(
            f"[TORZNAB][PROWLARR-PROBE] "
            f"fast-path rows={len(probe_rows)}"
        )

        return feed(
            probe_rows,
            len(probe_rows),
            0,
        )

    _, effective_max_limit = effective_torznab_limits()

    limit = min(
        limit,
        effective_max_limit,
    )

    requested_limit = limit

    # Sonarr/Prowlarr envoie souvent uniquement TMDB/IMDb sans q.
    # On récupère alors le titre depuis les torrents déjà identifiés
    # afin que build_sql() puisse également rechercher les torrents
    # sans TMDB/IMDb via son fallback texte.
    if (
        not (q and q.strip())
        and (
            tmdbid is not None
            or bool(imdbid)
        )
    ):
        normalized_imdb = normalize_imdb(imdbid)

        title_conditions = []
        title_params = {}

        if tmdbid is not None:
            title_conditions.append(
                "t.tmdb_id = :fallback_tmdbid"
            )
            title_params["fallback_tmdbid"] = tmdbid

        if normalized_imdb:
            title_conditions.append(
                "t.imdb_id = :fallback_imdbid"
            )
            title_params["fallback_imdbid"] = normalized_imdb

        if title_conditions:
            title_sql = f"""
                SELECT
                    COALESCE(
                        NULLIF(
                            t.parsed_data::jsonb->>'normalized_title',
                            ''
                        ),
                        NULLIF(
                            t.parsed_data::jsonb->>'parsed_title',
                            ''
                        )
                    ) AS resolved_title,
                    COUNT(*) AS occurrences,
                    MAX(COALESCE(t.seeders, 0)) AS max_seeders
                FROM torrent_items t
                WHERE (
                    {' OR '.join(title_conditions)}
                )
                  AND COALESCE(
                        NULLIF(
                            t.parsed_data::jsonb->>'normalized_title',
                            ''
                        ),
                        NULLIF(
                            t.parsed_data::jsonb->>'parsed_title',
                            ''
                        )
                      ) IS NOT NULL
                GROUP BY resolved_title
                ORDER BY
                    occurrences DESC,
                    max_seeders DESC
                LIMIT 1
            """

            async with engine.connect() as conn:
                title_res = await conn.execute(
                    text(title_sql),
                    title_params,
                )

                resolved_title = title_res.scalar_one_or_none()

            if resolved_title:
                q = str(resolved_title).strip()

                print(
                    "[TORZNAB][TITLE-FALLBACK] "
                    f"tmdb={tmdbid} "
                    f"imdb={normalized_imdb} "
                    f"q={q!r}"
                )

    batch_size = max(
        requested_limit,
        ALLDEBRID_BATCH_SIZE,
    )

    if ALLDEBRID_CACHE_ONLY:
        # En mode cache-only, les limites AllDebrid sont de vrais
        # plafonds absolus, indépendamment du limit demandé par
        # Prowlarr.
        max_check = max(
            1,
            min(
                ALLDEBRID_CHECK_LIMIT,
                ALLDEBRID_MAX_CHECK,
                ALLDEBRID_SAFE_MAX_CHECK,
            ),
        )

        batch_size = min(
            batch_size,
            max_check,
        )
    else:
        # Sans filtrage AllDebrid, ne pas limiter artificiellement
        # le nombre de résultats PostgreSQL demandé par Prowlarr.
        max_check = max(
            batch_size,
            ALLDEBRID_MAX_CHECK,
        )

    # Nombre de résultats filtrés nécessaires pour
    # satisfaire la pagination demandée par Prowlarr.
    wanted_count = offset + requested_limit

    filtered_rows = []

    query_offset = 0
    checked = 0
    batch_number = 0

    request_started = time.perf_counter()
    postgres_total_time = 0.0
    alldebrid_total_time = 0.0

    print(
        "[TORZNAB] "
        f"type={t} "
        f"sql_mode={sql_mode} "
        f"cat={cat!r} "
        f"q={q!r} "
        f"tmdb={tmdbid} "
        f"imdb={imdbid} "
        f"season={season} "
        f"ep={ep} "
        f"cache_only={ALLDEBRID_CACHE_ONLY} "
        f"limit={requested_limit} "
        f"offset={offset}"
    )

    while checked < max_check:
        batch_number += 1

        remaining = max_check - checked

        current_batch_size = min(
            batch_size,
            remaining,
        )

        sql, params = build_sql(
            sql_mode,
            q,
            imdbid,
            tmdbid,
            season,
            ep,
            year,
            query_offset,
            current_batch_size,
        )

        postgres_started = time.perf_counter()

        async with engine.connect() as conn:
            res = await conn.execute(
                text(sql),
                params,
            )

            batch_rows = [
                dict(r)
                for r in res.mappings().all()
            ]

        postgres_elapsed = (
            time.perf_counter() - postgres_started
        )

        postgres_total_time += postgres_elapsed

        # Plus aucun candidat PostgreSQL.
        if not batch_rows:
            break

        batch_count = len(batch_rows)

        checked += batch_count
        query_offset += batch_count

        # Contrôle AllDebrid du lot uniquement.
        alldebrid_started = time.perf_counter()

        filtered_batch = await filter_alldebrid_results(
            batch_rows
        )

        alldebrid_elapsed = (
            time.perf_counter() - alldebrid_started
        )

        alldebrid_total_time += alldebrid_elapsed

        filtered_rows.extend(
            filtered_batch
        )

        print(
            "[TORZNAB][BATCH] "
            f"n={batch_number} "
            f"offset={query_offset - batch_count} "
            f"postgres_rows={batch_count} "
            f"postgres_time={postgres_elapsed:.3f}s "
            f"ad_kept={len(filtered_batch)} "
            f"ad_time={alldebrid_elapsed:.3f}s "
            f"checked_total={checked} "
            f"kept_total={len(filtered_rows)}"
        )

        # On a suffisamment de résultats pour répondre
        # à la page demandée.
        if len(filtered_rows) >= wanted_count:
            break

        # PostgreSQL a renvoyé moins que demandé :
        # on est arrivé au bout des candidats.
        if batch_count < current_batch_size:
            break

    # Déduplication de sécurité par info_hash entre lots.
    dedup = {}

    for row in filtered_rows:
        h = str(
            row.get("info_hash") or ""
        ).strip().lower()

        if not h:
            continue

        old = dedup.get(h)

        if old is None:
            dedup[h] = row
            continue

        # Même logique de priorité :
        # garder celui avec le plus de seeders.
        if int(row.get("seeders") or 0) > int(
            old.get("seeders") or 0
        ):
            dedup[h] = row

    filtered_rows = list(
        dedup.values()
    )

    # On restaure le classement global.
    filtered_rows.sort(
        key=lambda r: (
            int(r.get("match_rank") or 0),
            int(r.get("seeders") or 0),
            int(r.get("created_at") or 0),
        ),
        reverse=True,
    )

    rows = filtered_rows[
        offset:offset + requested_limit
    ]

    total = offset + len(rows)

    if len(filtered_rows) > offset + requested_limit:
        total += 1

    request_elapsed = (
        time.perf_counter() - request_started
    )

    print(
        "[TORZNAB][DONE] "
        f"checked={checked} "
        f"filtered_unique={len(filtered_rows)} "
        f"returned={len(rows)} "
        f"postgres_total={postgres_total_time:.3f}s "
        f"alldebrid_total={alldebrid_total_time:.3f}s "
        f"total_time={request_elapsed:.3f}s"
    )

    for r in rows:
        r.pop("rn", None)
        r.pop("total_count", None)

    return feed(rows, total, offset)
