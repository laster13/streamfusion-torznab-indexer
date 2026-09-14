from __future__ import annotations

import asyncio
import hmac
import os
import re
import time
from dataclasses import dataclass

import httpx
import redis.asyncio as redis
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import create_async_engine
from app.alldebrid_global_quota import GLOBAL_SECOND_LIMIT, acquire_alldebrid_global_quota


DATABASE_URL = os.environ["DATABASE_URL"].strip()
ALLDEBRID_API_KEY = os.environ[
    "ALLDEBRID_API_KEY"
].strip()

BROKER_TOKEN = os.environ[
    "BROKER_TOKEN"
].strip()

if len(BROKER_TOKEN) < 32:
    raise RuntimeError(
        "BROKER_TOKEN invalide"
    )

REDIS_URL = os.getenv(
    "REDIS_URL",
    "redis://sfr-redis-dev:6379/0",
).strip()

RATE_PER_SECOND = float(GLOBAL_SECOND_LIMIT)

BATCH_SIZE = max(
    1,
    min(
        20,
        int(
            os.getenv(
                "BROKER_BATCH_SIZE",
                "20",
            )
        ),
    ),
)

PARALLEL_BATCHES = max(
    1,
    min(
        6,
        int(
            os.getenv(
                "BROKER_PARALLEL_BATCHES",
                "4",
            )
        ),
    ),
)

BROKER_CLEANUP_QUEUE_KEY = os.getenv(
    "BROKER_CLEANUP_QUEUE_KEY",
    "sf:torznab:alldebrid:cleanup",
).strip()

BROKER_CLEANUP_HIGH_WATER = max(
    20,
    int(
        os.getenv(
            "BROKER_CLEANUP_HIGH_WATER",
            "120",
        )
    ),
)

COALESCE_MS = max(
    0,
    min(
        250,
        int(
            os.getenv(
                "BROKER_COALESCE_MS",
                "80",
            )
        ),
    ),
)

POSITIVE_TTL = max(
    30,
    min(
        3600,
        int(
            os.getenv(
                "BROKER_POSITIVE_TTL",
                "300",
            )
        ),
    ),
)

BACKOFF_SECONDS = 60
CAPACITY_GUARD = 950

HASH_RE = re.compile(
    r"^[0-9a-f]{40}$"
)

engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=1.0,
    socket_timeout=1.0,
)

app = FastAPI(
    title="StreamFusion AllDebrid Broker",
    version="2.0.0",
)

rate_lock = asyncio.Lock()
last_request_at = 0.0

blocked_until = 0.0

cleanup_worker_task: asyncio.Task | None = None

foreground_active = 0
foreground_lock = asyncio.Lock()

# Utilisé UNIQUEMENT lorsqu'AllDebrid répond
# MAGNET_TOO_MANY(_ACTIVE).
#
# Deux recherches ne doivent pas lancer deux
# récupérations de capacité simultanément.
capacity_recovery_lock = asyncio.Lock()

CAPACITY_ERROR_CODES = {
    "MAGNET_TOO_MANY",
    "MAGNET_TOO_MANY_ACTIVE",
}



# Single-flight :
# un hash donné ne peut être contrôlé qu'une seule fois
# simultanément, même si PG et Meili le demandent.
INFLIGHT_LOCK = asyncio.Lock()

INFLIGHT: dict[
    str,
    asyncio.Future,
] = {}

# Deux contrôles AllDebrid peuvent fonctionner
# simultanément.
#
# Chaque contrôle ne garde qu'un batch de 10
# magnets avant cleanup : maximum pratique 20,
# sous la garde historique de 30.
AD_CONCURRENCY = asyncio.Semaphore(2)



class CheckRequest(BaseModel):
    hashes: list[str]


class CheckResponse(BaseModel):
    cached: list[str]
    uncached: list[str]

    requested: int
    unique: int

    redis_hits: int
    pg_hits: int
    status_hits: int

    checked_alldebrid: int

    elapsed: float


@dataclass
class PendingRequest:
    hashes: list[str]
    future: asyncio.Future


queue: asyncio.Queue[
    PendingRequest
] = asyncio.Queue()

worker_task: asyncio.Task | None = None


def normalize_hashes(
    values: list[str],
) -> list[str]:

    result: list[str] = []
    seen: set[str] = set()

    for value in values:
        h = str(
            value or ""
        ).strip().lower()

        if not HASH_RE.fullmatch(h):
            continue

        if h in seen:
            continue

        seen.add(h)
        result.append(h)

    return result


def redis_key(
    info_hash: str,
) -> str:

    return (
        "sf:torznab:"
        "alldebrid:positive:"
        + info_hash
    )



async def rate_wait() -> None:
    await acquire_alldebrid_global_quota()




async def api_post(
    client: httpx.AsyncClient,
    url: str,
    *,
    data=None,
) -> dict:

    await rate_wait()

    response = await client.post(
        url,
        headers={
            "Authorization":
                f"Bearer {ALLDEBRID_API_KEY}"
        },
        data=data,
    )

    if response.status_code in {
        429,
        503,
    }:
        raise HTTPException(
            status_code=503,
            detail=(
                "AllDebrid rate limit "
                f"HTTP {response.status_code}"
            ),
        )

    response.raise_for_status()

    body = response.json()

    if not isinstance(
        body,
        dict,
    ):
        raise RuntimeError(
            "Réponse AllDebrid non JSON"
        )

    return body


async def redis_positive_hits(
    hashes: list[str],
) -> set[str]:

    if not hashes:
        return set()

    try:
        values = await redis_client.mget(
            [
                redis_key(h)
                for h in hashes
            ]
        )

    except Exception as exc:
        print(
            "[BROKER][REDIS-READ-FALLBACK] "
            f"{type(exc).__name__}: {exc}"
        )

        return set()

    return {
        h
        for h, value
        in zip(
            hashes,
            values,
        )
        if value == "1"
    }



async def redis_positive_hits_combined(
    hashes: list[str],
) -> tuple[
    set[str],
    set[str],
]:
    """
    Un SEUL MGET Redis pour :

      1. cache positif court du broker
      2. cache positif Stream-Fusion

    Le cache Stream-Fusion n'est considéré positif
    QUE si le JSON contient exactement :

        "instant": true

    Les sentinelles "not_cached" sont volontairement
    ignorées et continueront donc jusqu'à PostgreSQL
    puis éventuellement AllDebrid.
    """

    if not hashes:
        return set(), set()

    broker_keys = [
        redis_key(h)
        for h in hashes
    ]

    sf_keys = [
        f"debrid_avail:alldebrid:{h}"
        for h in hashes
    ]

    try:

        values = await redis_client.mget(
            broker_keys
            + sf_keys
        )

    except Exception as exc:

        print(
            "[BROKER]"
            "[REDIS-COMBINED-FALLBACK] "
            f"{type(exc).__name__}: "
            f"{exc}"
        )

        return set(), set()

    count = len(hashes)

    broker_values = (
        values[:count]
    )

    sf_values = (
        values[count:]
    )

    broker_hits = {
        h
        for h, value
        in zip(
            hashes,
            broker_values,
        )
        if value == "1"
    }

    sf_hits: set[str] = set()

    # Import local volontaire :
    # aucune modification des imports globaux.
    import json

    for h, value in zip(
        hashes,
        sf_values,
    ):

        if value is None:
            continue

        try:

            if isinstance(
                value,
                bytes,
            ):
                value = value.decode(
                    "utf-8",
                    errors="strict",
                )

            data = json.loads(
                value
            )

        except Exception:
            # Fail-open vers PostgreSQL / AllDebrid.
            continue

        if (
            isinstance(data, dict)
            and data.get("instant") is True
        ):
            sf_hits.add(h)

    return (
        broker_hits,
        sf_hits,
    )


async def remember_positive(
    hashes: set[str],
) -> None:

    if not hashes:
        return

    try:
        pipe = redis_client.pipeline(
            transaction=False
        )

        for h in hashes:
            pipe.set(
                redis_key(h),
                "1",
                ex=POSITIVE_TTL,
            )

        await pipe.execute()

    except Exception as exc:
        print(
            "[BROKER][REDIS-WRITE-FALLBACK] "
            f"{type(exc).__name__}: {exc}"
        )


async def postgres_positive_hits(
    hashes: list[str],
) -> set[str]:

    if not hashes:
        return set()

    query = text("""
        SELECT info_hash
        FROM debrid_cache
        WHERE service = 'alldebrid'
          AND info_hash IN :hashes
          AND expires_at >
              extract(epoch from now())::bigint
          AND cached_data ->> 'instant' = 'true'
    """).bindparams(
        bindparam(
            "hashes",
            expanding=True,
        )
    )

    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                query,
                {
                    "hashes": hashes,
                },
            )

            return {
                str(value)
                .strip()
                .lower()
                for value
                in result.scalars()
                if value
            }

    except Exception as exc:
        print(
            "[BROKER][PG-CACHE-FALLBACK] "
            f"{type(exc).__name__}: "
            f"{exc}"
        )

        return set()



async def enqueue_cleanup_ids(
    magnet_ids: list[int],
) -> None:
    """
    Enregistre les IDs AVANT de répondre.

    Redis rend le cleanup persistant :
    un redémarrage du broker ne perd pas
    les IDs temporaires restant à supprimer.
    """
    if not magnet_ids:
        return

    now = time.time()

    mapping = {
        str(magnet_id): now
        for magnet_id in magnet_ids
    }

    try:
        await redis_client.zadd(
            BROKER_CLEANUP_QUEUE_KEY,
            mapping,
        )

    except Exception as exc:
        # On ne masque pas silencieusement une
        # impossibilité de rendre le cleanup durable.
        print(
            "[BROKER][CLEANUP-QUEUE-ERROR] "
            f"{type(exc).__name__}: {exc}"
        )

        raise


async def cleanup_queue_size() -> int:
    try:
        return int(
            await redis_client.zcard(
                BROKER_CLEANUP_QUEUE_KEY
            )
        )
    except Exception:
        return 0


async def cleanup_one_id(
    magnet_id: int,
) -> bool:
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(12.0)
        ) as client:

            await delete_magnet(
                client,
                magnet_id,
            )

        await redis_client.zrem(
            BROKER_CLEANUP_QUEUE_KEY,
            str(magnet_id),
        )

        return True

    except Exception as exc:
        print(
            "[BROKER][CLEANUP-RETRY] "
            f"id={magnet_id} "
            f"{type(exc).__name__}: {exc}"
        )

        return False


async def drain_cleanup_queue(
    *,
    max_items: int = 30,
    force: bool = False,
) -> int:

    global foreground_active

    if not force:
        async with foreground_lock:
            if foreground_active > 0:
                return 0

    try:
        raw_ids = await redis_client.zrange(
            BROKER_CLEANUP_QUEUE_KEY,
            0,
            max_items - 1,
        )
    except Exception:
        return 0

    if not raw_ids:
        return 0

    cleaned = 0

    for raw_id in raw_ids:

        if not force:
            async with foreground_lock:
                if foreground_active > 0:
                    break

        try:
            magnet_id = int(raw_id)
        except (
            TypeError,
            ValueError,
        ):
            await redis_client.zrem(
                BROKER_CLEANUP_QUEUE_KEY,
                raw_id,
            )
            continue

        if await cleanup_one_id(
            magnet_id
        ):
            cleaned += 1

    if cleaned:
        remaining = (
            await cleanup_queue_size()
        )

        print(
            "[BROKER][CLEANUP-BG] "
            f"deleted={cleaned} "
            f"remaining={remaining}"
        )

    return cleaned


async def cleanup_worker() -> None:
    """
    Cleanup basse priorité.

    Tant qu'une recherche AllDebrid est active,
    le worker laisse toute la bande passante
    au chemin critique.
    """
    while True:
        try:
            cleaned = (
                await drain_cleanup_queue(
                    max_items=30,
                    force=False,
                )
            )

            if cleaned == 0:
                await asyncio.sleep(
                    0.20
                )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            print(
                "[BROKER]"
                "[CLEANUP-WORKER-ERROR] "
                f"{type(exc).__name__}: "
                f"{exc}"
            )

            await asyncio.sleep(
                1.0
            )



async def status_snapshot(
    client: httpx.AsyncClient,
) -> list[dict]:

    body = await api_post(
        client,
        "https://api.alldebrid.com"
        "/v4.1/magnet/status",
    )

    if (
        body.get("status")
        != "success"
    ):
        raise RuntimeError(
            "Status AllDebrid invalide"
        )

    return (
        body.get("data", {})
        .get("magnets", [])
        or []
    )


async def delete_magnet(
    client: httpx.AsyncClient,
    magnet_id: int,
) -> None:

    body = await api_post(
        client,
        "https://api.alldebrid.com"
        "/v4/magnet/delete",
        data={
            "id": str(
                magnet_id
            ),
        },
    )

    if (
        body.get("status")
        == "success"
    ):
        return

    error = (
        body.get("error")
        or {}
    )

    if (
        error.get("code")
        == "MAGNET_INVALID_ID"
    ):
        print(
            "[BROKER]"
            "[DELETE-IDEMPOTENT] "
            f"id={magnet_id}"
        )

        return

    raise RuntimeError(
        "Suppression magnet "
        f"{magnet_id} impossible: "
        f"{error}"
    )




def alldebrid_error_code(
    body: dict,
) -> str:

    if not isinstance(
        body,
        dict,
    ):
        return ""

    error = (
        body.get("error")
        or {}
    )

    if not isinstance(
        error,
        dict,
    ):
        return ""

    return str(
        error.get("code")
        or ""
    )


async def upload_with_capacity_recovery(
    client: httpx.AsyncClient,
    batch: list[str],
) -> tuple[
    dict,
    float,
    bool,
]:
    """
    Chemin normal :
        un seul /magnet/upload.

    Chemin exceptionnel seulement si AD répond
    MAGNET_TOO_MANY ou MAGNET_TOO_MANY_ACTIVE :
        - sérialise la récupération ;
        - vide uniquement NOTRE queue cleanup ;
        - retente le même batch.

    Aucun magnet extérieur au broker n'est supprimé.
    """

    started = time.perf_counter()

    async def do_upload() -> dict:

        return await api_post(
            client,
            "https://api.alldebrid.com"
            "/v4/magnet/upload",
            data={
                "magnets[]": batch,
            },
        )

    body = await do_upload()

    if (
        body.get("status")
        == "success"
    ):
        return (
            body,
            time.perf_counter()
            - started,
            False,
        )

    code = (
        alldebrid_error_code(
            body
        )
    )

    if (
        code
        not in CAPACITY_ERROR_CODES
    ):
        raise RuntimeError(
            "Upload AllDebrid "
            f"invalide: "
            f"{body.get('error')}"
        )

    async with capacity_recovery_lock:

        print(
            "[BROKER]"
            "[CAPACITY-RECOVERY] "
            f"trigger={code} "
            f"batch={len(batch)}"
        )

        # Un autre coroutine peut avoir récupéré
        # de la capacité pendant l'attente du lock.
        #
        # On retente d'abord UNE fois sans delete.
        body = await do_upload()

        if (
            body.get("status")
            == "success"
        ):
            print(
                "[BROKER]"
                "[CAPACITY-RECOVERY] "
                "resolved=concurrent-cleanup"
            )

            return (
                body,
                time.perf_counter()
                - started,
                True,
            )

        code = (
            alldebrid_error_code(
                body
            )
        )

        if (
            code
            not in CAPACITY_ERROR_CODES
        ):
            raise RuntimeError(
                "Upload AllDebrid "
                f"invalide après retry: "
                f"{body.get('error')}"
            )

        # Plusieurs tentatives progressives.
        #
        # Elles ne sont exécutées QUE lorsque
        # la capacité AllDebrid est réellement
        # saturée.
        delays = (
            0.15,
            0.35,
            0.75,
            1.50,
        )

        for attempt, delay in enumerate(
            delays,
            start=1,
        ):

            before = (
                await cleanup_queue_size()
            )

            cleaned = (
                await drain_cleanup_queue(
                    max_items=60,
                    force=True,
                )
            )

            after = (
                await cleanup_queue_size()
            )

            print(
                "[BROKER]"
                "[CAPACITY-RECOVERY] "
                f"attempt={attempt} "
                f"queue_before={before} "
                f"deleted={cleaned} "
                f"queue_after={after}"
            )

            await asyncio.sleep(
                delay
            )

            body = await do_upload()

            if (
                body.get("status")
                == "success"
            ):
                print(
                    "[BROKER]"
                    "[CAPACITY-RECOVERY] "
                    f"resolved=attempt-{attempt}"
                )

                return (
                    body,
                    time.perf_counter()
                    - started,
                    True,
                )

            code = (
                alldebrid_error_code(
                    body
                )
            )

            if (
                code
                not in CAPACITY_ERROR_CODES
            ):
                raise RuntimeError(
                    "Upload AllDebrid "
                    f"invalide après recovery: "
                    f"{body.get('error')}"
                )

        # Ici, notre propre queue ne suffit pas
        # à libérer la capacité.
        #
        # Probable activité externe au broker
        # ou compte réellement saturé.
        print(
            "[BROKER]"
            "[CAPACITY-RECOVERY-FAILED] "
            f"code={code} "
            f"batch={len(batch)}"
        )

        raise HTTPException(
            status_code=503,
            detail=code,
        )



async def check_alldebrid(
    hashes: list[str],
) -> tuple[
    set[str],
    set[str],
    int,
]:

    global blocked_until
    global foreground_active

    cached: set[str] = set()
    uncached: set[str] = set()

    if not hashes:
        return (
            cached,
            uncached,
            0,
        )

    if (
        time.monotonic()
        < blocked_until
    ):
        raise HTTPException(
            status_code=503,
            detail=(
                "AllDebrid broker "
                "backoff actif"
            ),
        )

    # Si le cleanup s'accumule anormalement,
    # on purge avant de créer davantage de
    # magnets temporaires.
    pending_cleanup = (
        await cleanup_queue_size()
    )

    if (
        pending_cleanup
        >= BROKER_CLEANUP_HIGH_WATER
    ):
        print(
            "[BROKER]"
            "[CLEANUP-HIGH-WATER] "
            f"pending={pending_cleanup}"
        )

        await drain_cleanup_queue(
            max_items=60,
            force=True,
        )

    async with foreground_lock:
        foreground_active += 1

    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(25.0)
        ) as client:

            try:
                snapshot = (
                    await status_snapshot(
                        client
                    )
                )

            except HTTPException:
                blocked_until = (
                    time.monotonic()
                    + BACKOFF_SECONDS
                )
                raise

            except Exception as exc:
                print(
                    "[BROKER]"
                    "[STATUS-ERROR] "
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )

                raise HTTPException(
                    status_code=502,
                    detail=(
                        "Status AllDebrid "
                        "indisponible"
                    ),
                )

            existing_ids: set[int] = set()

            wanted = set(
                hashes
            )

            for item in snapshot:

                magnet_id = (
                    item.get("id")
                )

                if magnet_id is not None:
                    try:
                        existing_ids.add(
                            int(
                                magnet_id
                            )
                        )
                    except (
                        TypeError,
                        ValueError,
                    ):
                        pass

                h = str(
                    item.get("hash")
                    or ""
                ).strip().lower()

                if (
                    not HASH_RE.fullmatch(h)
                    or h not in wanted
                ):
                    continue

                if (
                    item.get("statusCode")
                    == 4
                ):
                    cached.add(h)

                else:
                    uncached.add(h)

            status_hits = (
                len(cached)
                + len(uncached)
            )

            unknown = [
                h
                for h in hashes
                if (
                    h not in cached
                    and h not in uncached
                )
            ]

            if not unknown:
                return (
                    cached,
                    uncached,
                    status_hits,
                )

            if (
                len(existing_ids)
                >= CAPACITY_GUARD
            ):
                blocked_until = (
                    time.monotonic()
                    + BACKOFF_SECONDS
                )

                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Capacité magnets "
                        "AllDebrid élevée"
                    ),
                )

            batches = [
                unknown[
                    i:
                    i + BATCH_SIZE
                ]
                for i in range(
                    0,
                    len(unknown),
                    BATCH_SIZE,
                )
            ]

            async def upload_batch(
                batch: list[str],
            ):
                (
                    body,
                    elapsed,
                    capacity_recovered,
                ) = await upload_with_capacity_recovery(
                    client,
                    batch,
                )

                batch_cached: set[str] = set()
                batch_uncached: set[str] = set()
                created_ids: list[int] = []

                magnets = (
                    body.get("data", {})
                    .get("magnets", [])
                    or []
                )

                seen: set[str] = set()

                for item in magnets:

                    h = str(
                        item.get("hash")
                        or ""
                    ).strip().lower()

                    if not HASH_RE.fullmatch(
                        h
                    ):
                        continue

                    seen.add(h)

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
                                not in
                                existing_ids
                            ):
                                created_ids.append(
                                    numeric_id
                                )

                        except (
                            TypeError,
                            ValueError,
                        ):
                            pass

                    if (
                        item.get("ready")
                        is True
                    ):
                        batch_cached.add(h)

                    else:
                        batch_uncached.add(h)

                # Tout hash réellement demandé doit être
                # classé explicitement.
                for h in batch:
                    if h not in seen:
                        batch_uncached.add(h)

                # IMPORTANT :
                # les IDs sont rendus persistants AVANT
                # que la réponse puisse remonter.
                if created_ids:
                    await enqueue_cleanup_ids(
                        created_ids
                    )

                print(
                    "[BROKER][UPLOAD] "
                    f"checked={len(batch)} "
                    f"cached="
                    f"{len(batch_cached)} "
                    f"cleanup_queued="
                    f"{len(created_ids)} "
                    f"capacity_recovered="
                    f"{capacity_recovered} "
                    f"time={elapsed:.3f}s"
                )

                return (
                    batch_cached,
                    batch_uncached,
                )

            # Comme Stream-Fusion :
            # plusieurs batches d'upload en parallèle.
            for pos in range(
                0,
                len(batches),
                PARALLEL_BATCHES,
            ):
                wave = batches[
                    pos:
                    pos + PARALLEL_BATCHES
                ]

                results = (
                    await asyncio.gather(
                        *[
                            upload_batch(batch)
                            for batch in wave
                        ]
                    )
                )

                for (
                    batch_cached,
                    batch_uncached,
                ) in results:

                    cached.update(
                        batch_cached
                    )

                    uncached.update(
                        batch_uncached
                    )

            return (
                cached,
                uncached,
                status_hits,
            )

    except HTTPException:
        raise

    except Exception as exc:
        print(
            "[BROKER]"
            "[AVAILABILITY-ERROR] "
            f"{type(exc).__name__}: "
            f"{exc}"
        )

        raise HTTPException(
            status_code=502,
            detail=(
                "Contrôle AllDebrid "
                "impossible"
            ),
        )

    finally:
        async with foreground_lock:
            foreground_active = max(
                0,
                foreground_active - 1,
            )


async def process_hashes(
    hashes: list[str],
) -> CheckResponse:

    started = (
        time.perf_counter()
    )

    normalized = normalize_hashes(
        hashes
    )

    # 1 + 2. UN SEUL MGET :
    #       cache broker + Redis Stream-Fusion.
    (
        redis_hits,
        sf_redis_hits,
    ) = await redis_positive_hits_combined(
        normalized
    )

    remaining = [
        h
        for h in normalized
        if (
            h not in redis_hits
            and h not in sf_redis_hits
        )
    ]

    # 3. Cache positif PostgreSQL Stream-Fusion.
    pg_hits = (
        await postgres_positive_hits(
            remaining
        )
    )

    remaining = [
        h
        for h in remaining
        if h not in pg_hits
    ]

    cached = (
        redis_hits
        | sf_redis_hits
        | pg_hits
    )

    uncached: set[str] = set()

    status_hits = 0

    # 3. Seulement les inconnus vont chez AD.
    if remaining:
        (
            ad_cached,
            ad_uncached,
            status_hits,
        ) = await check_alldebrid(
            remaining
        )

        cached.update(
            ad_cached
        )

        uncached.update(
            ad_uncached
        )

        # POSITIFS UNIQUEMENT.
        await remember_positive(
            ad_cached
        )

    elapsed = (
        time.perf_counter()
        - started
    )

    print(
        "[BROKER][DONE] "
        f"requested={len(hashes)} "
        f"unique={len(normalized)} "
        f"redis_hits="
        f"{len(redis_hits)} "
        f"sf_redis_hits="
        f"{len(sf_redis_hits)} "
        f"pg_hits={len(pg_hits)} "
        f"status_hits="
        f"{status_hits} "
        f"ad_checked="
        f"{len(remaining)} "
        f"cached={len(cached)} "
        f"time={elapsed:.3f}s"
    )

    return CheckResponse(
        cached=sorted(
            cached
        ),
        uncached=sorted(
            uncached
        ),
        requested=len(
            hashes
        ),
        unique=len(
            normalized
        ),
        redis_hits=len(
            redis_hits
        ),
        pg_hits=len(
            pg_hits
        ),
        status_hits=status_hits,
        checked_alldebrid=len(
            remaining
        ),
        elapsed=elapsed,
    )



async def process_hashes_singleflight(
    hashes: list[str],
) -> CheckResponse:

    started = time.perf_counter()

    normalized = normalize_hashes(
        hashes
    )

    if not normalized:
        return CheckResponse(
            cached=[],
            uncached=[],
            requested=len(hashes),
            unique=0,
            redis_hits=0,
            pg_hits=0,
            status_hits=0,
            checked_alldebrid=0,
            elapsed=0.0,
        )

    # -------------------------------------------------
    # 1 + 2. UN SEUL MGET :
    #        cache broker + Redis Stream-Fusion
    # -------------------------------------------------

    (
        redis_hits,
        sf_redis_hits,
    ) = await redis_positive_hits_combined(
        normalized
    )

    remaining = [
        h
        for h in normalized
        if (
            h not in redis_hits
            and h not in sf_redis_hits
        )
    ]

    # -------------------------------------------------
    # 3. Cache positif PostgreSQL Stream-Fusion
    # -------------------------------------------------

    pg_hits = (
        await postgres_positive_hits(
            remaining
        )
    )

    unresolved = [
        h
        for h in remaining
        if h not in pg_hits
    ]

    cached = (
        set(redis_hits)
        | set(sf_redis_hits)
        | set(pg_hits)
    )

    uncached: set[str] = set()

    status_hits = 0

    # hash -> Future partagé entre requêtes.
    futures: dict[
        str,
        asyncio.Future,
    ] = {}

    owners: list[str] = []

    loop = asyncio.get_running_loop()

    # -------------------------------------------------
    # 3. Attribution single-flight
    # -------------------------------------------------

    async with INFLIGHT_LOCK:

        for h in unresolved:

            future = INFLIGHT.get(h)

            if future is None:
                future = (
                    loop.create_future()
                )

                INFLIGHT[h] = future

                owners.append(h)

            futures[h] = future

    followers = (
        len(unresolved)
        - len(owners)
    )

    print(
        "[BROKER][SINGLEFLIGHT] "
        f"requested={len(normalized)} "
        f"cache_hits="
        f"{len(redis_hits)+len(sf_redis_hits)+len(pg_hits)} "
        f"owners={len(owners)} "
        f"followers={followers}"
    )

    # -------------------------------------------------
    # 4. Seuls les propriétaires appellent AllDebrid.
    # -------------------------------------------------

    if owners:

        owner_cached: set[str] = set()
        owner_uncached: set[str] = set()

        owner_error: Exception | None = None

        try:

            async with AD_CONCURRENCY:

                (
                    owner_cached,
                    owner_uncached,
                    status_hits,
                ) = await check_alldebrid(
                    owners
                )

            classified = (
                owner_cached
                | owner_uncached
            )

            if (
                classified
                != set(owners)
            ):
                raise RuntimeError(
                    "Classification AllDebrid "
                    "incomplète "
                    f"{len(classified)}/"
                    f"{len(owners)}"
                )

            await remember_positive(
                owner_cached
            )

        except Exception as exc:
            owner_error = exc

        # Publie le résultat aux requêtes abonnées.
        async with INFLIGHT_LOCK:

            for h in owners:

                future = INFLIGHT.get(h)

                if future is None:
                    continue

                if not future.done():

                    if owner_error is not None:
                        future.set_exception(
                            owner_error
                        )

                    else:
                        future.set_result(
                            h in owner_cached
                        )

    # -------------------------------------------------
    # 5. Chaque requête attend uniquement les futures
    #    correspondant à ses hashes.
    # -------------------------------------------------

    if unresolved:

        values = await asyncio.gather(
            *[
                futures[h]
                for h in unresolved
            ],
            return_exceptions=True,
        )

        errors: list[str] = []

        for h, value in zip(
            unresolved,
            values,
        ):

            if isinstance(
                value,
                BaseException,
            ):
                errors.append(
                    f"{type(value).__name__}: "
                    f"{value}"
                )

                continue

            if value is True:
                cached.add(h)

            elif value is False:
                uncached.add(h)

            else:
                errors.append(
                    "classification "
                    f"invalide pour {h}"
                )

        if errors:
            raise HTTPException(
                status_code=502,
                detail=(
                    "Contrôle AllDebrid "
                    "single-flight incomplet"
                ),
            )

    # -------------------------------------------------
    # 6. Nettoyage du registre seulement par
    #    le propriétaire des futures.
    # -------------------------------------------------

    if owners:

        async with INFLIGHT_LOCK:

            for h in owners:

                current = (
                    INFLIGHT.get(h)
                )

                if (
                    current
                    is futures.get(h)
                ):
                    INFLIGHT.pop(
                        h,
                        None,
                    )

    elapsed = (
        time.perf_counter()
        - started
    )

    print(
        "[BROKER][DONE-SINGLEFLIGHT] "
        f"requested={len(hashes)} "
        f"unique={len(normalized)} "
        f"redis_hits={len(redis_hits)} "
        f"sf_redis_hits={len(sf_redis_hits)} "
        f"pg_hits={len(pg_hits)} "
        f"owners={len(owners)} "
        f"followers={followers} "
        f"cached={len(cached)} "
        f"uncached={len(uncached)} "
        f"time={elapsed:.3f}s"
    )

    return CheckResponse(
        cached=sorted(
            cached
        ),
        uncached=sorted(
            uncached
        ),
        requested=len(
            hashes
        ),
        unique=len(
            normalized
        ),
        redis_hits=len(
            redis_hits
        ),
        pg_hits=len(
            pg_hits
        ),
        status_hits=status_hits,
        checked_alldebrid=len(
            owners
        ),
        elapsed=elapsed,
    )


async def broker_worker() -> None:

    while True:
        first = await queue.get()

        group = [
            first
        ]

        if COALESCE_MS > 0:
            await asyncio.sleep(
                COALESCE_MS
                / 1000.0
            )

        while True:
            try:
                group.append(
                    queue.get_nowait()
                )

            except asyncio.QueueEmpty:
                break

        union: list[str] = []
        seen: set[str] = set()

        raw_count = 0

        for item in group:
            raw_count += len(
                item.hashes
            )

            for h in normalize_hashes(
                item.hashes
            ):
                if h in seen:
                    continue

                seen.add(h)
                union.append(h)

        print(
            "[BROKER][COALESCE] "
            f"requests={len(group)} "
            f"raw_hashes={raw_count} "
            f"unique_hashes="
            f"{len(union)} "
            f"saved="
            f"{raw_count-len(union)}"
        )

        try:
            result = (
                await process_hashes(
                    union
                )
            )

            cached_set = set(
                result.cached
            )

            uncached_set = set(
                result.uncached
            )

            for item in group:
                wanted = set(
                    normalize_hashes(
                        item.hashes
                    )
                )

                if (
                    not
                    item.future.done()
                ):
                    item.future.set_result(
                        CheckResponse(
                            cached=sorted(
                                wanted
                                & cached_set
                            ),
                            uncached=sorted(
                                wanted
                                & uncached_set
                            ),
                            requested=len(
                                item.hashes
                            ),
                            unique=len(
                                wanted
                            ),
                            redis_hits=(
                                result.redis_hits
                            ),
                            pg_hits=(
                                result.pg_hits
                            ),
                            status_hits=(
                                result.status_hits
                            ),
                            checked_alldebrid=(
                                result
                                .checked_alldebrid
                            ),
                            elapsed=(
                                result.elapsed
                            ),
                        )
                    )

        except Exception as exc:
            for item in group:
                if (
                    not
                    item.future.done()
                ):
                    item.future.set_exception(
                        exc
                    )

        finally:
            for _ in group:
                queue.task_done()


@app.on_event("startup")


async def startup() -> None:
    global cleanup_worker_task

    cleanup_worker_task = (
        asyncio.create_task(
            cleanup_worker()
        )
    )

    pending = (
        await cleanup_queue_size()
    )

    print(
        "[BROKER][START] "
        f"mode=singleflight "
        f"rate={RATE_PER_SECOND}/s "
        f"batch={BATCH_SIZE} "
        f"parallel_batches="
        f"{PARALLEL_BATCHES} "
        f"parallel_ad=2 "
        f"positive_ttl="
        f"{POSITIVE_TTL}s "
        f"cleanup_pending="
        f"{pending}"
    )


@app.on_event("shutdown")


async def shutdown() -> None:
    global cleanup_worker_task

    if cleanup_worker_task:
        cleanup_worker_task.cancel()

        try:
            await cleanup_worker_task

        except asyncio.CancelledError:
            pass

    await redis_client.aclose()
    await engine.dispose()


@app.get("/health")
async def health():

    redis_ok = False
    postgres_ok = False

    try:
        redis_ok = bool(
            await redis_client.ping()
        )

    except Exception:
        pass

    try:
        async with engine.connect() as conn:
            await conn.execute(
                text("SELECT 1")
            )

        postgres_ok = True

    except Exception:
        pass

    return {
        "status": (
            "ok"
            if (
                redis_ok
                and postgres_ok
                and bool(
                    ALLDEBRID_API_KEY
                )
            )
            else "degraded"
        ),
        "redis": redis_ok,
        "postgres": postgres_ok,
        "alldebrid_key":
            bool(
                ALLDEBRID_API_KEY
            ),
        "rate_per_second":
            RATE_PER_SECOND,
        "batch_size":
            BATCH_SIZE,
        "coalesce_ms":
            COALESCE_MS,
        "positive_ttl":
            POSITIVE_TTL,
        "queue":
            queue.qsize(),
        "cleanup_queue":
            await cleanup_queue_size(),
    }


@app.post(
    "/v1/check",
    response_model=CheckResponse,
)

async def check(
    request: CheckRequest,
    authorization: str | None = Header(
        default=None
    ),
):

    expected = (
        "Bearer "
        + BROKER_TOKEN
    )

    if (
        not authorization
        or not hmac.compare_digest(
            authorization,
            expected,
        )
    ):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
        )

    hashes = normalize_hashes(
        request.hashes
    )

    if not hashes:
        return CheckResponse(
            cached=[],
            uncached=[],
            requested=len(
                request.hashes
            ),
            unique=0,
            redis_hits=0,
            pg_hits=0,
            status_hits=0,
            checked_alldebrid=0,
            elapsed=0.0,
        )

    return (
        await
        process_hashes_singleflight(
            hashes
        )
    )
