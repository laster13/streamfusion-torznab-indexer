"""
Quota global AllDebrid partagé entre processus/containers.

Les compteurs utilisent Redis TIME et un ZSET commun afin que
tous les consommateurs connus du compte respectent exactement
le même budget.

Protection volontairement plus stricte que les limites AllDebrid :
    8 requêtes / seconde
    450 requêtes / minute

Redis indisponible => fail closed : aucune requête AllDebrid
n'est émise sans pouvoir réserver son quota.
"""

from __future__ import annotations

import asyncio
import os

import redis.asyncio as redis


GLOBAL_SECOND_LIMIT = max(
    1,
    int(
        os.getenv(
            "ALLDEBRID_GLOBAL_SECOND_LIMIT",
            "8",
        )
    ),
)

GLOBAL_MINUTE_LIMIT = max(
    GLOBAL_SECOND_LIMIT,
    int(
        os.getenv(
            "ALLDEBRID_GLOBAL_MINUTE_LIMIT",
            "450",
        )
    ),
)

REDIS_URL = (
    os.getenv(
        "ALLDEBRID_GLOBAL_REDIS_URL",
        "",
    ).strip()
    or os.getenv(
        "REDIS_URL",
        "",
    ).strip()
    or "redis://sfr-redis-dev:6379/0"
)

REQUEST_KEY = (
    "sf:alldebrid:global-quota:requests"
)

SEQUENCE_KEY = (
    "sf:alldebrid:global-quota:sequence"
)


redis_client = redis.from_url(
    REDIS_URL,
    encoding="utf-8",
    decode_responses=True,
    socket_connect_timeout=2.0,
    socket_timeout=2.0,
)


_ACQUIRE_SCRIPT = r"""
local key = KEYS[1]
local seqkey = KEYS[2]

local second_limit = tonumber(ARGV[1])
local minute_limit = tonumber(ARGV[2])

local t = redis.call("TIME")
local now = (
    tonumber(t[1]) * 1000
    + math.floor(tonumber(t[2]) / 1000)
)

redis.call(
    "ZREMRANGEBYSCORE",
    key,
    "-inf",
    now - 60000
)

local second_count = redis.call(
    "ZCOUNT",
    key,
    "(" .. tostring(now - 1000),
    "+inf"
)

local minute_count = redis.call(
    "ZCARD",
    key
)

if (
    second_count < second_limit
    and minute_count < minute_limit
) then

    local seq = redis.call(
        "INCR",
        seqkey
    )

    local member = (
        tostring(now)
        .. "-"
        .. tostring(seq)
    )

    redis.call(
        "ZADD",
        key,
        now,
        member
    )

    redis.call(
        "PEXPIRE",
        key,
        70000
    )

    redis.call(
        "PEXPIRE",
        seqkey,
        70000
    )

    return {
        1,
        0,
        second_count + 1,
        minute_count + 1
    }
end

local wait_ms = 1

if second_count >= second_limit then

    local oldest_second = redis.call(
        "ZRANGEBYSCORE",
        key,
        "(" .. tostring(now - 1000),
        "+inf",
        "WITHSCORES",
        "LIMIT",
        0,
        1
    )

    if #oldest_second >= 2 then
        local candidate = (
            tonumber(oldest_second[2])
            + 1000
            - now
            + 1
        )

        if candidate > wait_ms then
            wait_ms = candidate
        end
    end
end

if minute_count >= minute_limit then

    local oldest_minute = redis.call(
        "ZRANGE",
        key,
        0,
        0,
        "WITHSCORES"
    )

    if #oldest_minute >= 2 then
        local candidate = (
            tonumber(oldest_minute[2])
            + 60000
            - now
            + 1
        )

        if candidate > wait_ms then
            wait_ms = candidate
        end
    end
end

return {
    0,
    wait_ms,
    second_count,
    minute_count
}
"""


async def acquire_alldebrid_global_quota() -> None:
    """
    Attend atomiquement une place dans le quota global.

    IMPORTANT :
    une place n'est enregistrée qu'au moment où elle est
    réellement accordée.

    Si Redis est indisponible, on échoue fermé afin de ne
    jamais contourner silencieusement la protection globale.
    """

    while True:

        try:
            result = await redis_client.eval(
                _ACQUIRE_SCRIPT,
                2,
                REQUEST_KEY,
                SEQUENCE_KEY,
                GLOBAL_SECOND_LIMIT,
                GLOBAL_MINUTE_LIMIT,
            )

        except Exception as exc:
            raise RuntimeError(
                "Quota global AllDebrid indisponible : "
                "Redis inaccessible"
            ) from exc

        if (
            isinstance(result, (list, tuple))
            and len(result) >= 2
        ):
            granted = int(result[0])
            wait_ms = max(
                1,
                int(result[1]),
            )

            if granted == 1:
                return

            await asyncio.sleep(
                wait_ms / 1000.0
            )

            continue

        raise RuntimeError(
            "Réponse Redis invalide pour "
            "le quota global AllDebrid"
        )
