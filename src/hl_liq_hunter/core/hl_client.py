"""
Async HTTP client for the Hyperliquid Info API.

Wraps the three endpoints used in Phase 1–2:
    clearinghouseState  weight=2
    candleSnapshot      weight=20 (Phase 1 simplified; refine in Phase 4)
    allMids             weight=2

Design notes
------------
- aiohttp.ClientSession is created in __aenter__ / closed in __aexit__
  so the client is always used as an async context manager.  Creating the
  session inside __aenter__ (not __init__) avoids the "attached to a
  different event loop" error when tests create fresh loops per test.
- QuotaManager.acquire() is called *before* the HTTP request so that
  quota is consumed even if the request fails — the API will have seen
  the request regardless.
- 429 → log warning + sleep 8 s + return None.  No retry: the caller
  (scanner loop) decides whether to re-enqueue the address.
- All other errors (4xx/5xx, timeout, JSON decode) → log + return None.
- No address validation here: that responsibility belongs to the registry
  layer which owns the address namespace.  The HL server validates more
  strictly than any regex we could write, and wrapping a wrong-format
  address in a clean ValueError would hide the server's actual error
  message from the logs.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional, cast

import aiohttp

from hl_liq_hunter.config import ENDPOINT_WEIGHTS, HL_INFO_URL
from hl_liq_hunter.core.quota_manager import QuotaManager

log = logging.getLogger(__name__)

_RATE_LIMIT_SLEEP_S: float = 8.0


class HLClient:
    """Async client for the Hyperliquid Info REST API."""

    def __init__(
        self,
        quota: QuotaManager,
        base_url: str = HL_INFO_URL,
    ) -> None:
        """
        Parameters
        ----------
        quota:
            Shared QuotaManager; acquire() is called before each request.
        base_url:
            Override to target testnet or a local stub server.
            Defaults to HL_INFO_URL from config.
        """
        self._quota = quota
        self._base_url = base_url
        self._session: Optional[aiohttp.ClientSession] = None

    # ── async context manager ─────────────────────────────────────────────────

    async def __aenter__(self) -> "HLClient":
        connector = aiohttp.TCPConnector(limit=50)
        timeout = aiohttp.ClientTimeout(total=10)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ── private helpers ───────────────────────────────────────────────────────

    async def _post(
        self,
        endpoint_name: str,
        payload: dict[str, object],
        weight: int,
    ) -> Optional[object]:
        """
        Acquire quota then POST *payload* to *base_url*.

        Returns the parsed JSON body on success, None on any error.
        """
        assert self._session is not None, (
            "HLClient must be used as an async context manager"
        )
        await self._quota.acquire(weight)
        try:
            async with self._session.post(
                self._base_url, json=payload
            ) as resp:
                if resp.status == 429:
                    log.warning(
                        "rate limited on %s (HTTP 429) — sleeping %.0fs",
                        endpoint_name,
                        _RATE_LIMIT_SLEEP_S,
                    )
                    await asyncio.sleep(_RATE_LIMIT_SLEEP_S)
                    return None
                if resp.status >= 400:
                    log.error(
                        "%s returned HTTP %d", endpoint_name, resp.status
                    )
                    return None
                try:
                    return cast(object, await resp.json(content_type=None))
                except (json.JSONDecodeError, Exception) as exc:
                    log.error(
                        "%s — JSON decode failed: %s", endpoint_name, exc
                    )
                    return None
        except asyncio.TimeoutError:
            log.debug("%s — request timed out", endpoint_name)
            return None
        except aiohttp.ClientError as exc:
            log.debug("%s — network error: %s", endpoint_name, exc)
            return None

    # ── public API ────────────────────────────────────────────────────────────

    async def clearinghouse_state(
        self, address: str
    ) -> Optional[dict[str, object]]:
        """
        Fetch the full clearinghouse state for *address*.

        No address-format validation is performed here; invalid addresses
        will receive an error response from the server which is logged and
        returned as None.

        Weight: 2 (confirmed — see docs/api_notes.md).
        """
        result = await self._post(
            "clearinghouseState",
            {"type": "clearinghouseState", "user": address},
            weight=ENDPOINT_WEIGHTS["clearinghouseState"],
        )
        if result is None:
            return None
        if not isinstance(result, dict):
            log.error("clearinghouseState — unexpected response type: %r", type(result))
            return None
        return result

    async def candle_snapshot(
        self,
        symbol: str,
        interval: str,
        start_ms: int,
        end_ms: int,
    ) -> Optional[list[object]]:
        """
        Fetch OHLCV candles for *symbol*.

        Weight: 20 (Phase 1 simplified; refine per candle count in Phase 4).

        Parameters
        ----------
        symbol:   e.g. "BTC", "ETH"
        interval: e.g. "1m", "5m", "1h"
        start_ms: window start, Unix ms
        end_ms:   window end, Unix ms
        """
        result = await self._post(
            "candleSnapshot",
            {
                "type": "candleSnapshot",
                "req": {
                    "coin": symbol,
                    "interval": interval,
                    "startTime": start_ms,
                    "endTime": end_ms,
                },
            },
            weight=ENDPOINT_WEIGHTS["candleSnapshot"],
        )
        if result is None:
            return None
        if not isinstance(result, list):
            log.error("candleSnapshot — unexpected response type: %r", type(result))
            return None
        return result

    async def all_mids(self) -> Optional[dict[str, float]]:
        """
        Fetch mid prices for all listed symbols.

        Weight: 2.

        Returns
        -------
        dict mapping symbol → float mid price, or None on any error.
        HL returns string prices; this method converts them to float.
        """
        result = await self._post(
            "allMids",
            {"type": "allMids"},
            weight=ENDPOINT_WEIGHTS["allMids"],
        )
        if result is None:
            return None
        if not isinstance(result, dict):
            log.error("allMids — unexpected response type: %r", type(result))
            return None
        try:
            return {k: float(v) for k, v in result.items()}
        except (ValueError, TypeError) as exc:
            log.error("allMids — price conversion failed: %s", exc)
            return None
