"""
Calls one endpoint of a live balance_tracker through the installed hiveio-balance-api wheel and the transport of its
runtime package (hiveio_api_client), and checks the response is parsed into the generated model.

Usage: python3 call_endpoint.py [<endpoint URL> ...] - the first endpoint that answers is used; without arguments
the public API nodes are tried.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Final

from hiveio_api_client import AsyncRemoteHandle, HttpUrl, RemoteHandleSettings
from hiveio_api_client.exceptions import CommunicationError, HttpStatusError
from hiveio_balance_api.balance_api_client.balance_api_client import BalanceApi

_PUBLIC_ENDPOINTS: Final[list[str]] = ["https://api.syncad.com", "https://api.hive.blog"]
_ACCOUNT: Final[str] = "gtg"


async def _call_delegations(endpoints: list[str]) -> object:
    failures = []
    for endpoint in endpoints:
        handle = AsyncRemoteHandle(RemoteHandleSettings(http_endpoint=HttpUrl(endpoint), max_retries=2), api=BalanceApi)
        try:
            return await handle.api.accounts_delegations(_ACCOUNT)
        except (CommunicationError, HttpStatusError) as error:
            print(f"{endpoint} did not answer: {error}", file=sys.stderr)
            failures.append(endpoint)
    raise SystemExit(f"No endpoint answered: {failures}")


async def main() -> None:
    result = await _call_delegations(sys.argv[1:] or _PUBLIC_ENDPOINTS)
    print(f"{type(result).__name__}: {result}")
    for field in ("incoming_delegations", "outgoing_delegations"):
        if not isinstance(getattr(result, field, None), list):
            raise SystemExit(f"Expected a parsed response with the list {field}, got {result!r}")


if __name__ == "__main__":
    asyncio.run(main())
