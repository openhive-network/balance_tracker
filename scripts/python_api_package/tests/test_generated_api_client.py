from __future__ import annotations

from typing import Final

from tests.api_caller import HafbeApiCaller

from beekeepy._communication.url import HttpUrl

DEFAULT_ENDPOINT_FOR_TESTS: Final[HttpUrl] = HttpUrl("https://api.syncad.com")
SEARCHED_ACCOUNT_IN_TESTS: Final[str] = "gtg"

async def test_generated_api_client():
    # ARRANGE
    api_caller = HafbeApiCaller(endpoint_url=DEFAULT_ENDPOINT_FOR_TESTS)

    # ACT
    async with api_caller as api:
        result = await api.api.balance_api.accounts_delegations(SEARCHED_ACCOUNT_IN_TESTS)

    # ASSERT
    assert isinstance(result.incoming_delegations, list), "Expected incoming_delegations to be a list"
    assert isinstance(result.outgoing_delegations, list), "Expected outgoing_delegations to be a list"
