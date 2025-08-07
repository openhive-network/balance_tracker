from __future__ import annotations

from beekeepy._apis.abc.sendable import AsyncSendable

from balance_api.balance_api_client.balance_api_client import BalanceApi


class BalanceApiCollection:
    def __init__(self, owner: AsyncSendable) -> None:
        self.balance_api = BalanceApi(owner=owner)
