from __future__ import annotations

from beekeepy.handle.remote import AsyncSendable

from hiveio_balance_api.balance_api_client.balance_api_client import BalanceApi


class BalanceApiCollection:
    def __init__(self, owner: AsyncSendable) -> None:
        self.balance_api = BalanceApi(owner=owner)
