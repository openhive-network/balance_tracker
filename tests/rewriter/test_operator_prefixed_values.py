"""A REST value that starts with a PostgREST operator prefix reaches its function verbatim (#82).

PostgREST reads such a GET argument as a filter (PGRST100, or the argument dropped and
PGRST202). The rewriter forwards GETs as form POSTs, so each case here must answer through
the rewriter exactly as a direct JSON POST of the same arguments does.

Every TEXT argument of every endpoint is covered: the account names the rewrite rules take
from the path, and every TEXT query parameter. Equality with the oracle proves the value was
read as an argument; test_value_arrives_unchanged checks the value itself, through the
"Account '<name>' does not exist" answer that echoes it.
"""
import pytest

from common import direct_post, postgrest_code, via_rewriter

VALUES = ["not.existing", "eq.blocktrades", "in.(gtg)", "like.gtg*", "is.null", "fts.gtg"]

# (public path with {v} where the value goes in the path, function, its fixed arguments,
#  the argument {v} is, and whether {v} is a query parameter instead of a path segment)
PATH, QUERY = False, True
VESTS = {"coin-type": "VESTS"}
HIVE = {"coin-type": "HIVE"}
CASES = [
    ("/accounts/{v}/balances", "get_account_balances", {}, "account-name", PATH),
    ("/accounts/{v}/delegations", "get_balance_delegations", {}, "account-name", PATH),
    ("/accounts/{v}/rc-delegations", "get_rc_delegations", {}, "account-name", PATH),
    ("/accounts/{v}/recurrent-transfers", "get_recurrent_transfers", {}, "account-name", PATH),
    ("/accounts/{v}/balance-history", "get_balance_history", VESTS, "account-name", PATH),
    ("/accounts/{v}/aggregated-history", "get_balance_aggregation", VESTS, "account-name", PATH),
    ("/accounts/{v}/vesting-history", "get_account_vesting_history", {}, "account-name", PATH),
    ("/accounts/{v}/vesting-stats", "get_account_vesting_stats", {}, "account-name", PATH),
    ("/accounts/blocktrades/balance-history", "get_balance_history", {"account-name": "blocktrades", **VESTS}, "from-block", QUERY),
    ("/accounts/blocktrades/balance-history", "get_balance_history", {"account-name": "blocktrades", **VESTS}, "to-block", QUERY),
    ("/accounts/blocktrades/aggregated-history", "get_balance_aggregation", {"account-name": "blocktrades", **VESTS}, "from-block", QUERY),
    ("/accounts/blocktrades/aggregated-history", "get_balance_aggregation", {"account-name": "blocktrades", **VESTS}, "to-block", QUERY),
    ("/accounts/blocktrades/vesting-history", "get_account_vesting_history", {"account-name": "blocktrades"}, "from-block", QUERY),
    ("/accounts/blocktrades/vesting-history", "get_account_vesting_history", {"account-name": "blocktrades"}, "to-block", QUERY),
    ("/accounts/blocktrades/vesting-stats", "get_account_vesting_stats", {"account-name": "blocktrades"}, "from-block", QUERY),
    ("/accounts/blocktrades/vesting-stats", "get_account_vesting_stats", {"account-name": "blocktrades"}, "to-block", QUERY),
    ("/transfer-statistics", "get_transfer_statistics", HIVE, "from-block", QUERY),
    ("/transfer-statistics", "get_transfer_statistics", HIVE, "to-block", QUERY),
    ("/vesting-stats", "get_vesting_stats", {}, "from-block", QUERY),
    ("/vesting-stats", "get_vesting_stats", {}, "to-block", QUERY),
    ("/proposals/payments", "get_proposals_payments", {}, "proposal-ids", QUERY),
    # The catch-all rule (/<function> -> /rpc/<function>) with the arguments in the query string.
    ("/get_account_balances", "get_account_balances", {}, "account-name", QUERY),
]


def _id(case):
    path, _function, _fixed, arg, in_query = case
    return f"{path}?{arg}" if in_query else path


def _get(path, fixed, arg, value, in_query):
    """The public GET for a case: the fixed arguments, except account-name, which a fixed
    case always carries in its path, go in the query string, plus the value."""
    params = {k: v for k, v in fixed.items() if k != "account-name"}
    if in_query:
        params[arg] = value
        return via_rewriter(path, params)
    return via_rewriter(path.replace("{v}", value), params)


@pytest.mark.parametrize("value", VALUES)
@pytest.mark.parametrize("case", CASES, ids=_id)
def test_value_reaches_function_verbatim(case, value):
    path, function, fixed, arg, in_query = case
    got = _get(path, fixed, arg, value, in_query)
    want = direct_post(function, {**fixed, arg: value})
    # The oracle itself must have called the function with the argument.
    assert postgrest_code(want) not in ("PGRST100", "PGRST202"), want
    assert got == want


@pytest.mark.parametrize("value", VALUES)
@pytest.mark.parametrize("path", ["/accounts/{v}/balances", "/get_account_balances"])
def test_value_arrives_unchanged(path, value):
    """An unknown account is answered with its name, so the value the function received is
    visible: as a path segment and as a query parameter (the catch-all route)."""
    if "{v}" in path:
        status, body = via_rewriter(path.replace("{v}", value))
    else:
        status, body = via_rewriter(path, {"account-name": value})
    assert isinstance(body, dict), (status, body)
    assert body.get("message") == f"Account '{value}' does not exist", (status, body)
