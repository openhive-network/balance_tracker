"""PostgREST's own parameters keep their meaning through the rewriter.

The rewriter's hive.rpc_form module moves into a form body only the parameters the called
function declares (endpoints/rpc_arguments.lua) and keeps every other one in the URL: column
filters, select, order, limit, offset. So they still shape the result instead of becoming
unknown arguments.
"""
import http.client
import json
import pathlib
import re
from typing import Any
from urllib.parse import urlsplit

from common import REWRITER, TIMEOUT_S, direct_get, direct_post, postgrest_code, rewriter_post, via_rewriter

ENDPOINTS = pathlib.Path(__file__).resolve().parents[2] / "endpoints"

# A row-returning endpoint (SETOF) with a handful of rows on the 5M-block lane.
ROWS_PATH = "/accounts/blocktrades/aggregated-history"
ROWS_ARGS = {"coin-type": "VESTS", "granularity": "monthly", "direction": "asc"}
ROWS_FUNCTION_ARGS = {"account-name": "blocktrades", **ROWS_ARGS}


def _rows(ans: tuple[int, Any]) -> list[Any]:
    status, body = ans
    assert status == 200, ans
    assert isinstance(body, list), ans
    return body


def _full() -> list[Any]:
    full = _rows(via_rewriter(ROWS_PATH, ROWS_ARGS))
    assert len(full) >= 3, full
    return full


def test_argument_map_matches_endpoint_signatures() -> None:
    """A parameter the map misses would stay in the URL, where PostgREST reads an operator-
    prefixed value as a filter. The shipped map must name every btracker_endpoints function
    a REST path reaches with exactly its SQL parameters (all overloads together)."""
    signature = re.compile(r"CREATE OR REPLACE FUNCTION btracker_endpoints\.(\w+)\s*\((.*?)\)\s*RETURNS",
                           re.S | re.I)
    declared: dict[str, set[str]] = {}
    for sql in ENDPOINTS.rglob("*.sql"):
        for fn, params in signature.findall(sql.read_text(encoding="utf-8")):
            declared.setdefault(fn, set()).update(re.findall(r'"([^"]+)"', re.sub(r"--[^\n]*", "", params)))
    # root() serves the OpenAPI document at /, a call without arguments.
    declared.pop("root", None)
    shipped = {fn: set(re.findall(r'\["([^"]+)"\] = true', fields))
               for fn, fields in re.findall(r'^\s*\["([^"]+)"\] = \{(.*)\},$',
                                            (ENDPOINTS / "rpc_arguments.lua").read_text(encoding="utf-8"), re.M)}
    assert len(declared) >= 15, f"found only {len(declared)} endpoint functions under {ENDPOINTS}"
    assert shipped == declared


def test_limit_trims_rows():
    full = _full()
    got = via_rewriter(ROWS_PATH, {**ROWS_ARGS, "limit": "2"})
    assert _rows(got) == full[:2]
    assert got == direct_post("get_balance_aggregation", ROWS_FUNCTION_ARGS, query={"limit": "2"})


def test_offset_and_limit():
    full = _full()
    got = via_rewriter(ROWS_PATH, {**ROWS_ARGS, "offset": "1", "limit": "2"})
    assert _rows(got) == full[1:3]
    assert got == direct_post("get_balance_aggregation", ROWS_FUNCTION_ARGS, query={"offset": "1", "limit": "2"})


def test_order_and_select():
    full = _full()
    got = via_rewriter(ROWS_PATH, {**ROWS_ARGS, "select": "date", "order": "date.desc", "limit": "2"})
    want = [{"date": r["date"]} for r in sorted(full, key=lambda r: r["date"], reverse=True)[:2]]
    assert _rows(got) == want
    assert got == direct_post("get_balance_aggregation", ROWS_FUNCTION_ARGS,
                              query={"select": "date", "order": "date.desc", "limit": "2"})


def test_column_filter_narrows_rows() -> None:
    """A column filter is no argument of the function: it stays in the URL and PostgREST
    filters the function's rows by it."""
    full = _full()
    since = full[len(full) // 2]["date"]
    query = {"date": f"gte.{since}"}
    got = via_rewriter(ROWS_PATH, {**ROWS_ARGS, **query})
    assert _rows(got) == [r for r in full if r["date"] >= since]
    assert 0 < len(got[1]) < len(full)
    assert got == direct_post("get_balance_aggregation", ROWS_FUNCTION_ARGS, query=query)


def test_negated_column_filter_beside_operator_prefixed_argument() -> None:
    """`not.` means a filter in a column filter and a plain value in an argument, in one call."""
    full = _full()
    first = full[0]["date"]
    got = via_rewriter(ROWS_PATH, {**ROWS_ARGS, "date": f"not.eq.{first}"})
    assert _rows(got) == full[1:]
    got = via_rewriter("/accounts/not.blocktrades/aggregated-history", {**ROWS_ARGS, "date": f"not.eq.{first}"})
    want = direct_post("get_balance_aggregation", {**ROWS_FUNCTION_ARGS, "account-name": "not.blocktrades"},
                       query={"date": f"not.eq.{first}"})
    assert postgrest_code(want) not in ("PGRST100", "PGRST202"), want
    assert got == want


def test_head_then_get_on_one_connection() -> None:
    """A HEAD answers with the GET's status and headers and no body, so the GET after it on
    the same keep-alive connection reads its own answer."""
    url = urlsplit(REWRITER)
    version = via_rewriter("/version")
    conn = http.client.HTTPConnection(url.hostname or "localhost", url.port or 80, timeout=TIMEOUT_S)
    try:
        for account in ("blocktrades", "not.blocktrades"):
            path = f"{url.path}/accounts/{account}/balances"
            want_status, want_body = via_rewriter(f"/accounts/{account}/balances")
            assert postgrest_code((want_status, want_body)) not in ("PGRST100", "PGRST202"), want_body
            conn.request("HEAD", path)
            head = conn.getresponse()
            assert head.status == want_status
            assert head.read() == b""
            conn.request("GET", f"{url.path}/version")
            get = conn.getresponse()
            assert (get.status, json.loads(get.read())) == version
    finally:
        conn.close()


def test_value_with_a_space():
    """A timestamp from-block carries a space, sent as `+`: in the form body PostgREST must
    decode it back, as it does in a query string."""
    args = {**ROWS_ARGS, "from-block": "2016-06-01 00:00:00"}
    got = via_rewriter(ROWS_PATH, args)
    assert _rows(got), got
    assert got == direct_post("get_balance_aggregation", {**ROWS_FUNCTION_ARGS, **args})
    assert got != via_rewriter(ROWS_PATH, ROWS_ARGS)


def test_limit_on_the_catch_all_route():
    """The catch-all rule (/<function>) splits the same way: arguments to the body, limit to the URL."""
    got = via_rewriter("/get_balance_aggregation", {**ROWS_FUNCTION_ARGS, "limit": "1"})
    assert _rows(got) == _full()[:1]


def test_limit_beside_operator_prefixed_argument():
    got = via_rewriter("/accounts/not.existing/aggregated-history", {**ROWS_ARGS, "limit": "1"})
    want = direct_post("get_balance_aggregation", {**ROWS_FUNCTION_ARGS, "account-name": "not.existing"},
                       query={"limit": "1"})
    assert postgrest_code(want) not in ("PGRST100", "PGRST202"), want
    assert got == want


def test_calls_without_arguments_and_json_posts_unchanged():
    assert via_rewriter("/version") == direct_post("get_btracker_version", {})
    assert via_rewriter("/last-synced-block") == direct_get("get_btracker_last_synced_block", "")
    # Only GETs are converted: a JSON POST goes through as sent.
    args = {"account-name": "eq.blocktrades"}
    got = rewriter_post("/get_account_balances", args)
    want = direct_post("get_account_balances", args)
    assert postgrest_code(want) not in ("PGRST100", "PGRST202"), want
    assert got == want
