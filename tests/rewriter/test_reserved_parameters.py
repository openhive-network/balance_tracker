"""PostgREST's own parameters keep their meaning through the rewriter (#82).

The rewriter moves a GET's function arguments into a form body but keeps PostgREST's
reserved parameters (select, columns, on_conflict, ...limit, ...offset, ...order, ...and,
...or) in the URL, so they still shape the result instead of becoming unknown arguments.
"""
import pathlib
import re

from common import direct_get, direct_post, postgrest_code, rewriter_post, via_rewriter

ENDPOINTS = pathlib.Path(__file__).resolve().parents[2] / "endpoints"

# A row-returning endpoint (SETOF) with a handful of rows on the 5M-block lane.
ROWS_PATH = "/accounts/blocktrades/aggregated-history"
ROWS_ARGS = {"coin-type": "VESTS", "granularity": "monthly", "direction": "asc"}
ROWS_FUNCTION_ARGS = {"account-name": "blocktrades", **ROWS_ARGS}


def _rows(ans):
    status, body = ans
    assert status == 200, ans
    assert isinstance(body, list), ans
    return body


def _full():
    full = _rows(via_rewriter(ROWS_PATH, ROWS_ARGS))
    assert len(full) >= 3, full
    return full


def test_no_endpoint_argument_has_a_reserved_name():
    """The rewriter keeps these names in the URL, so a function argument with one of them
    would never reach the function. Checked against every btracker_endpoints signature."""
    reserved = {"select", "columns", "on_conflict"}
    reserved_last = {"limit", "offset", "order", "and", "or"}
    signature = re.compile(r"CREATE OR REPLACE FUNCTION btracker_endpoints\.(\w+)\s*\((.*?)\)\s*RETURNS",
                           re.S | re.I)
    seen, bad = 0, []
    for sql in ENDPOINTS.rglob("*.sql"):
        for fn, params in signature.findall(sql.read_text(encoding="utf-8")):
            seen += 1
            for name in re.findall(r'"([^"]+)"', re.sub(r"--[^\n]*", "", params)):
                if name in reserved or name.rsplit(".", 1)[-1] in reserved_last:
                    bad.append(f"{fn}({name})")
    assert seen >= 15, f"found only {seen} endpoint signatures under {ENDPOINTS}"
    assert not bad, bad


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
