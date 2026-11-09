"""The shipped argument map (endpoints/rpc_arguments.lua) against the endpoints' SQL signatures.

Reads files only: no rewriter or PostgREST needs to run, so this also runs where no
container can (.aidev/run-checks.sh step rewriter-map).
"""
import pathlib
import re

ENDPOINTS = pathlib.Path(__file__).resolve().parents[2] / "endpoints"


def _declared() -> dict[str, set[str]]:
    signature = re.compile(r"CREATE OR REPLACE FUNCTION btracker_endpoints\.(\w+)\s*\((.*?)\)\s*RETURNS",
                           re.S | re.I)
    declared: dict[str, set[str]] = {}
    for sql in ENDPOINTS.rglob("*.sql"):
        for fn, params in signature.findall(sql.read_text(encoding="utf-8")):
            declared.setdefault(fn, set()).update(re.findall(r'"([^"]+)"', re.sub(r"--[^\n]*", "", params)))
    # root() serves the OpenAPI document at /, a call without arguments.
    declared.pop("root", None)
    return declared


def _shipped() -> dict[str, set[str]]:
    return {fn: set(re.findall(r'\["([^"]+)"\] = true', fields))
            for fn, fields in re.findall(r'^\s*\["([^"]+)"\] = \{(.*)\},$',
                                         (ENDPOINTS / "rpc_arguments.lua").read_text(encoding="utf-8"), re.M)}


def test_argument_map_matches_endpoint_signatures() -> None:
    """A parameter the map misses would stay in the URL, where PostgREST reads an operator-
    prefixed value as a filter. The shipped map must name every btracker_endpoints function
    a REST path reaches with exactly its SQL parameters (all overloads together)."""
    declared, shipped = _declared(), _shipped()
    assert len(declared) >= 15, f"found only {len(declared)} endpoint functions under {ENDPOINTS}"
    differences = []
    for fn in sorted(declared.keys() | shipped.keys()):
        if fn not in shipped:
            differences.append(f"{fn}: not in rpc_arguments.lua")
        elif fn not in declared:
            differences.append(f"{fn}: in rpc_arguments.lua but no btracker_endpoints function")
        else:
            differences += [f"{fn}: parameter {p} missing from rpc_arguments.lua"
                            for p in sorted(declared[fn] - shipped[fn])]
            differences += [f"{fn}: parameter {p} in rpc_arguments.lua but not in its SQL signature"
                            for p in sorted(shipped[fn] - declared[fn])]
    assert not differences, ("endpoints/rpc_arguments.lua differs from the SQL signatures "
                             "(regenerate it with scripts/openapi_rewrite.sh):\n" + "\n".join(differences))
