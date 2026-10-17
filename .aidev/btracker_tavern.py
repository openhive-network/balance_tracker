#!/usr/bin/env python3
"""Run the tavern pattern tests (tests/tavern/patterns-mainnet) against PostgREST over the run's clone.

    btracker_tavern.py OUT_DIR

As CI's pattern-test runs them, against the balance tracker synced to block
5000000: each test POSTs to /rpc/<function> at BTRACKER_ADDRESS:BTRACKER_PORT and
hive/tests_api's validate_response compares the answer with the test's
.pat.json. The comparator writes each answer beside its test, so the tests run
from a copy in OUT_DIR/tavern, which keeps those answers as artefacts.

tests/tavern/patterns-mock is not run: it needs CI's mock blocks inserted into
HAF's base tables, which the shared HAF's consumer may not write.

Writes OUT_DIR/tavern.xml (one junit case per tavern test) and exits with
pytest's status.
"""
import os
import shutil
import subprocess
import sys

from postgrest import serving

HERE = os.path.dirname(os.path.abspath(__file__))
TAVERN_SOURCE = os.path.join(HERE, "..", "tests", "tavern")
PATTERNS = "patterns-mainnet"
WORKERS = "4"


def main(out: str) -> int:
    os.makedirs(out, exist_ok=True)
    tavern_dir = os.path.abspath(os.path.join(out, "tavern"))
    shutil.rmtree(tavern_dir, ignore_errors=True)
    shutil.copytree(TAVERN_SOURCE, tavern_dir, ignore=shutil.ignore_patterns("patterns-mock"))
    with serving(os.path.join(out, "postgrest-tavern.log")) as base:
        host, port = base.removeprefix("http://").rsplit(":", 1)
        env = dict(os.environ, BTRACKER_ADDRESS=host, BTRACKER_PORT=port, TAVERN_DIR=tavern_dir)
        return subprocess.run(
            [sys.executable, "-m", "pytest", "-n", WORKERS, "-p", "no:cacheprovider",
             "--junitxml", os.path.abspath(os.path.join(out, "tavern.xml")), PATTERNS],
            cwd=tavern_dir, env=env, check=False,
        ).returncode


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} OUT_DIR")
    sys.exit(main(sys.argv[1]))
