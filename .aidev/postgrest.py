"""PostgREST over the installed balance tracker schema, for the checks that call its API.

Configured as docker/docker-compose.yml's backend-postgrest service (schema
btracker_endpoints, anonymous role btracker_user, extra search path btracker_app),
on a free loopback port, connecting with BTRACKER_TEST_DB_URL (the run's clone of
the shared HAF, as haf_shared_consumer, which switches to btracker_user through
btracker_owner).
"""
import contextlib
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Iterator

STARTUP_TIMEOUT_S = 60


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


def _answers(base: str) -> bool:
    try:
        with urllib.request.urlopen(base + "/rpc/get_btracker_version", timeout=5) as resp:
            status: int = resp.status
            return status == 200
    except (urllib.error.URLError, OSError):
        return False


@contextlib.contextmanager
def serving(log_path: str) -> Iterator[str]:
    """Run PostgREST for the duration of the block; yield its base URL.

    Its output goes to log_path. Raises RuntimeError when it exits or does not
    answer within STARTUP_TIMEOUT_S.
    """
    port = _free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ,
               PGRST_DB_URI=os.environ["BTRACKER_TEST_DB_URL"],
               PGRST_DB_SCHEMA="btracker_endpoints",
               PGRST_DB_ANON_ROLE="btracker_user",
               PGRST_DB_EXTRA_SEARCH_PATH="btracker_app",
               PGRST_SERVER_HOST="127.0.0.1",
               PGRST_SERVER_PORT=str(port))
    with open(log_path, "w") as log:
        proc = subprocess.Popen(["postgrest"], env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + STARTUP_TIMEOUT_S
            while not _answers(base):
                if proc.poll() is not None:
                    raise RuntimeError(f"postgrest exited with {proc.returncode}; see {log_path}")
                if time.monotonic() > deadline:
                    raise RuntimeError(f"postgrest not serving after {STARTUP_TIMEOUT_S}s; see {log_path}")
                time.sleep(1)
            yield base
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
