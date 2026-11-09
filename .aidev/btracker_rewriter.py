#!/usr/bin/env python3
"""Run tests/rewriter through a postgrest-rewriter image built from the candidate tree.

    btracker_rewriter.py OUT_DIR

As CI's rewriter-test runs them, against the balance tracker synced to block 5000000:
Dockerfile.rewriter is built from this checkout, and run on a private docker network
beside a PostgREST container (docker/docker-compose-test.yml's postgrest service,
POSTGREST_IMAGE overriding its image) under the alias the nginx template proxies to,
connecting with BTRACKER_TEST_DB_URL (the run's clone). Both publish to loopback ports,
which the tests reach as REWRITER_URL and POSTGREST_URL, so the tests must run where the
docker daemon's loopback is theirs (a host, or a container on the host's network).

Writes OUT_DIR/rewriter.xml (one junit case per test) and the two containers' logs
(rewriter-*-container.log), and
exits with pytest's status; with SKIPPED, and the reason as its last line, when there is
no docker to build and run the images with.
"""
import os
import secrets
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
SKIPPED = 77
POSTGREST_IMAGE = os.environ.get("POSTGREST_IMAGE", "registry.gitlab.syncad.com/hive/haf_api_node/postgrest:latest")
# The upstream name and port docker/balance_tracker_nginx.conf.template proxies to.
POSTGREST_ALIAS = "balance-tracker-postgrest"
STARTUP_TIMEOUT_S = 90
WORKERS = "4"


def _docker(*args: str, stdout: int | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["docker", *args], check=True, stdout=stdout, text=True)


def _docker_unavailable() -> str | None:
    if shutil.which("docker") is None:
        return "no docker client in the test runtime: the rewriter image cannot be built or run here"
    info = subprocess.run(["docker", "info"], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True, check=False)
    if info.returncode != 0:
        reason = (info.stderr.strip().splitlines() or ["no output"])[-1]
        return f"the test runtime reaches no docker daemon ({reason}): the rewriter image cannot be built or run here"
    return None


def _published(container: str, port: str) -> str:
    out = _docker("port", container, port, stdout=subprocess.PIPE).stdout.split()
    return f"http://{out[0]}"


def _answers(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            status: int = resp.status
            return status == 200
    except (urllib.error.URLError, OSError):
        return False


def _wait_for(url: str) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_S
    while not _answers(url):
        if time.monotonic() > deadline:
            raise RuntimeError(f"{url} not answering after {STARTUP_TIMEOUT_S}s")
        time.sleep(1)


@contextmanager
def _stack(out: str) -> Iterator[tuple[str, str]]:
    """Build the rewriter, run it and PostgREST; yield (rewriter URL, PostgREST URL).

    Removes the containers, the network and the image afterwards, keeping their logs in out.
    """
    name = f"btracker-rewriter-{secrets.token_hex(4)}"
    image, postgrest, rewriter = f"{name}:candidate", f"{name}-postgrest", f"{name}-rewriter"
    started: list[str] = []
    network = False
    try:
        _docker("build", "--file", os.path.join(ROOT, "Dockerfile.rewriter"), "--tag", image, ROOT)
        _docker("network", "create", name, stdout=subprocess.DEVNULL)
        network = True
        _docker("run", "--detach", "--name", postgrest, "--network", name, "--network-alias", POSTGREST_ALIAS,
                "--publish", "127.0.0.1::3000",
                "--env", f"PGRST_DB_URI={os.environ['BTRACKER_TEST_DB_URL']}",
                "--env", "PGRST_DB_SCHEMA=btracker_endpoints",
                "--env", "PGRST_DB_ANON_ROLE=btracker_user",
                "--env", "PGRST_DB_EXTRA_SEARCH_PATH=btracker_app",
                POSTGREST_IMAGE, stdout=subprocess.DEVNULL)
        started.append(postgrest)
        _docker("run", "--detach", "--name", rewriter, "--network", name, "--publish", "127.0.0.1::80", image,
                stdout=subprocess.DEVNULL)
        started.append(rewriter)
        rewriter_url, postgrest_url = _published(rewriter, "80/tcp"), _published(postgrest, "3000/tcp")
        _wait_for(postgrest_url + "/rpc/get_btracker_version")
        _wait_for(rewriter_url + "/version")
        yield rewriter_url, postgrest_url
    finally:
        for container in started:
            with open(os.path.join(out, f"rewriter-{container.rsplit('-', 1)[1]}-container.log"), "w") as log:
                subprocess.run(["docker", "logs", container], stdout=log, stderr=subprocess.STDOUT, check=False)
        if started:
            subprocess.run(["docker", "rm", "--force", *started], stdout=subprocess.DEVNULL, check=False)
        if network:
            subprocess.run(["docker", "network", "rm", name], stdout=subprocess.DEVNULL, check=False)
        subprocess.run(["docker", "image", "rm", "--force", image], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL, check=False)


def main(out: str) -> int:
    reason = _docker_unavailable()
    if reason:
        print(reason)
        return SKIPPED
    out = os.path.abspath(out)
    os.makedirs(out, exist_ok=True)
    with _stack(out) as (rewriter_url, postgrest_url):
        print(f"rewriter {rewriter_url}, PostgREST {postgrest_url}", flush=True)
        env = dict(os.environ, REWRITER_URL=rewriter_url, POSTGREST_URL=postgrest_url)
        return subprocess.run(
            [sys.executable, "-m", "pytest", "-n", WORKERS, "-p", "no:cacheprovider",
             "--junitxml", os.path.join(out, "rewriter.xml"), os.path.join("tests", "rewriter")],
            cwd=ROOT, env=env, check=False,
        ).returncode


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(f"usage: {sys.argv[0]} OUT_DIR")
    sys.exit(main(sys.argv[1]))
