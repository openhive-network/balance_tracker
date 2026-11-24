"""Concurrent massive sync: haf_app_driver.py's range processor when BTRACKER_SYNC_PARALLELISM > 1.

    BTRACKER_SCHEMA=<schema> BTRACKER_SYNC_PARALLELISM=<n> PYTHONPATH=<this directory> \\
        haf_app_driver.py --app=<application> ... --process-python=btracker_parallel:process_blocks

The driver opens a transaction, calls hive.app_next_iteration, calls process_blocks(conn, first,
last) on the same connection and commits; that commit is what moves the HAF context. Here, for one
range of the balance tracker installed in BTRACKER_SCHEMA (btracker_app standalone, hafbe_bal under
haf_block_explorer):

  * <schema>.begin_parallel_range decides, in the driver's transaction, whether the processor groups
    run the range (db/parallel_sync.sql says when they do not: outside massive sync, while the
    context is attached, the HF23 batch, a transaction that locked the schema's relations);
  * if they do, the range is staged once into the schema's shared staging (stage_shared_batch, on a
    worker connection, committed), then the groups (processor_groups()) are dealt out in call order
    to at most BTRACKER_SYNC_PARALLELISM worker connections, which run at once; a connection runs its
    groups one after another, each in a transaction of its own that commits the group's writes with
    its progress row (process_processor_group). Once every group has committed, finish_parallel_range
    runs in the driver's transaction;
  * either way the application's registered entry procedure then runs in the driver's transaction,
    as the driver would run it without this module: btracker's own (btracker_process_blocks), or
    haf_block_explorer's, which runs its own processors and calls btracker_process_blocks for the
    same range. btracker_process_blocks does not process a range finish_parallel_range applied.

If the staging or a group fails, the statements after it on its connection do not run, the first
failure is raised once every connection has finished, the driver rolls its transaction back and the
context stays before the range: the range is delivered again, a staging that committed is reused and
the groups that committed skip it.

The staging is pipelined: while a batch's groups run, one more worker connection stages the range the
next batch is likely to be (as long as the longest range delivered so far, right after this one), and
the next call waits for that staging before anything else. When it staged the range the driver
delivers, the groups start at once; otherwise the range is staged before its groups. The SQL keeps it
safe whatever the guess: two batches' stagings live in separate slots, a range past HAF's irreversible
block is not staged, and a group reads only a slot marked complete for exactly its range.

A worker connection gets everything it runs for a batch in one round trip: its share of the groups,
or the staging, as one query string of BEGIN; SELECT ...; COMMIT blocks, in autocommit mode. Each
batch logs one line with the wall time of its phases.

The worker connections use the driver's connection parameters (conn.get_dsn_parameters(), so the
same user, options and application_name) and the password the driver's connection authenticated with
(conn.info.password): conn.dsn masks it. Each one, when it opens, takes HAF's shared block-processor
lock on the application, as the driver's --lock does for its own connection, so an install waits for
every worker backend too; and sets client_connection_check_interval, so a backend whose process is
gone stops within seconds instead of finishing its group. Their server messages are written to
stdout as the driver writes its own.
"""

from __future__ import annotations

import datetime
import os
import queue
import re
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Protocol

PARALLELISM_ENV = "BTRACKER_SYNC_PARALLELISM"
SCHEMA_ENV = "BTRACKER_SCHEMA"

_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")

# The application's lock name comes in as a setting, so that the DO block needs no quoting of it.
_SESSION_SQL = """SELECT set_config('btracker.parallel_app', %s, false);
DO $$
BEGIN
  IF current_setting('server_version_num')::INT >= 140000 THEN
    PERFORM set_config('client_connection_check_interval', '10s', false);
  END IF;
  IF to_regprocedure('hive.acquire_app_block_processor_locks(text[])') IS NOT NULL THEN
    PERFORM hive.acquire_app_block_processor_locks(ARRAY[current_setting('btracker.parallel_app')]);
  END IF;
END
$$"""


class _Cursor(Protocol):
    def execute(self, sql: str, params: Sequence[object] = ...) -> Any: ...

    def fetchone(self) -> Any: ...


class _ConnectionInfo(Protocol):
    password: str | None


class _Connection(Protocol):
    info: _ConnectionInfo
    notices: list[str]
    autocommit: bool

    def get_dsn_parameters(self) -> dict[str, str]: ...

    def cursor(self) -> _Cursor: ...

    def close(self) -> None: ...


def parallelism_from_env(environ: Mapping[str, str] = os.environ) -> int:
    """BTRACKER_SYNC_PARALLELISM as a positive integer; 1 when unset or empty.

    :raises ValueError: the value is not a positive integer.
    """
    raw = environ.get(PARALLELISM_ENV, "").strip()
    if not raw:
        return 1
    if not raw.isdigit() or int(raw) < 1:
        raise ValueError(f"{PARALLELISM_ENV} must be a positive integer, not {raw!r}")
    return int(raw)


def schema_from_env(environ: Mapping[str, str] = os.environ) -> str:
    """BTRACKER_SCHEMA; btracker_app when unset or empty.

    :raises ValueError: the value is not a lower-case SQL identifier.
    """
    schema = environ.get(SCHEMA_ENV, "").strip() or "btracker_app"
    if not _IDENTIFIER.fullmatch(schema):
        raise ValueError(f"{SCHEMA_ENV} must be a lower-case SQL identifier, not {schema!r}")
    return schema


class ParallelCoordinator:
    """Runs a range's processor groups on up to `parallelism` connections of its own, and stages the
    next range ahead on one more.

    `connect` opens a worker connection from libpq connection parameters given as keyword arguments
    (psycopg2.connect); `db_error` is the base class of what its connections raise (psycopg2.Error).
    """

    def __init__(
        self, schema: str, parallelism: int, connect: Callable[..., _Connection], db_error: type[Exception]
    ) -> None:
        if parallelism < 1:
            raise ValueError(f"parallelism must be at least 1, not {parallelism}")
        if not _IDENTIFIER.fullmatch(schema):
            raise ValueError(f"schema must be a lower-case SQL identifier, not {schema!r}")
        self._schema = schema
        self._parallelism = parallelism
        self._connect = connect
        self._db_error = db_error
        self._stage_sql = f"SELECT {schema}.stage_shared_batch(%s, %s)"
        self._staged_sql = f"SELECT {schema}.shared_batch_complete(%s, %s)"
        self._group_sql = f"SELECT {schema}.process_processor_group(%s, %s, %s)"
        self._idle: queue.SimpleQueue[_Connection] = queue.SimpleQueue()
        self._executor: ThreadPoolExecutor | None = None
        self._application: str | None = None
        self._procedure: str | None = None
        self._group_shares: list[list[str]] | None = None
        self._out_lock = threading.Lock()
        self._batch_blocks = 0
        self._ahead: Future[tuple[int, int] | None] | None = None

    def process(self, conn: _Connection, first_block: int, last_block: int) -> None:
        started = time.monotonic()
        cur = conn.cursor()
        # Before begin_parallel_range, which may empty the slots once massive sync is over.
        staged_ahead = self._staged_ahead()
        if self._procedure is None:
            self._prepare(cur)
        if self._procedure is None or self._application is None:
            raise RuntimeError("the coordinator has no application")
        cur.execute(f"SELECT {self._schema}.begin_parallel_range(%s, %s)", (first_block, last_block))
        if self._parallelism > 1 and bool(cur.fetchone()[0]):
            self._process_groups(cur, conn, staged_ahead, started, first_block, last_block)
        cur.execute(f"CALL {self._procedure}(%s::hive.blocks_range)", (f"({first_block},{last_block})",))

    def _process_groups(
        self,
        cur: _Cursor,
        conn: _Connection,
        staged_ahead: tuple[int, int] | None,
        started: float,
        first_block: int,
        last_block: int,
    ) -> None:
        if self._group_shares is None or self._executor is None:
            raise RuntimeError("the coordinator has no groups")
        params = _worker_connection_params(conn)
        staging_started = time.monotonic()
        if staged_ahead != (first_block, last_block):
            self._run_at_once(params, "batch staging", self._stage_sql, [[None]], first_block, last_block)
        groups_started = time.monotonic()
        self._batch_blocks = max(self._batch_blocks, last_block - first_block + 1)
        self._ahead = self._executor.submit(
            self._stage_ahead, params, last_block + 1, last_block + self._batch_blocks
        )
        self._run_at_once(params, "processor groups", self._group_sql, self._group_shares, first_block, last_block)
        finish_started = time.monotonic()
        cur.execute(f"SELECT {self._schema}.finish_parallel_range(%s, %s)", (first_block, last_block))
        self._log(
            f"blocks {first_block}..{last_block}: begin {staging_started - started:.3f}s, "
            f"staging {groups_started - staging_started:.3f}s"
            f"{' (staged ahead)' if staged_ahead == (first_block, last_block) else ''}, "
            f"groups {finish_started - groups_started:.3f}s, finish {time.monotonic() - finish_started:.3f}s"
        )

    def _run_at_once(
        self,
        params: Mapping[str, str],
        kind: str,
        sql: str,
        shares: Sequence[Sequence[str | None]],
        first_block: int,
        last_block: int,
    ) -> None:
        """`sql` for each name of each share and the range: each share on a worker connection of its
        own, in one round trip, each name's statement committed there on its own."""
        if self._executor is None:
            raise RuntimeError("the coordinator has no executor")
        futures = [
            self._executor.submit(self._run_on_worker, params, kind, sql, names, first_block, last_block)
            for names in shares
        ]
        # Every share is waited for, failed or not, so no worker of this range is still running when
        # the next phase starts, or when the driver rolls back and asks for the range again.
        errors = [f.exception() for f in futures]
        failed = next((e for e in errors if e is not None), None)
        if failed is not None:
            raise failed

    def _prepare(self, cur: _Cursor) -> None:
        cur.execute(
            f"SELECT a.name, a.process_procedure, {self._schema}.processor_groups()"
            " FROM hafd.applications a WHERE a.contexts @> ARRAY[%s]::TEXT[]",
            (self._schema,),
        )
        row = cur.fetchone()
        if row is None or row[1] is None:
            raise RuntimeError(
                f"no application with an entry procedure holds context {self._schema} in the HAF registry"
            )
        self._application, self._procedure, groups = row
        workers = min(self._parallelism, len(groups))
        # Dealt out in call order (longest first): connection i runs groups i, i + workers, ...
        self._group_shares = [list(groups[i::workers]) for i in range(workers)]
        # One more than the groups' connections: the staging ahead runs beside them.
        self._executor = ThreadPoolExecutor(max_workers=workers + 1, thread_name_prefix="btracker_group")

    def _staged_ahead(self) -> tuple[int, int] | None:
        """The range the previous batch staged ahead, once that staging has finished; None when it
        did not complete one (or there was none)."""
        ahead, self._ahead = self._ahead, None
        if ahead is None:
            return None
        try:
            return ahead.result()
        except self._db_error:
            # _run_on_worker logged it; the batch stages its range itself.
            return None

    def _stage_ahead(self, params: Mapping[str, str], first_block: int, last_block: int) -> tuple[int, int] | None:
        complete = self._run_on_worker(
            params, "batch staging ahead", self._stage_sql, [None], first_block, last_block, check=self._staged_sql
        )
        return (first_block, last_block) if complete else None

    def _run_on_worker(
        self,
        params: Mapping[str, str],
        kind: str,
        sql: str,
        names: Sequence[str | None],
        first_block: int,
        last_block: int,
        check: str | None = None,
    ) -> Any:
        """`sql` for each name and the range, each committed on its own (a None name is left out of
        the statement's parameters); then `check`, given the range, whose value is returned (None
        without one)."""
        try:
            worker = self._idle.get_nowait()
        except queue.Empty:
            worker = self._open_worker(params, kind)
        healthy = False
        try:
            # One query string, so one round trip. A statement that fails leaves its BEGIN block
            # aborted and the server skips the rest of the string; the blocks before it stay committed.
            statements = [f"BEGIN; {sql}; COMMIT" for _ in names]
            values: list[object] = []
            for name in names:
                values += [first_block, last_block] if name is None else [name, first_block, last_block]
            if check is not None:
                statements.append(check)
                values += [first_block, last_block]
            cur = worker.cursor()
            cur.execute("; ".join(statements), values)
            result = cur.fetchone()[0] if check is not None else None
            healthy = True
        except self._db_error as e:
            described = ", ".join(n for n in names if n is not None)
            self._log(f"{kind}{' ' + described if described else ''}, blocks {first_block}..{last_block}, failed: {str(e).strip()}")
            healthy = self._rolled_back(worker)
            raise
        finally:
            self._forward_notices(worker)
            if healthy:
                self._idle.put(worker)
            else:
                self._close(worker)
        return result

    def _open_worker(self, params: Mapping[str, str], kind: str) -> _Connection:
        """A new worker connection in autocommit mode, holding HAF's shared lock on the application."""
        worker = self._connect(**params)
        worker.autocommit = True
        worker.notices = []
        try:
            worker.cursor().execute(_SESSION_SQL, (self._application,))
        except self._db_error as e:
            self._log(f"{kind}: a new worker connection could not take HAF's lock on {self._application}: {str(e).strip()}")
            self._close(worker)
            raise
        finally:
            self._forward_notices(worker)
        return worker

    def _rolled_back(self, worker: _Connection) -> bool:
        """Whether the worker connection is still usable after a statement failed: one that cannot
        roll back (a lost connection) is closed and replaced on the next range. ROLLBACK as a
        statement: psycopg2's rollback() sends nothing on a connection in autocommit mode, which a
        failed BEGIN block leaves aborted."""
        try:
            worker.cursor().execute("ROLLBACK")
            return True
        except self._db_error as e:
            self._log(f"discarding a worker connection that could not roll back: {str(e).strip()}")
            return False

    def _close(self, worker: _Connection) -> None:
        try:
            worker.close()
        except self._db_error as e:
            self._log(f"closing a discarded worker connection failed: {str(e).strip()}")

    def _forward_notices(self, worker: _Connection) -> None:
        for notice in worker.notices:
            lines = [line.rstrip() for line in notice.rstrip("\n").split("\n") if line.strip()]
            if lines:
                self._emit(lines)
        del worker.notices[:]

    def _log(self, message: str) -> None:
        self._emit([f"btracker_parallel: {message}"])

    def _emit(self, lines: list[str]) -> None:
        ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        with self._out_lock:
            sys.stdout.write(f"{ts} {lines[0]}\n" + "".join(f"    {extra}\n" for extra in lines[1:]))
            sys.stdout.flush()


def _worker_connection_params(conn: _Connection) -> dict[str, str]:
    params = dict(conn.get_dsn_parameters())
    password = conn.info.password
    # libpq reports "" for a connection that authenticated without a password.
    if password:
        params["password"] = password
    return params


_coordinator: ParallelCoordinator | None = None


def process_blocks(conn: _Connection, first_block: int, last_block: int) -> None:
    """haf_app_driver.py's --process-python entry point: one delivered range, in the driver's
    transaction on `conn`, which this does not commit."""
    global _coordinator
    if _coordinator is None:
        import psycopg2

        _coordinator = ParallelCoordinator(schema_from_env(), parallelism_from_env(), psycopg2.connect, psycopg2.Error)
    _coordinator.process(conn, first_block, last_block)
