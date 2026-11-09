#!/usr/bin/env python3
"""balance-tracker-postgrest in the rewriter stack: a forwarder to the PostgREST the rewriter step serves.

docker/balance_tracker_nginx.conf.template proxies to balance-tracker-postgrest:3000, a
name nginx resolves when the stack starts, while a PostgREST over the run's clone can only
start once the install step has made the clone. This service holds the name: a client that
connects to REGISTER_PORT and sends its PostgREST's port as one line becomes the target, at
the client's own address, and every connection to PORT is then piped to it. A connection
before any registration is closed unanswered, which nginx answers with a 502.
"""
import asyncio

PORT = 3000
REGISTER_PORT = 3001
_CHUNK = 65536

_target: tuple[str, int] | None = None


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(_CHUNK):
            writer.write(data)
            await writer.drain()
        if writer.can_write_eof():
            writer.write_eof()
    except OSError:
        writer.close()


async def _register(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    global _target
    line = await reader.readline()
    try:
        port = int(line)
    except ValueError:
        writer.write(b"expected the PostgREST port as one line\n")
    else:
        _target = (writer.get_extra_info("peername")[0], port)
        print(f"forwarding to {_target[0]}:{port}", flush=True)
        writer.write(b"ok\n")
    await writer.drain()
    writer.close()


async def _forward(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    target = _target
    if target is None:
        writer.close()
        return
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(*target)
    except OSError as exc:
        print(f"cannot reach {target[0]}:{target[1]}: {exc}", flush=True)
        writer.close()
        return
    await asyncio.gather(_pipe(reader, upstream_writer), _pipe(upstream_reader, writer))
    upstream_writer.close()
    writer.close()


async def main() -> None:
    forward = await asyncio.start_server(_forward, port=PORT)
    register = await asyncio.start_server(_register, port=REGISTER_PORT)
    print(f"listening on {PORT}, registering on {REGISTER_PORT}", flush=True)
    async with forward, register:
        await asyncio.gather(forward.serve_forever(), register.serve_forever())


if __name__ == "__main__":
    asyncio.run(main())
