"""Fallback dependency if redis-server is not installed: speaks the tiny subset of
the Redis protocol the apps use (INCR, PING). Usage: python3 minikv.py <port>"""
import asyncio
import sys

counts = {}


async def handle(r, w):
    try:
        while True:
            data = await r.read(4096)
            if not data:
                break
            parts = data.split(b"\r\n")
            cmd = parts[2].upper() if len(parts) > 2 else b""
            if cmd == b"INCR":
                k = parts[4]
                counts[k] = counts.get(k, 0) + 1
                w.write(b":%d\r\n" % counts[k])
            elif cmd == b"PING":
                w.write(b"+PONG\r\n")
            else:
                w.write(b"-ERR unsupported\r\n")
            await w.drain()
    except Exception:
        pass
    finally:
        w.close()


async def main():
    s = await asyncio.start_server(handle, "127.0.0.1", int(sys.argv[1]))
    await s.serve_forever()

asyncio.run(main())
