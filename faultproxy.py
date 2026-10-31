"""TCP fault-injection proxy (a tiny, dependency-free Toxiproxy).

One listener per app replica and one for Redis. Faults are set through an admin
HTTP endpoint:
    POST /set?name=app1&delay_ms=220     add latency to every connection
    POST /set?name=redis&down=1          refuse (reset) every connection
    POST /set?name=app1&delay_ms=0&down=0  clear
    GET  /state
"""
import asyncio
import json
from urllib.parse import parse_qs, urlparse

import config as C

ROUTES = {name: (pp, ap) for name, (ap, pp) in C.APPS.items()}
ROUTES["redis"] = (C.REDIS_PROXY_PORT, C.REDIS_PORT)
FAULTS = {name: {"delay_ms": 0, "down": False} for name in ROUTES}


async def pump(reader, writer, delay_first=0.0):
    first = True
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            if first and delay_first > 0:
                await asyncio.sleep(delay_first)
            first = False
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


def make_handler(name, upstream_port):
    async def handle(cr, cw):
        f = FAULTS[name]
        if f["down"]:
            cw.transport.abort()
            return
        try:
            ur, uw = await asyncio.open_connection(C.HOST, upstream_port)
        except Exception:
            cw.transport.abort()
            return
        d = f["delay_ms"] / 1000.0
        await asyncio.gather(pump(cr, uw), pump(ur, cw, delay_first=d))
    return handle


async def admin(reader, writer):
    try:
        line = (await reader.readline()).decode()
        while (await reader.readline()) not in (b"\r\n", b"\n", b""):
            pass
        if line.count(" ") < 2:
            return  # bare connection (port check) - ignore
        method, target, _ = line.split(" ", 2)
        u = urlparse(target)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        code, body = 200, {}
        if u.path == "/set" and q.get("name") in FAULTS:
            f = FAULTS[q["name"]]
            if "delay_ms" in q:
                f["delay_ms"] = int(q["delay_ms"])
            if "down" in q:
                f["down"] = q["down"] == "1"
            body = {q["name"]: f}
        elif u.path == "/state":
            body = FAULTS
        else:
            code, body = 404, {"error": "unknown"}
        raw = json.dumps(body).encode()
        writer.write(f"HTTP/1.0 {code} OK\r\nContent-Type: application/json\r\n"
                     f"Content-Length: {len(raw)}\r\n\r\n".encode() + raw)
        await writer.drain()
    finally:
        writer.close()


async def main():
    servers = [await asyncio.start_server(make_handler(n, up), C.HOST, lp)
               for n, (lp, up) in ROUTES.items()]
    servers.append(await asyncio.start_server(admin, C.HOST, C.PROXY_ADMIN_PORT))
    await asyncio.gather(*(s.serve_forever() for s in servers))


if __name__ == "__main__":
    asyncio.run(main())
