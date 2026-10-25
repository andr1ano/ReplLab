import argparse
import asyncio
import json
import random
import statistics
import time

import httpx

MIX = {"A": 0.5, "B": 0.95, "C": 1.0, "W": 0.0}

def pct(sorted_ms: list[float], p: float) -> float:
    if not sorted_ms:
        return 0.0
    return sorted_ms[min(len(sorted_ms) - 1, int(p / 100 * len(sorted_ms)))]

def summarize(lat: list[float]) -> dict:
    s = sorted(lat)
    return {"count": len(s), "mean_ms": round(statistics.fmean(s), 2) if s else 0.0,
            "p50_ms": round(pct(s, 50), 2), "p95_ms": round(pct(s, 95), 2), "p99_ms": round(pct(s, 99), 2)}

async def load_phase(client: httpx.AsyncClient, table: str, n: int, conc: int, wq: dict):
    sem = asyncio.Semaphore(conc)

    async def one(i):
        async with sem:
            r = await client.post(f"/tables/{table}/records", params=wq,
                                  json={"pk": f"user{i}", "value": {"field0": "x" * 100, "n": i}})
            if r.status_code not in (201, 409):
                raise RuntimeError(f"load failed: {r.status_code} {r.text}")
    t = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(n)))
    return time.perf_counter() - t

async def run_phase(client, table, records, workload, duration, conc, wq, rq, dist, seed):
    rnd = random.Random(seed)
    read_share = MIX[workload]
    lat = {"read": [], "update": []}
    errors = {"read": 0, "update": 0}
    stale = 0
    stop = time.perf_counter() + duration

    def pick():
        if dist == "zipf":
            return min(records - 1, int(records * rnd.random() ** 3))
        return rnd.randrange(records)

    async def worker():
        nonlocal stale
        while time.perf_counter() < stop:
            i = pick()
            read = rnd.random() < read_share
            t = time.perf_counter()
            try:
                if read:
                    r = await client.get(f"/tables/{table}/records", params={"pk": f"user{i}", **rq})
                    ok = r.status_code == 200
                else:
                    r = await client.put(f"/tables/{table}/records", params=wq,
                                         json={"pk": f"user{i}", "value": {"field0": "y" * 100, "n": i}})
                    ok = r.status_code == 200
            except httpx.HTTPError:
                ok = False
            kind = "read" if read else "update"
            if ok:
                lat[kind].append((time.perf_counter() - t) * 1000)
            else:
                errors[kind] += 1
    t0 = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(conc)))
    elapsed = time.perf_counter() - t0
    done = len(lat["read"]) + len(lat["update"])
    return {"elapsed_s": round(elapsed, 2), "ops": done, "throughput_ops_s": round(done / elapsed, 1),
            "errors": errors, "read": summarize(lat["read"]), "update": summarize(lat["update"]),
            "all": summarize(lat["read"] + lat["update"])}

async def run(args) -> dict:
    limits = httpx.Limits(max_connections=args.concurrency * 2, max_keepalive_connections=args.concurrency * 2)
    async with httpx.AsyncClient(base_url=args.url, timeout=30, limits=limits) as c:
        r = await c.post("/tables", json={"name": args.table, "partition_key": "user_id"})
        if r.status_code not in (201, 409):
            raise SystemExit(f"cannot create table: {r.status_code} {r.text}")
        wq = {"w": args.w} if args.w else {}
        rq = {"r": args.r} if args.r else {}
        load_s = await load_phase(c, args.table, args.records, args.concurrency, wq) if args.records else 0
        res = await run_phase(c, args.table, args.records, args.workload, args.duration,
                              args.concurrency, wq, rq, args.dist, args.seed)
        health = (await c.get("/health")).json()
    return {"workload": args.workload, "records": args.records, "concurrency": args.concurrency,
            "w": args.w, "r": args.r, "replication": health.get("replication"),
            "load_throughput_ops_s": round(args.records / load_s, 1) if load_s else None, **res}

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--table", default="usertable")
    ap.add_argument("--workload", choices=sorted(MIX), default="B")
    ap.add_argument("--records", type=int, default=1000)
    ap.add_argument("--duration", type=float, default=10)
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--w", type=int, default=0, help="кворум запису (0 = за замовчуванням координатора)")
    ap.add_argument("--r", type=int, default=0, help="кворум читання (0 = за замовчуванням координатора)")
    ap.add_argument("--dist", choices=["uniform", "zipf"], default="uniform")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args()
    out = asyncio.run(run(a))
    if a.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return
    rd, up = out["read"], out["update"]
    print(f"workload={a.workload} conc={a.concurrency} w={a.w or 'def'} r={a.r or 'def'} "
          f"replication={out['replication']}")
    print(f"throughput: {out['throughput_ops_s']} ops/s   ops={out['ops']}   errors={out['errors']}")
    print(f"read   : p50={rd['p50_ms']}ms p95={rd['p95_ms']}ms p99={rd['p99_ms']}ms  (n={rd['count']})")
    print(f"update : p50={up['p50_ms']}ms p95={up['p95_ms']}ms p99={up['p99_ms']}ms  (n={up['count']})")

if __name__ == "__main__":
    main()
