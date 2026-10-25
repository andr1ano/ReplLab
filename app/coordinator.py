import asyncio
import json
import logging
import os
import tempfile
import time
from collections import Counter
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Literal

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator

from .ring import HashRing, hash64, route_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("coordinator")

NAME = r"^[A-Za-z][A-Za-z0-9_]{0,63}$"
ID = r"^[A-Za-z0-9_.-]{1,64}$"

H_SHARD, H_LEADER, H_VERSION, H_ACKS, H_READERS = (
    "X-Shard-Id", "X-Leader", "X-Version", "X-Acks", "X-Read-Replicas")
DEAD = float("-inf")

# ───────────────────────────── моделі ─────────────────────────────
class TableDef(BaseModel):
    name: str = Field(pattern=NAME)
    partition_key: str = Field(pattern=NAME)
    sort_key: str | None = Field(default=None, pattern=NAME)

    @model_validator(mode="after")
    def _distinct(self):
        if self.sort_key == self.partition_key:
            raise ValueError("sort_key must differ from partition_key")
        return self


class RecordIn(BaseModel):
    pk: str = Field(min_length=1, max_length=512)
    sk: str | None = Field(default=None, min_length=1, max_length=512)
    value: Any


class ReplicaIn(BaseModel):
    shard_id: str = Field(pattern=ID)
    replica_id: str = Field(pattern=ID)
    url: str = Field(pattern=r"^https?://\S+$")
    role: Literal["leader", "follower"]
    ready: bool = False
    applied_seq: int = 0

# ───────────────────────────── стан ─────────────────────────────
@dataclass
class Replica:
    id: str
    url: str
    role: str
    last_seen: float = DEAD
    ready: bool = False
    applied: int = 0
    reads: int = 0

@dataclass
class Group:
    id: str
    replicas: dict[str, Replica] = field(default_factory=dict)
    rr: int = 0

    @property
    def leader(self) -> Replica | None:
        return next((r for r in self.replicas.values() if r.role == "leader"), None)

class State:
    def __init__(self, catalog_path: str, vnodes: int, n: int, w: int, r: int, ttl: float):
        self.catalog_path, self.n, self.w, self.r, self.ttl = catalog_path, n, w, r, ttl
        if not (1 <= w <= n and 1 <= r <= n):
            raise SystemExit(f"некоректні кворуми: N={n} W={w} R={r} (потрібно 1 ≤ W,R ≤ N)")
        self.tables: dict[str, TableDef] = {}
        self.groups: dict[str, Group] = {}
        self.ring = HashRing(vnodes)
        self.ready = asyncio.Event()
        self.ready.set()
        self.admin_lock = asyncio.Lock()
        self.http: httpx.AsyncClient | None = None

    def alive(self, rep: Replica) -> bool:
        return time.monotonic() - rep.last_seen <= self.ttl

    def live_followers(self, g: Group) -> list[Replica]:
        return [r for r in g.replicas.values() if r.role == "follower" and self.alive(r)]

    def readable(self, g: Group) -> list[Replica]:
        return sorted((r for r in g.replicas.values() if self.alive(r) and r.ready), key=lambda r: r.id)

    def load(self) -> None:
        if not os.path.exists(self.catalog_path):
            return
        with open(self.catalog_path) as f:
            data = json.load(f)
        self.tables = {n: TableDef(**t) for n, t in data.get("tables", {}).items()}
        for sid, g in data.get("shards", {}).items():
            grp = Group(sid, {rid: Replica(rid, r["url"], r["role"]) for rid, r in g["replicas"].items()})
            self.groups[sid] = grp
            if grp.leader:
                self.ring.add_shard(sid)
        log.info("каталог: %d таблиць, %d шардів", len(self.tables), len(self.groups))

    def save(self) -> None:
        d = os.path.dirname(self.catalog_path) or "."
        os.makedirs(d, exist_ok=True)
        data = {
            "tables": {n: t.model_dump() for n, t in self.tables.items()},
            "shards": {sid: {"replicas": {r.id: {"url": r.url, "role": r.role} for r in g.replicas.values()}}
                       for sid, g in self.groups.items()},
        }
        fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, self.catalog_path)

# ───────────────────────────── виклики реплік ─────────────────────────────
async def rcall(st: State, rep: Replica, method: str, path: str, **kw) -> httpx.Response:
    try:
        return await st.http.request(method, rep.url + path, **kw)
    except httpx.HTTPError as e:
        rep.last_seen = DEAD
        raise HTTPException(503, f"replica unavailable: {rep.id}") from e


def pick_group(st: State, table: str, pk: str) -> Group:
    try:
        sid = st.ring.get_shard(route_key(table, pk))
    except LookupError:
        raise HTTPException(503, "no shards registered")
    return st.groups[sid]

async def quorum_fetch(st: State, g: Group, path: str, params: dict, r: int) -> tuple[list[dict], list[Replica]]:
    elig = st.readable(g)
    if len(elig) < r:
        raise HTTPException(503, f"read quorum unavailable: потрібно R={r}, живих готових реплік {len(elig)}")
    start = g.rr % len(elig)
    g.rr += 1
    queue = elig[start:] + elig[:start]
    answers: list[dict] = []
    used: list[Replica] = []

    async def one(rep: Replica):
        resp = await st.http.get(rep.url + path, params=params)
        resp.raise_for_status()
        return rep, resp.json()

    first = [queue.pop(0) for _ in range(r)]
    for res, rep in zip(await asyncio.gather(*(one(x) for x in first), return_exceptions=True), first):
        if isinstance(res, Exception):
            rep.last_seen = DEAD
        else:
            answers.append(res[1]); used.append(rep)
    while len(answers) < r and queue:
        rep = queue.pop(0)
        try:
            _, body = await one(rep)
            answers.append(body); used.append(rep)
        except Exception:
            rep.last_seen = DEAD
    if len(answers) < r:
        raise HTTPException(503, "read quorum unavailable: репліки не відповіли")
    for rep in used:
        rep.reads += 1
    return answers, used

# ───────────────────────────── rebalance ─────────────────────────────
async def rebalance(st: State) -> dict:
    scanned, moves, arrived = 0, Counter(), set()

    async def leader_write(g: Group, body: dict) -> httpx.Response:
        ld = g.leader
        if ld is None or not st.alive(ld):
            raise HTTPException(503, f"leader of {g.id} unavailable")
        fl = [f.url for f in st.live_followers(g)]
        return await rcall(st, ld, "POST", "/internal/write",
                           json={**body, "w": 1 + len(fl), "followers": fl})

    for sid, g in list(st.groups.items()):
        ld = g.leader
        if ld is None or sid not in st.ring.shards():
            continue
        if not st.alive(ld):
            raise HTTPException(503, f"leader of {sid} unavailable")
        after: dict = {}
        while True:
            r = await rcall(st, ld, "GET", "/internal/scan", params={"limit": 500, **after})
            r.raise_for_status()
            items = r.json()["items"]
            if not items:
                break
            for it in items:
                ident = (it["table"], it["pk"], it["sk"])
                if ident in arrived:
                    continue
                scanned += 1
                owner = st.ring.get_shard(route_key(it["table"], it["pk"]))
                if owner == sid:
                    continue
                w = await leader_write(st.groups[owner], {"op": "upsert", **it})
                if w.status_code != 200:
                    raise HTTPException(502, f"rebalance: запис на {owner} не вдався ({w.status_code})")
                d = await leader_write(g, {"op": "delete", "table": it["table"], "pk": it["pk"], "sk": it["sk"]})
                if d.status_code not in (200, 404):
                    raise HTTPException(502, f"rebalance: видалення на {sid} не вдалось ({d.status_code})")
                arrived.add(ident)
                moves[f"{sid}->{owner}"] += 1
            last = items[-1]
            after = {"after_table": last["table"], "after_pk": last["pk"], "after_sk": last["sk"]}
    moved = sum(moves.values())
    return {"scanned": scanned, "moved": moved,
            "moved_fraction": round(moved / scanned, 4) if scanned else 0.0, "moves": dict(moves)}

# ───────────────────────────── застосунок ─────────────────────────────
def create_app(catalog_path: str | None = None, vnodes: int | None = None) -> FastAPI:
    n = int(os.getenv("REPLICATION_FACTOR", "3"))
    st = State(
        catalog_path or os.getenv("CATALOG_PATH", "data/catalog.json"),
        vnodes or int(os.getenv("VNODES", "128")),
        n, int(os.getenv("WRITE_QUORUM", "2")), int(os.getenv("READ_QUORUM", "2")),
        float(os.getenv("REPLICA_TTL", "6")),
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        st.load()
        st.http = httpx.AsyncClient(timeout=30)
        log.info("реплікація: N=%d W=%d R=%d → %s", st.n, st.w, st.r,
                 "R+W>N, читання завжди свіжі" if st.r + st.w > st.n else "R+W≤N, читання МОЖУТЬ бути застарілими")
        yield
        await st.http.aclose()

    app = FastAPI(title="Sharded replicated KV store — coordinator", version="2.0.0", lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        msg = "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
        return JSONResponse(status_code=400, content={"detail": msg})

    async def gate():
        await st.ready.wait()

    def table_or_404(name: str) -> TableDef:
        t = st.tables.get(name)
        if t is None:
            raise HTTPException(404, f"table '{name}' not found")
        return t

    def check_key(t: TableDef, sk: str | None) -> None:
        if t.sort_key is None and sk is not None:
            raise HTTPException(400, f"table '{t.name}' has no sort key")
        if t.sort_key is not None and sk is None:
            raise HTTPException(400, f"table '{t.name}' requires sk ({t.sort_key})")

    def quorum_arg(v: int | None, default: int, name: str) -> int:
        v = default if v is None else v
        if not 1 <= v <= st.n:
            raise HTTPException(400, f"{name} має бути від 1 до N={st.n}")
        return v

    PK = Query(..., min_length=1, max_length=512)
    SK = Query(None, min_length=1, max_length=512)
    W = Query(None, ge=1, description="Скільки реплік мають підтвердити запис (за замовчуванням WRITE_QUORUM)")
    R = Query(None, ge=1, description="Скільки реплік опитати при читанні (за замовчуванням READ_QUORUM)")

    async def write_op(table: str, op: str, rec: RecordIn, w: int | None, response: Response) -> dict:
        t = table_or_404(table)
        check_key(t, rec.sk)
        w = quorum_arg(w, st.w, "w")
        g = pick_group(st, table, rec.pk)
        ld = g.leader
        if ld is None or not st.alive(ld):
            raise HTTPException(503, f"leader of {g.id} unavailable: запис неможливий")
        followers = st.live_followers(g)
        if 1 + len(followers) < w:
            raise HTTPException(503, f"write quorum unavailable: потрібно W={w}, живих реплік {1 + len(followers)}")
        r = await rcall(st, ld, "POST", "/internal/write", json={
            "op": op, "table": table, "pk": rec.pk, "sk": rec.sk or "", "value": rec.value,
            "w": w, "followers": [f.url for f in followers]})
        if r.status_code == 409:
            raise HTTPException(409, "record already exists")
        if r.status_code == 404:
            raise HTTPException(404, "record not found")
        if r.status_code == 503:
            d = r.json()["detail"]
            raise HTTPException(503, d["message"] if isinstance(d, dict) else d)
        if r.status_code != 200:
            raise HTTPException(502, f"unexpected leader response: {r.status_code}")
        body = r.json()
        response.headers.update({H_SHARD: g.id, H_LEADER: ld.id, H_VERSION: str(body["seq"]), H_ACKS: str(body["acks"])})
        return {"table": table, "pk": rec.pk, "sk": rec.sk, "value": rec.value, "version": body["seq"]}

    @app.post("/tables/{table}/records", status_code=201, dependencies=[Depends(gate)])
    async def create_record(table: str, rec: RecordIn, response: Response, w: int | None = W):
        return await write_op(table, "create", rec, w, response)

    @app.put("/tables/{table}/records", dependencies=[Depends(gate)])
    async def update_record(table: str, rec: RecordIn, response: Response, w: int | None = W):
        return await write_op(table, "update", rec, w, response)

    @app.delete("/tables/{table}/records", status_code=204, dependencies=[Depends(gate)])
    async def delete_record(table: str, pk: str = PK, sk: str | None = SK, w: int | None = W):
        resp = Response(status_code=204)
        await write_op(table, "delete", RecordIn(pk=pk, sk=sk, value=None), w, resp)
        return resp

    # ── читання з кворуму ──
    async def read_one(table: str, pk: str, sk: str | None, r: int | None, response: Response):
        t = table_or_404(table)
        check_key(t, sk)
        r = quorum_arg(r, st.r, "r")
        g = pick_group(st, table, pk)
        answers, used = await quorum_fetch(st, g, "/internal/record", {"table": table, "pk": pk, "sk": sk or ""}, r)
        best = max(answers, key=lambda a: a["version"])
        response.headers.update({H_SHARD: g.id, H_READERS: ",".join(x.id for x in used), H_VERSION: str(best["version"])})
        return best

    @app.get("/tables/{table}/records/exists", dependencies=[Depends(gate)])
    async def exists(table: str, response: Response, pk: str = PK, sk: str | None = SK, r: int | None = R):
        best = await read_one(table, pk, sk, r, response)
        return {"exists": best["found"]}

    @app.get("/tables/{table}/records", dependencies=[Depends(gate)])
    async def read_record(table: str, response: Response, pk: str = PK, sk: str | None = SK, r: int | None = R):
        best = await read_one(table, pk, sk, r, response)
        if not best["found"]:
            raise HTTPException(404, "record not found")
        return {"table": table, "pk": pk, "sk": sk, "value": best["value"], "version": best["version"]}

    @app.get("/tables/{table}/query", dependencies=[Depends(gate)])
    async def query_partition(
        table: str, response: Response, pk: str = PK,
        sk_prefix: str = Query("", max_length=512),
        limit: int = Query(100, ge=1, le=1000), r: int | None = R,
    ):
        t = table_or_404(table)
        if t.sort_key is None:
            raise HTTPException(400, f"table '{table}' has no sort key; use read")
        r = quorum_arg(r, st.r, "r")
        g = pick_group(st, table, pk)
        answers, used = await quorum_fetch(
            st, g, "/internal/partition",
            {"table": table, "pk": pk, "sk_prefix": sk_prefix, "limit": limit, "include_deleted": "true"}, r)
        merged: dict[str, dict] = {}
        for a in answers:
            for it in a["items"]:
                if it["sk"] not in merged or it["version"] > merged[it["sk"]]["version"]:
                    merged[it["sk"]] = it
        items = [{"sk": k, "value": v["value"], "version": v["version"]}
                 for k, v in sorted(merged.items()) if not v["deleted"]][:limit]
        response.headers.update({H_SHARD: g.id, H_READERS: ",".join(x.id for x in used)})
        return {"table": table, "pk": pk, "items": items}

    # ── службові ──
    @app.get("/health")
    async def health():
        return {"status": "ok", "shards": len(st.groups), "tables": len(st.tables),
                "replication": {"N": st.n, "W": st.w, "R": st.r, "strong": st.r + st.w > st.n}}

    # ── таблиці ──
    @app.post("/tables", status_code=201)
    async def create_table(t: TableDef):
        async with st.admin_lock:
            if t.name in st.tables:
                raise HTTPException(409, f"table '{t.name}' already exists")
            st.tables[t.name] = t
            st.save()
        return t

    @app.get("/tables")
    async def list_tables():
        return list(st.tables.values())

    @app.get("/tables/{table}")
    async def get_table(table: str):
        return table_or_404(table)

    # ── шарди й репліки ──
    @app.post("/shards")
    async def register_replica(body: ReplicaIn, response: Response):
        url = body.url.rstrip("/")
        g = st.groups.get(body.shard_id)
        rep = g.replicas.get(body.replica_id) if g else None
        info = {"shard_id": body.shard_id, "replica_id": body.replica_id, "role": body.role}
        if rep and rep.url == url and rep.role == body.role:
            rep.last_seen, rep.ready, rep.applied = time.monotonic(), body.ready, body.applied_seq
            return {**info, "created": False, "rebalance": None}

        h = await rcall(st, Replica(body.replica_id, url, body.role), "GET", "/health")
        if h.status_code != 200:
            raise HTTPException(502, f"unexpected replica response: {h.status_code}")
        async with st.admin_lock:
            g = st.groups.setdefault(body.shard_id, Group(body.shard_id))
            if body.role == "leader" and g.leader and g.leader.id != body.replica_id:
                raise HTTPException(409, f"shard {g.id} already has leader {g.leader.id}")
            if len(g.replicas) >= st.n and body.replica_id not in g.replicas:
                raise HTTPException(409, f"shard {g.id} already has N={st.n} replicas")
            new_leader_shard = body.role == "leader" and g.id not in st.ring.shards()
            g.replicas[body.replica_id] = Replica(
                body.replica_id, url, body.role, time.monotonic(), body.ready, body.applied_seq)
            st.save()
            if not new_leader_shard:
                response.status_code = 201
                return {**info, "created": True, "rebalance": None}
            st.ready.clear()
            try:
                st.ring.add_shard(g.id)
                report = await rebalance(st)
            except HTTPException as e:
                raise HTTPException(500, f"shard registered, but rebalance failed ({e.detail}); retry POST /rebalance")
            finally:
                st.ready.set()
            log.info("shard %s joined: %s", g.id, report)
            response.status_code = 201
            return {**info, "created": True, "rebalance": report}

    def replica_view(rep: Replica) -> dict:
        return {"id": rep.id, "url": rep.url, "role": rep.role, "alive": st.alive(rep),
                "ready": rep.ready, "applied_seq": rep.applied}

    @app.get("/shards")
    async def list_shards():
        return [{"id": g.id, "leader": g.leader.id if g.leader else None,
                 "replicas": [replica_view(r) for r in sorted(g.replicas.values(), key=lambda r: r.id)]}
                for g in sorted(st.groups.values(), key=lambda g: g.id)]

    @app.post("/rebalance")
    async def run_rebalance():
        async with st.admin_lock:
            st.ready.clear()
            try:
                return await rebalance(st)
            finally:
                st.ready.set()

    @app.get("/stats", dependencies=[Depends(gate)])
    async def stats():
        async def one(rep: Replica) -> tuple[str, dict]:
            base = {"role": rep.role, "alive": st.alive(rep), "ready": rep.ready, "reads_served": rep.reads}
            if not base["alive"]:
                return rep.id, base
            try:
                s = (await st.http.get(rep.url + "/stats", timeout=5)).json()
                keep = ("records", "applied_seq", "lag_bytes", "paused", "started_from_seq", "applied_since_start")
                return rep.id, {**base, **{k: s[k] for k in keep}}
            except Exception:
                return rep.id, base

        out, total = {}, 0
        for g in sorted(st.groups.values(), key=lambda g: g.id):
            reps = dict(await asyncio.gather(*(one(r) for r in sorted(g.replicas.values(), key=lambda r: r.id))))
            lead = reps.get(g.leader.id) if g.leader else None
            n = lead.get("records") if lead else None
            total += n or 0
            out[g.id] = {"leader": g.leader.id if g.leader else None, "records": n, "replicas": reps}
        return {"total": total, "shards": out}

    @app.get("/route")
    async def route(table: str, pk: str = PK):
        g = pick_group(st, table, pk)
        return {"shard_id": g.id, "leader": g.leader.id if g.leader else None,
                "replicas": [r.id for r in sorted(g.replicas.values(), key=lambda r: r.id)],
                "hash": hash64(route_key(table, pk))}

    # ── демонстрація відставання реплік ──
    async def replica_admin(replica_id: str, action: str):
        for g in st.groups.values():
            rep = g.replicas.get(replica_id)
            if rep:
                r = await rcall(st, rep, "POST", f"/internal/{action}")
                if r.status_code != 200:
                    raise HTTPException(r.status_code, r.json().get("detail", "replica error"))
                return {"replica_id": replica_id, **r.json()}
        raise HTTPException(404, f"replica '{replica_id}' not found")

    @app.post("/replicas/{replica_id}/pause")
    async def pause_replica(replica_id: str):
        return await replica_admin(replica_id, "pause")

    @app.post("/replicas/{replica_id}/resume")
    async def resume_replica(replica_id: str):
        return await replica_admin(replica_id, "resume")

    return app
