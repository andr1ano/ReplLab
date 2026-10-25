import asyncio
import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
log = logging.getLogger("replica")


class ReplicationError(RuntimeError):
    pass

# ───────────────────────────── лог ─────────────────────────────
class Log:

    def __init__(self, path: str, fsync: bool = True):
        self.path = path
        self.fsync = fsync
        self._fd: int | None = None

    def open_for_append(self) -> None:
        d = os.path.dirname(self.path)
        if d:
            os.makedirs(d, exist_ok=True)
        self._fd = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)

    def append(self, entry: dict) -> int:
        data = (json.dumps(entry, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        os.write(self._fd, data)
        if self.fsync:
            os.fsync(self._fd)
        return os.fstat(self._fd).st_size

    def read(self, pos: int, max_entries: int = 500) -> list[tuple[dict, int]]:
        try:
            f = open(self.path, "rb")
        except FileNotFoundError:
            return []
        out: list[tuple[dict, int]] = []
        with f:
            f.seek(pos)
            while len(out) < max_entries:
                line = f.readline()
                if not line or not line.endswith(b"\n"):
                    break
                pos += len(line)
                out.append((json.loads(line), pos))
        return out

    def size(self) -> int:
        try:
            return os.path.getsize(self.path)
        except FileNotFoundError:
            return 0

    def truncate(self, pos: int) -> None:
        with open(self.path, "r+b") as f:
            f.truncate(pos)


# ───────────────────────────── сховище ─────────────────────────────
class Store:
    def __init__(self, path: str):
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS records (
                   tbl TEXT NOT NULL, pk TEXT NOT NULL, sk TEXT NOT NULL DEFAULT '',
                   value TEXT NOT NULL,
                   version INTEGER NOT NULL,
                   deleted INTEGER NOT NULL DEFAULT 0,
                   PRIMARY KEY (tbl, pk, sk)) WITHOUT ROWID"""
        )
        self.conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v INTEGER NOT NULL)")
        self.conn.commit()
        self.applied_seq = self._meta("applied_seq")
        self.log_pos = self._meta("log_pos")

    def _meta(self, k: str) -> int:
        row = self.conn.execute("SELECT v FROM meta WHERE k=?", (k,)).fetchone()
        return row[0] if row else 0

    def apply_batch(self, items: list[tuple[dict, int]]) -> int:
        if not items:
            return 0
        with self.lock, self.conn:
            applied, n = self.applied_seq, 0
            for entry, _pos in items:
                seq = entry["seq"]
                if seq <= applied:
                    continue
                if seq != applied + 1:
                    raise ReplicationError(f"прогалина в логу: застосовано {applied}, прочитано {seq}")
                deleted = 1 if entry["op"] == "delete" else 0
                value = "null" if deleted else json.dumps(entry["value"], ensure_ascii=False)
                self.conn.execute(
                    """INSERT INTO records(tbl,pk,sk,value,version,deleted) VALUES (?,?,?,?,?,?)
                       ON CONFLICT(tbl,pk,sk) DO UPDATE SET
                         value=excluded.value, version=excluded.version, deleted=excluded.deleted""",
                    (entry["table"], entry["pk"], entry["sk"], value, seq, deleted),
                )
                applied, n = seq, n + 1
            last_pos = items[-1][1]
            self.conn.execute("INSERT OR REPLACE INTO meta VALUES ('applied_seq', ?)", (applied,))
            self.conn.execute("INSERT OR REPLACE INTO meta VALUES ('log_pos', ?)", (last_pos,))
        self.applied_seq, self.log_pos = applied, last_pos
        return n

    def row(self, table: str, pk: str, sk: str):
        with self.lock:
            return self.conn.execute(
                "SELECT value, version, deleted FROM records WHERE tbl=? AND pk=? AND sk=?",
                (table, pk, sk),
            ).fetchone()

    def partition(self, table: str, pk: str, prefix: str, limit: int, include_deleted: bool):
        sql = """SELECT sk, value, version, deleted FROM records
                 WHERE tbl=? AND pk=? AND substr(sk,1,?)=?"""
        if not include_deleted:
            sql += " AND deleted=0"
        sql += " ORDER BY sk LIMIT ?"
        with self.lock:
            rows = self.conn.execute(sql, (table, pk, len(prefix), prefix, limit)).fetchall()
        return [{"sk": sk, "value": json.loads(v), "version": ver, "deleted": bool(d)}
                for sk, v, ver, d in rows]

    def scan(self, after, limit: int):
        sql, args = "SELECT tbl, pk, sk, value FROM records WHERE deleted=0", []
        if after:
            sql += " AND (tbl, pk, sk) > (?,?,?)"
            args += list(after)
        sql += " ORDER BY tbl, pk, sk LIMIT ?"
        args.append(limit)
        with self.lock:
            rows = self.conn.execute(sql, args).fetchall()
        return [{"table": t, "pk": p, "sk": s, "value": json.loads(v)} for t, p, s, v in rows]

    def counts(self) -> dict[str, int]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT tbl, COUNT(*) FROM records WHERE deleted=0 GROUP BY tbl"
            ).fetchall()
        return dict(rows)

# ───────────────────────────── вузол ─────────────────────────────
class Node:
    def __init__(self, shard_id: str, replica_id: str, role: str, store: Store, wal: Log):
        self.shard_id, self.replica_id, self.role = shard_id, replica_id, role
        self.store, self.log = store, wal
        self.apply_lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.paused = False
        self.ready = False
        self.applied_since_start = 0
        self.started_from_seq = store.applied_seq
        self.started_from_pos = store.log_pos
        self.next_seq = 0

    def catch_up(self) -> int:
        with self.apply_lock:
            if self.paused:
                return self.store.applied_seq
            while True:
                batch = self.log.read(self.store.log_pos, 500)
                if not batch:
                    break
                self.applied_since_start += self.store.apply_batch(batch)
        return self.store.applied_seq

    def start_as_leader(self) -> None:
        self.catch_up()
        if self.log.size() > self.store.log_pos:
            log.warning("відкидаю недописаний хвіст логу (%d → %d)", self.log.size(), self.store.log_pos)
            self.log.truncate(self.store.log_pos)
        self.log.open_for_append()
        self.next_seq = self.store.applied_seq + 1
        self.ready = True

    def leader_write(self, op: str, table: str, pk: str, sk: str, value: Any):
        with self.write_lock:
            row = self.store.row(table, pk, sk)
            live = row is not None and not row[2]
            if op == "create" and live:
                return "conflict", None
            if op in ("update", "delete") and not live:
                return "not_found", None
            seq = self.next_seq
            entry = {"seq": seq, "op": "delete" if op == "delete" else "put",
                     "table": table, "pk": pk, "sk": sk, "value": value, "ts": round(time.time(), 3)}
            pos = self.log.append(entry)
            self.store.apply_batch([(entry, pos)])
            self.next_seq = seq + 1
            return "ok", seq

    def status(self) -> dict:
        return {
            "shard_id": self.shard_id, "replica_id": self.replica_id, "role": self.role,
            "ready": self.ready, "paused": self.paused,
            "applied_seq": self.store.applied_seq, "log_pos": self.store.log_pos,
            "log_size": self.log.size(), "lag_bytes": self.log.size() - self.store.log_pos,
            "started_from_seq": self.started_from_seq, "started_from_pos": self.started_from_pos,
            "applied_since_start": self.applied_since_start,
        }

# ───────────────────────────── HTTP ─────────────────────────────
class WriteReq(BaseModel):
    op: str = Field(pattern="^(create|update|upsert|delete)$")
    table: str
    pk: str
    sk: str = ""
    value: Any = None
    w: int = 1
    followers: list[str] = []

class SyncReq(BaseModel):
    upto: int

def create_app(shard_id=None, replica_id=None, role=None, db_path=None, log_path=None,
               coordinator_url=None, public_url=None) -> FastAPI:
    shard_id = shard_id or os.environ["SHARD_ID"]
    replica_id = replica_id or os.environ["REPLICA_ID"]
    role = (role or os.getenv("ROLE", "follower")).lower()
    db_path = db_path or os.getenv("DB_PATH", f"data/{replica_id}.db")
    log_path = log_path or os.getenv("LOG_PATH", f"data/{shard_id}.log")
    coordinator_url = coordinator_url or os.getenv("COORDINATOR_URL")
    public_url = public_url or os.getenv("PUBLIC_URL")
    poll = float(os.getenv("POLL_INTERVAL", "0.2"))
    heartbeat = float(os.getenv("HEARTBEAT_INTERVAL", "2"))
    ack_timeout = float(os.getenv("ACK_TIMEOUT", "3"))
    assert role in ("leader", "follower"), "ROLE має бути leader або follower"

    store = Store(db_path)
    wal = Log(log_path, fsync=os.getenv("LOG_FSYNC", "1") != "0")
    node = Node(shard_id, replica_id, role, store, wal)
    log.info("%s (%s) старт: applied_seq=%d log_pos=%d, розмір логу=%d → продовжую з байта %d",
             replica_id, role, store.applied_seq, store.log_pos, wal.size(), store.log_pos)
    if role == "leader":
        node.start_as_leader()
        log.info("%s: лідер готовий, наступний seq=%d", replica_id, node.next_seq)

    http: dict[str, httpx.AsyncClient] = {}
    background: set[asyncio.Task] = set()

    async def notify(url: str, seq: int) -> bool:
        try:
            r = await http["c"].post(f"{url}/internal/sync", json={"upto": seq}, timeout=ack_timeout)
            return r.status_code == 200 and r.json()["applied"] >= seq
        except Exception:
            return False

    async def tail_loop(wake_ev: asyncio.Event):
        while True:
            try:
                await asyncio.to_thread(node.catch_up)
                if not node.ready and not node.paused:
                    node.ready = True
                    log.info("%s: наздогнав лог (applied_seq=%d) — готовий до читань",
                             replica_id, store.applied_seq)
                    wake_ev.set()
            except Exception:
                log.exception("помилка застосування логу")
            await asyncio.sleep(poll)

    async def heartbeat_loop(wake_ev: asyncio.Event):
        ok = False
        while True:
            payload = {"shard_id": shard_id, "replica_id": replica_id, "url": public_url,
                       "role": role, "ready": node.ready, "applied_seq": store.applied_seq}
            try:
                r = await http["c"].post(f"{coordinator_url}/shards", json=payload, timeout=10)
                r.raise_for_status()
                if not ok:
                    log.info("%s: зареєстровано в %s", replica_id, coordinator_url)
                ok = True
            except Exception as e:
                ok = False
                log.warning("реєстрація не вдалась (%s), повторюю", e)
            try:
                await asyncio.wait_for(wake_ev.wait(), timeout=heartbeat if ok else 1)
            except asyncio.TimeoutError:
                pass
            wake_ev.clear()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        http["c"] = httpx.AsyncClient()
        wake_ev = asyncio.Event()
        tasks = []
        if role == "follower":
            tasks.append(asyncio.create_task(tail_loop(wake_ev)))
        if coordinator_url and public_url:
            tasks.append(asyncio.create_task(heartbeat_loop(wake_ev)))
        yield
        for t in tasks:
            t.cancel()
        await http["c"].aclose()

    app = FastAPI(title=f"KV replica {replica_id} (internal)", lifespan=lifespan)

    def need(r: str):
        if node.role != r:
            raise HTTPException(409, f"ця репліка — {node.role}, а не {r}")

    @app.get("/health")
    def health():
        return {"status": "ok", "shard_id": shard_id, "replica_id": replica_id, "role": role}

    @app.get("/internal/status")
    def status():
        return node.status()

    @app.get("/stats")
    def stats():
        by_table = store.counts()
        return {**node.status(), "records": sum(by_table.values()), "tables": by_table}

    # ── лідер: запис ──
    @app.post("/internal/write")
    async def write(req: WriteReq):
        need("leader")
        status, seq = await asyncio.to_thread(
            node.leader_write, req.op, req.table, req.pk, req.sk, req.value)
        if status == "conflict":
            raise HTTPException(409, "record already exists")
        if status == "not_found":
            raise HTTPException(404, "record not found")

        tasks = [asyncio.create_task(notify(u, seq)) for u in req.followers]
        acks, pending = 1, set(tasks)
        deadline = time.monotonic() + ack_timeout
        while pending and acks < req.w:
            done, pending = await asyncio.wait(
                pending, timeout=max(0.0, deadline - time.monotonic()),
                return_when=asyncio.FIRST_COMPLETED)
            if not done:
                break
            acks += sum(1 for t in done if t.result())
        for t in pending:
            background.add(t)
            t.add_done_callback(background.discard)
        if acks < req.w:
            raise HTTPException(503, detail={
                "message": f"кворум запису не досягнуто: {acks} з {req.w}. Запис є в логу "
                           f"й на лідері, відстаючі репліки дочитають його самі",
                "seq": seq, "acks": acks})
        return {"seq": seq, "acks": acks}

    @app.get("/internal/scan")
    def scan(limit: int = Query(500, ge=1, le=5000), after_table: str | None = None,
             after_pk: str | None = None, after_sk: str | None = None):
        need("leader")
        after = (after_table, after_pk, after_sk) if None not in (after_table, after_pk, after_sk) else None
        return {"items": store.scan(after, limit)}

    # ── фолловер: сповіщення «дочитай лог до upto» ──
    @app.post("/internal/sync")
    async def sync(req: SyncReq):
        applied = await asyncio.to_thread(node.catch_up)
        return {"applied": applied}

    # ── читання (будь-яка репліка) ──
    @app.get("/internal/record")
    def record(table: str, pk: str, sk: str = ""):
        row = store.row(table, pk, sk)
        base = {"replica": replica_id, "applied": store.applied_seq}
        if row is None:
            return {**base, "found": False, "version": 0, "value": None}
        value, version, deleted = row
        return {**base, "found": not deleted, "version": version,
                "value": None if deleted else json.loads(value)}

    @app.get("/internal/partition")
    def partition(table: str, pk: str, sk_prefix: str = "", limit: int = Query(100, ge=1, le=1000),
                  include_deleted: bool = False):
        return {"replica": replica_id, "items": store.partition(table, pk, sk_prefix, limit, include_deleted)}

    # ── демонстрація відставання ──
    @app.post("/internal/pause")
    def pause():
        need("follower")
        node.paused = True
        return {"paused": True, "applied_seq": store.applied_seq}

    @app.post("/internal/resume")
    def resume():
        need("follower")
        node.paused = False
        return {"paused": False, "applied_seq": store.applied_seq}

    return app
