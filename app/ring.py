import bisect
import hashlib

def hash64(s: str) -> int:
    return int.from_bytes(hashlib.sha256(s.encode("utf-8")).digest()[:8], "big")

def route_key(table: str, pk: str) -> str:
    return f"{table}\x1f{pk}"

def mod_shard(key: str, shard_ids: list[str]) -> str:
    ids = sorted(shard_ids)
    return ids[hash64(key) % len(ids)]

class HashRing:
    def __init__(self, vnodes: int = 128):
        self.vnodes = vnodes
        self._ring: dict[int, str] = {}
        self._points: list[int] = []

    def _rebuild(self) -> None:
        self._points = sorted(self._ring)

    def add_shard(self, shard_id: str) -> None:
        for i in range(self.vnodes):
            self._ring.setdefault(hash64(f"{shard_id}#vn{i}"), shard_id)
        self._rebuild()

    def remove_shard(self, shard_id: str) -> None:
        self._ring = {p: s for p, s in self._ring.items() if s != shard_id}
        self._rebuild()

    def shards(self) -> list[str]:
        return sorted(set(self._ring.values()))

    def get_shard(self, key: str) -> str:
        if not self._points:
            raise LookupError("no shards registered")
        i = bisect.bisect_right(self._points, hash64(key)) % len(self._points)
        return self._ring[self._points[i]]

    def copy(self) -> "HashRing":
        r = HashRing(self.vnodes)
        r._ring = dict(self._ring)
        r._points = list(self._points)
        return r
