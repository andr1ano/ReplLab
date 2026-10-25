# Sharded KV store з реплікацією

Координатор (єдина точка входу, REST) + шарди; кожен шард — група з 3 реплік (лідер + 2 фолловери, SQLite).
Python 3.12, FastAPI, Docker Compose.

```
клієнт ─► coordinator ── hash(table, pk) → consistent hashing → шард
              │             запис → лідер (кворум W)    читання → R живих реплік по колу
      ┌───────┴────────┐
   shard-1 (L F F)   shard-2 (L F F)   [shard-3 (L F F) — profile scale]
   лідер дописує в shard.log, фолловери читають його й застосовують
```

## Запуск

```bash
docker compose up -d --build           # координатор + shard-1, shard-2 (по 3 репліки)
docker compose --profile scale up -d   # + shard-3: частина ключів переїжджає на нього
docker compose down -v                 # зупинити й стерти томи
```

Swagger UI: http://localhost:8000/docs · специфікація: `openapi.yaml`.

## Швидка демонстрація

```bash
B=localhost:8000
curl -s $B/shards | python3 -m json.tool          # 3 репліки на шард: leader + 2 follower
curl -s -XPOST $B/tables -H 'content-type: application/json' -d '{"name":"users","partition_key":"user_id"}'

# запис через лідера: x-leader, x-version (= seq у логу), x-acks (скільки реплік підтвердили)
curl -si -XPOST $B/tables/users/records -H 'content-type: application/json' -d '{"pk":"alice","value":{"age":30}}'

# читання по всіх репліках (x-read-replicas обертається r1→r2→r3), R=1
for i in 1 2 3 4 5 6; do curl -si "$B/tables/users/records?pk=alice&r=1" | grep -i x-read-replicas; done

# відставання + кворум: r3 «заморожуємо» (підставте шард ключа з /route), пишемо, читаємо з R=1 і R=2
curl -s "$B/route?table=users&pk=alice"
curl -s -XPOST $B/replicas/shard-1-r3/pause
curl -s -XPUT "$B/tables/users/records?w=2" -H 'content-type: application/json' -d '{"pk":"alice","value":{"age":99}}'
for i in 1 2 3 4 5 6; do curl -s "$B/tables/users/records?pk=alice&r=1"; echo; done   # іноді стара версія
for i in 1 2 3 4 5 6; do curl -s "$B/tables/users/records?pk=alice&r=2"; echo; done   # завжди свіжа (R+W>N)
curl -s -XPOST $B/replicas/shard-1-r3/resume

# offset: фолловер лежить, пишемо, піднімаємо — він дочитує лише нове
docker compose stop shard1-r2
curl -s -XPOST "$B/tables/users/records?w=1" -H 'content-type: application/json' -d '{"pk":"bob","value":1}'
docker compose start shard1-r2; sleep 3
curl -s $B/stats | python3 -m json.tool     # started_from_seq, applied_since_start у реплік
```

## Обмеження

* Немає автоматичного вибору лідера: упав лідер → записи в шард 503, читання працюють з фолловерів.
* Лог — файл у спільному томі (на різних машинах потрібен мережевий диск або брокер); без compaction.
* Один координатор (каталог у файлі, після рестарту відновлюється; репліки перереєструються heartbeat-ом).
