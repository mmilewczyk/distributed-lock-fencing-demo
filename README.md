# distributed-lock-fencing-demo

Practical comparison of two distributed locking strategies applied to a **fintech payment transfer** scenario. Both
applications use an intentionally unsafe **read-modify-write** pattern - the only difference is what happens to a stale
write when the lock expires.

|                              | redisson-app (port 8081)     | curator-app (port 8082)             |
|------------------------------|------------------------------|-------------------------------------|
| **Lock mechanism**           | TTL key in Redis             | Ephemeral znode in ZooKeeper        |
| **Fencing token**            | ❌ Not natively provided      | ✅ ZK zxid (monotonic globally)      |
| **Session semantics**        | ❌ TTL expiry - silent        | ✅ Ephemeral node deletion           |
| **Safe under GC pause?**     | ❌ No - stale write accepted  | ✅ Yes - PostgreSQL rejects it       |
| **Storage-layer protection** | ❌ Impossible without a token | ✅ `WHERE last_fence_token < :token` |

---

## The Problem

The transfer uses a **read-modify-write** pattern:

1. **READ** `currentBalance` from PostgreSQL into JVM memory
2. **MODIFY** - compute `newBalance = currentBalance - amount` in Java
3. **WRITE** `newBalance` back with no conditional WHERE clause

A distributed lock serializes callers so Step 1 of Process B only runs after Process A completes Step 3. This is safe
under normal conditions.

**When a GC pause causes the Redis lock TTL to expire:**

```
Process A:  READ balance=1000 ---- GC pause 6s ---- WRITE balance=700   <- stale, overwrites B
                                        │
                                  TTL (5s) expires
                                        │
Process B:                        READ balance=1000 -- WRITE balance=700 <- correct
```

Both processes write `700`. Expected final balance: `400 PLN`. Actual: `700 PLN`. **300 PLN vanished silently.**

`isHeldByCurrentThread()` still returns `true` in Process A - it checks local JVM state, not Redis. There is no
callback, no exception, no signal. The write goes through.

## The Fix: Fencing Token

Apache Curator + ZooKeeper solve this by adding a monotonically increasing token to the PostgreSQL write:

```sql
UPDATE accounts
SET balance          = :newBalance,
    last_fence_token = :fencingToken
WHERE account_id = :id
  AND last_fence_token < :fencingToken; -- reject stale write atomically
```

```
Process A:  READ balance=1000, token=42 ---- GC pause ---- WRITE token=42 -> 0 rows (42 < 58) ✅ REJECTED
                                                  │
                                        ZK session expires
                                                  │
Process B:                              READ balance=1000, token=58 -- WRITE token=58 -> 1 row ✅ ACCEPTED
```

The decision lives in PostgreSQL - not in the application layer. The application never needs to ask "is my lock still
valid?"

---

## Quick Start

```bash
docker compose up -d
```

Seed accounts on both apps:

```bash
curl -X POST "http://localhost:8081/api/accounts?accountId=ACC-001&balance=1000"
curl -X POST "http://localhost:8081/api/accounts?accountId=ACC-002&balance=0"
curl -X POST "http://localhost:8082/api/accounts?accountId=ACC-001&balance=1000"
curl -X POST "http://localhost:8082/api/accounts?accountId=ACC-002&balance=0"
```

### Normal transfer

```bash
# Redis - no fencing
curl -X POST http://localhost:8081/api/transfers \
  -H "Content-Type: application/json" \
  -d '{"sourceAccountId":"ACC-001","targetAccountId":"ACC-002","amount":300}'

# ZooKeeper - with fencing token
curl -X POST http://localhost:8082/api/transfers \
  -H "Content-Type: application/json" \
  -d '{"sourceAccountId":"ACC-001","targetAccountId":"ACC-002","amount":300}'
```

Response from `curator-app` includes the fencing token:

```json
{
  "status": "COMPLETED",
  "lockStrategy": "ZK_FENCING_TOKEN",
  "fencingToken": 42
}
```

### GC pause simulation - the key demo

Send **identical requests** to both apps with `gcPauseMs > 5000` (default lock TTL).

```bash
# redisson-app - demonstrates the lost update
curl -X POST http://localhost:8081/api/transfers/gc-pause \
  -H "Content-Type: application/json" \
  -d '{"sourceAccountId":"ACC-001","targetAccountId":"ACC-002","amount":300,"gcPauseMs":6000}'
```

```json
{
  "processAStatus": "COMPLETED",
  "processBStatus": "COMPLETED",
  "balanceExpected": 400.00,
  "balanceActual": 700.00,
  "moneyLostPln": 300.00,
  "lostUpdateOccurred": true,
  "explanation": "⚠️ LOST UPDATE detected. Both processes wrote the same stale newBalance..."
}
```

```bash
# curator-app - same payload, fencing makes it safe
curl -X POST http://localhost:8082/api/transfers/gc-pause \
  -H "Content-Type: application/json" \
  -d '{"sourceAccountId":"ACC-001","targetAccountId":"ACC-002","amount":300,"gcPauseMs":6000}'
```

```json
{
  "processAStatus": "REJECTED_STALE_LOCK",
  "processBStatus": "COMPLETED",
  "processAFencingToken": 42,
  "processBFencingToken": 58,
  "moneyLostPln": 0.00,
  "lostUpdateOccurred": false,
  "explanation": "✅ Fencing token protected against lost update..."
}
```

`processAFencingToken < processBFencingToken` - proves ZK zxid monotonicity.

### Inspect account state

```bash
curl http://localhost:8082/api/accounts/ACC-001
```

```json
{
  "accountId": "ACC-001",
  "balance": 700.00,
  "lastFenceToken": 58,
  "updatedAt": "2025-01-15T14:23:01Z"
}
```

---

## Running the Tests

### curator-lock - no Docker needed (embedded ZK via `curator-test`)

```bash
./mvnw test -pl curator-lock
```

Key tests:

| Test                                                    | What it proves                                         |
|---------------------------------------------------------|--------------------------------------------------------|
| `staleFencingToken_rejectedByPostgres_notByApplication` | The guard lives in SQL `WHERE`, not in Java            |
| `staleFencingToken_serviceReturnsStaleLockResult`       | Service returns `RejectedByFencingToken` end-to-end    |
| `fencingTokens_areMonotonicallyIncreasing`              | ZK zxid strictly increases across sequential transfers |
| `concurrentTransfers_balanceConservation`               | 8 threads - total funds always equal initial 1000 PLN  |

### redisson-app and curator-app - requires Docker (PostgreSQL via Testcontainers)

```bash
./mvnw test -pl redisson-app
./mvnw test -pl curator-app
```

### All modules

```bash
./mvnw test
```

---

## How the Guard Works

**`AccountRepository.setBalanceWithFencingToken`** (curator-lock):

```java
int rows = jdbc.update("""
		UPDATE accounts
		SET    balance          = ?,
		       last_fence_token = ?
		WHERE  account_id       = ?
		  AND  last_fence_token < ?          -- reject stale write
		""", newBalance, fencingToken, accountId, fencingToken);

if(rows ==0){
		throw new

StaleFencingTokenException(accountId, fencingToken);
}
```

The `WHERE last_fence_token < :token` clause is **atomic with the UPDATE** inside a single SQL statement. Two concurrent
UPDATEs on the same row are serialized by PostgreSQL row-level locking. The one with the lower token loses - always.

Compare with the unsafe write in `redisson-app`:

```java
// No guard. If Process B already wrote 700, this overwrites it with the same 700.
jdbc.update("UPDATE accounts SET balance = ? WHERE account_id = ?",
            newBalance, accountId);
```

---

## Why Not Redlock?

Redlock improves availability but does **not** solve the fencing token problem:

- It still relies on TTL expiry - a GC pause longer than the TTL can still produce two simultaneous "lock holders"
- Clock drift across Redis nodes can cause incorrect TTL calculations
- Redlock does not define fencing token semantics - this is not an oversight, it is a fundamental architectural
  constraint of the approach

---

## Tech Stack

- Java 25, Spring Boot 4.x
- Redisson 3.29 (Redis client)
- Apache Curator 5.6 (ZooKeeper client)
- PostgreSQL 16 (real conditional UPDATEs - no in-memory mocks)
- Testcontainers 1.19 (PostgreSQL + Redis in tests)
- Curator `TestingServer` (embedded ZooKeeper - no Docker for ZK)

## References

- [Martin Kleppmann - How to do distributed locking (2016)](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)