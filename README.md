# Lucid Stream

Lucid Stream is a small Rust event-sourcing library with PostgreSQL and in-memory
implementations. The PostgreSQL writer keeps the fast `BIGSERIAL` event position path,
while its event chaser safely handles the permanent sequence gaps created by transaction
rollbacks, failed inserts, backend crashes, or database overload.

```toml
[dependencies]
lucidstream = "2.1"
lucidstream-pg = "2.1"
```

## PostgreSQL setup

Run the embedded migrations before creating a domain, then construct the repository:

```rust,ignore
use lucidstream_pg::{Repo, init_domain, migrate};

migrate(&pool).await?;
init_domain(&pool, "wallet").await?;
let repo = Repo::new(pool.clone(), "wallet").await?;
```

`Repo::new` verifies that the event table owns a sequence with `CACHE 1`. PostgreSQL uses
`CACHE 1` for `BIGSERIAL` by default. Lucid Stream never rewinds, repairs, or renumbers that
sequence; gaps are valid event positions at which no event exists.

## Gap-safe event chasing

Use `PgEventChaser` for projections. It owns Lucid Stream's event-sorting state machine and
returns committed events in increasing sequence order, while permitting permanent numerical
gaps.

```rust,ignore
use lucidstream_pg::PgEventChaser;

let checkpoint = load_projection_checkpoint(&pool, "wallet_balance").await?;
let mut chaser = PgEventChaser::<WalletEvent, EventMetadata>::connect(
    &pool,
    "wallet",
    checkpoint,
).await?;

loop {
    let event = chaser.next().await?;
    let sequence = event.sequence;

    let mut transaction = pool.begin().await?;
    apply_wallet_projection(&mut transaction, &event).await?;
    save_projection_checkpoint(
        &mut transaction,
        "wallet_balance",
        sequence,
    ).await?;
    transaction.commit().await?;

    // Acknowledge only after the projection and durable checkpoint commit.
    chaser.acknowledge(sequence)?;
}
```

The durable checkpoint is the most recent event sequence successfully handled by that
projection. Applications must not enforce `sequence == checkpoint + 1`: a missing number can
be a permanent sequence gap. The chaser decides when it is safe to pass such a gap.

Only one active chaser may own a projection checkpoint. Use an application-level advisory
lock or leader election when multiple application instances can run the same projection.

## Why no event can be missed

The chaser captures a **safe head** `H` using one short PostgreSQL transaction:

1. Begin a `READ COMMITTED` transaction.
2. Acquire `LOCK TABLE <domain>_events IN SHARE MODE`.
3. After the lock is acquired, read the greatest committed event sequence.
4. Commit immediately, then query actual rows in `checkpoint < sequence <= H` order.

An event insert holds a conflicting `ROW EXCLUSIVE` table lock. Therefore the `SHARE` lock
waits for every already-started insert to commit or roll back before the chaser reads `H`.
With `CACHE 1`, no backend can retain an unused lower sequence value and insert it after the
safe-head transaction releases its lock. Any new writer must receive a sequence greater than
`H`.

Consequently, after capturing `H`:

- every committed event at or below `H` is visible and queried;
- every absent sequence at or below `H` is a permanent gap;
- no lower event can appear after the projection advances through `H`.

PostgreSQL `LISTEN/NOTIFY` is only a low-latency wake-up hint. Notification payloads are not
processed as authoritative events. A periodic reconciliation also captures a new safe head,
so a disconnected listener or lost notification delays processing but cannot cause an event
to be skipped.

The guarantee requires the event table to remain append-only and use its owned `CACHE 1`
sequence. Do not preallocate positions, insert explicit positions, update event positions, or
delete events behind a running chaser.

## Design decision: retain `BIGSERIAL`

A gapless transactional counter can make every failed transaction reuse its position, but it
also serializes concurrent writers on one counter row. Lucid Stream instead keeps PostgreSQL's
non-transactional `BIGSERIAL` allocation and moves gap safety to the read side with safe-head
capture. The normal write path receives no additional counter-table write.

A representative local benchmark compared the two write paths:

| Position allocator | Median throughput | Relative to `BIGSERIAL` |
| --- | ---: | ---: |
| PostgreSQL `BIGSERIAL` | 86,181 events/s | baseline |
| Transactional counter row | 25,763 events/s | -70.1% |

The workload used PostgreSQL 13.23 with `synchronous_commit`, `fsync`, and full-page writes
enabled, 32 concurrent writers, 10 events per commit, 10,000 commits per run, and three
repetitions on an Apple M3 Max. These numbers are a local design comparison rather than a
universal PostgreSQL capacity claim; application hardware, schema, event size, indexes, and
durability configuration will change absolute throughput.

The safe-head operation can briefly wait behind an open writer. That wait is intentional: it
is the proof that an absent lower position is final. Under normal successful operation,
sequences are still contiguous; gaps appear only when a sequence was allocated but its event
was not committed.

## Migrating from 1.x

Lucid Stream 2.0 removes the old dense-sequence chaser APIs. Existing event and aggregate
tables remain compatible, but applications must replace sorters that enforce `last + 1` and
must apply the safe-chasing migration. See the
[2.0 migration guide](https://github.com/dt665m/lucidstream-rs/blob/master/MIGRATING-2.0.md).
