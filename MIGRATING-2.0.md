# Migrating to Lucid Stream 2.0

Lucid Stream 2.0 is a clean application API cut with an in-place database upgrade.
The event and aggregate table formats do not change, and existing projection checkpoints
remain valid starting positions. Applications must replace their old notification-driven
event sorter with `PgEventChaser`.

## 1. Apply the database migration

Apply `lucidstream-pg/migrations/20260903000000_safe_event_chasing.sql` before deploying
the 2.0 application.

Applications that run `lucidstream_pg::EMBEDDED_MIGRATE` receive it automatically.
Applications that own a separate migration history must copy its SQL into a new,
application-owned timestamped migration. Never edit an already-applied Lucid Stream
migration.

The migration is safe to apply while a 1.x application is running:

- Existing `*_commit` procedures and the `BIGSERIAL` write path are unchanged.
- `ls_check_sequence_integrity` remains callable by 1.x, but validates `CACHE 1`
  instead of rewinding a sequence.
- Existing event sequences are not scanned or modified. `BIGSERIAL` creates them
  with PostgreSQL's default `CACHE 1`, and 2.0 verifies that invariant at startup.

The migration does not lock or rewrite event tables, renumber positions, or add tables.

## 2. Replace the application event sorter

The following 1.x chase APIs have been removed so an unsafe chaser cannot silently carry
forward:

- `select_latest_sequence`
- `Repo::select_latest_sequence`
- `Repo::check_sequence_integrity`
- `LucidNotification`
- `From<PgNotification> for QueryEvent`

Construct `PgEventChaser` from the projection's existing durable checkpoint. Notifications
are wake-up hints; only events queried from a certified safe window are delivered.

```rust,ignore
let checkpoint = load_projection_checkpoint(&pool, projection_id).await?;
let mut chaser = PgEventChaser::<Event, Metadata>::connect(
    &pool,
    "wallet",
    checkpoint,
).await?;

loop {
    let event = chaser.next().await?;
    let sequence = event.sequence;

    // This transaction must atomically apply the projection changes and
    // persist event.sequence as the projection checkpoint.
    apply_event_and_checkpoint(&pool, projection_id, event).await?;

    // Acknowledgement is in-memory and must happen only after the database commit.
    chaser.acknowledge(sequence)?;
}
```

Only one active chaser may own a given projection checkpoint. Application-level advisory
locks or leader election remain the application's responsibility.

## 3. Cut over application instances

After the database migration succeeds, deploy the 2.0 application instances using the new
chaser. A rolling deployment is acceptable when application-level ownership ensures that
old and new chasers do not process the same projection concurrently.

Existing `LISTEN/NOTIFY` triggers can remain unchanged. Full event payloads are ignored by
the new chaser; they can be replaced with sparse notifications in a later migration.

## Historical projection state

The new chaser prevents lower committed events from appearing behind future checkpoints.
It cannot prove that a 1.x projection did not already miss an event. Rebuilding a projection
from sequence zero is required when historical correctness must be guaranteed. Reusing its
existing checkpoint accepts that historical risk while protecting all events after cutover.

## YBB

YBB runs only `backend-rs/domain/migrations/domain`, not Lucid Stream's embedded migrations.
Add the small `ls_check_sequence_integrity` replacement as a new YBB migration, then replace
the local orderbook and wallet sorter implementations with `PgEventChaser`. No YBB event
sequence or table needs to be altered when its sequence still has the default `CACHE 1`.

Lucid Stream 2.0 currently uses SQLx 0.9. An application using SQLx 0.8 must upgrade SQLx at
the same time or Lucid Stream must be built against SQLx 0.8; pool and connection types from
the two major SQLx versions are not interchangeable.
