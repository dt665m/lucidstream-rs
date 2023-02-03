-- Sets up a trigger for the given table to automatically set a column called `updated_at` whenever the row is modified (unless `updated_at` was included
-- in the modified columns)
--
-- # Example
--
-- ```sql
-- CREATE TABLE users (id SERIAL PRIMARY KEY, updated_at TIMESTAMP NOT NULL DEFAULT NOW());
--
-- SELECT ls_manage_updated_at('users');
-- ```
CREATE OR REPLACE FUNCTION ls_manage_updated_at(_tbl regclass) RETURNS VOID
AS $$
BEGIN
	EXECUTE format
	('CREATE TRIGGER set_updated_at BEFORE UPDATE ON %s
                    FOR EACH ROW EXECUTE PROCEDURE ls_set_updated_at()', _tbl);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ls_set_updated_at() RETURNS TRIGGER
AS $$
BEGIN
	IF (
        NEW IS DISTINCT FROM OLD AND
        NEW.updated_at IS NOT DISTINCT FROM OLD.updated_at
    ) THEN
        NEW.updated_at := current_timestamp;
END
IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION md5short(text) returns INT
AS $$
 SELECT ('x'||substr(md5($1),1,8))::BIT(32)::INT;
$$ LANGUAGE sql;

-- Resets the BIGSERIAL sequence of an aggregate's event table
-- when an application error causes a failure to store events in the 'commit_proc' function
-- or any other reason.  Reverted sequences are problematic because event chasers 
-- use the event sequence to keep track of tasks that are already done.
-- #TODO probably should create a way to also find holes and shift events to fill the hole.
CREATE OR REPLACE FUNCTION ls_check_sequence_integrity(_domain text) RETURNS VOID
AS $$
DECLARE
    events_table TEXT;
    sequence_table TEXT;
    last_seq BIGINT;
    current_seq BIGINT;
BEGIN
    events_table := format('%s_events', _domain);
    sequence_table := format('%s_sequence_seq', events_table);

    EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', events_table);
    EXECUTE format('SELECT last_value FROM %s', sequence_table) INTO last_seq;
    EXECUTE format('SELECT sequence FROM %s ORDER BY sequence DESC LIMIT 1', events_table) INTO current_seq ;
    IF last_seq != current_seq THEN
        RAISE LOG E'last sequence: %, current successful commit: %', last_seq, current_seq;
        EXECUTE format('ALTER SEQUENCE %s RESTART WITH %s', sequence_table, current_seq+1);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Creates an events table for a specific aggregate kind;
-- param `_table` is the created table's name
--
-- # Example
--
-- ```sql
-- SELECT ls_new_events_table('some_events');
-- ```
CREATE OR REPLACE FUNCTION ls_new_events_table(_table text) RETURNS VOID
AS $$
BEGIN
	EXECUTE format
	('CREATE TABLE IF NOT EXISTS %s 
		(
            sequence BIGSERIAL,
            aggregate_id TEXT NOT NULL,
            id UUID NOT NULL,
            version BIGINT NOT NULL,
            md5short INT NOT NULL,
            data JSONB NOT NULL
		)', _table);

	EXECUTE format
	('CREATE UNIQUE INDEX IF NOT EXISTS %1$s_optimistic_concurrency_idx ON %1$s (aggregate_id, version)', _table);
	EXECUTE format
	('CREATE UNIQUE INDEX IF NOT EXISTS %1$s_id ON %1$s (id)', _table);
END;
$$ LANGUAGE plpgsql;

-- Creates an an aggregate table;
-- param `_table` is the created table's name
--
-- # Example
--
-- ```sql
-- SELECT ls_new_aggregates_table('some_aggregates');
-- ```
CREATE OR REPLACE FUNCTION ls_new_aggregates_table(_table text) RETURNS VOID
AS $$
BEGIN
	EXECUTE format
	('CREATE TABLE IF NOT EXISTS %s 
		(
            aggregate_id TEXT PRIMARY KEY,
            version BIGINT NOT NULL,
            current_state JSONB NOT NULL
        )', _table);
END;
$$ LANGUAGE plpgsql;

-- Creates the stored procedure that enables the event sourcing + projection system to work for any given type of aggregate.
-- These are the magic rules we are enforcing at the data entry level to enable the event sourcing paradigm.
-- Guarantees:
-- 	- expect_version == saved_version for optimistic concurrency
--  - atomic+transactional storage of the new_state and new_events, keeping the two synchronized
--
-- # Example
--
-- ```sql
-- DROP TABLE IF EXISTS ab_events;
-- DROP TABLE IF EXISTS ab_aggregates;
-- SELECT ls_new_commit_proc('test');
-- SELECT * FROM test_events_sequence_seq;
-- CALL commit_test('3a7973b7-46fc-4b0b-a6c9-6bce97c67c93', 0, 2, '{"hello": "world"}'::jsonb, ARRAY['{"what": "the_hell", "kind": "fun", "version": 1, "id": "a"}', '{"what": "is_this", "kind": "pain", "version": 2, "id": "b"}']::jsonb[], ARRAY['77569696-d2e2-464c-b6a2-795dcbe4ba91'::uuid, '6237ff76-506d-4ccf-af12-c94fc2f4c329'::uuid]);
-- SELECT * FROM test_events_sequence_seq;
-- CALL commit_test('3a7973b7-46fc-4b0b-a6c9-6bce97c67c93', 2, 4, '{"hello": "world"}'::jsonb, ARRAY['{"what": "the_hell", "kind": "fun", "version": 1, "id": "a"}', '{"what": "is_this", "kind": "pain", "version": 2, "id": "b"}']::jsonb[], ARRAY['77569696-d2e2-464c-b6a2-795dcbe4ba91'::uuid, '6237ff76-506d-4ccf-af12-c94fc2f4c329'::uuid]);
-- SELECT * FROM test_events_sequence_seq;
-- SELECT ls_check_sequence_integrity('test');
-- SELECT * FROM test_events_sequence_seq;
-- CALL commit_test('3a7973b7-46fc-4b0b-a6c9-6bce97c67c93', 2, 4, '{"hello": "world"}'::jsonb, ARRAY['{"what": "the_hell", "kind": "fun", "version": 3, "id": "a"}', '{"what": "is_this", "kind": "pain", "version": 4, "id": "b"}']::jsonb[], ARRAY['91bff2c9-ac9f-4335-8d44-a2c202dd76af'::uuid, '37bb5d8b-76f8-447e-bbe5-c6ccaf818c3f'::uuid]);
-- ```
CREATE OR REPLACE FUNCTION ls_new_commit_proc(_domainName text) RETURNS VOID 
AS $outer$
BEGIN
    PERFORM ls_new_aggregates_table(_domainName || '_aggregates');
    PERFORM ls_new_events_table(_domainName || '_events');

	EXECUTE format
	('CREATE OR REPLACE PROCEDURE %1$s_commit(
            expect_aggregate_id TEXT, expect_version BIGINT, updated_version BIGINT, state JSONB, events JSONB[], event_ids UUID[]
        ) AS $inner$

			DECLARE
				saved_version BIGINT;
                event_len INT;
				new_event JSONB;
                new_event_id UUID;
			BEGIN
                -- Take a lock on the aggregates row to serialize access to the aggregate (optimistic concurrency)
				SELECT version INTO saved_version FROM %1$s_aggregates WHERE aggregate_id = expect_aggregate_id FOR UPDATE;

                event_len := cardinality(events);
                IF event_len = 0 OR event_len != cardinality(event_ids) THEN
                    RAISE EXCEPTION ''event length or event_id length invalid'';
                END IF;

                -- Use insert instead of upsert to shortcircuit a race condition when there are two 
                -- aggregate creations happening in parallel
				IF saved_version IS NULL THEN
					INSERT INTO %1$s_aggregates (aggregate_id, version, current_state) 
						VALUES (expect_aggregate_id, updated_version, state);
				ELSIF saved_version = expect_version THEN
                    UPDATE %1$s_aggregates SET version = updated_version, current_state = state WHERE aggregate_id = expect_aggregate_id;
                ELSE
                    -- check if we already commit the events in the past by checking the event_ids and the content hash
                    RAISE LOG E''optimistic concurrency and failure check for aggregate: %%'', expect_aggregate_id;
                    FOR i IN 1..event_len LOOP
                        new_event := events[i];
                        new_event_id := event_ids[i];
                        IF NOT EXISTS(SELECT FROM %1$s_events WHERE id = new_event_id AND md5short = md5short(new_event::text)) THEN
					        RAISE EXCEPTION ''optimistic concurrency exception. saved_version: %%'', saved_version;
                        END IF;
                    END LOOP;

                    RAISE LOG E''previous commit was successful, returning success'';
                    RETURN;
                END IF;
					
                FOR i IN 1..event_len LOOP
                    -- RAISE LOG E''\nraw_event --> %% \naggregate_id --> %% \nevent_version--> %% \nevent_type --> %%'', new_event, new_event->''id'', new_event->''version'', new_event->''kind'';
                    new_event := events[i];
                    new_event_id := event_ids[i];
                    INSERT INTO %1$s_events (aggregate_id, id, version, md5short, data) 
                        VALUES (expect_aggregate_id, new_event_id, expect_version+i, md5short(new_event::text), new_event);
                END LOOP;
			END;
			$inner$ LANGUAGE plpgsql;', _domainName);
END;
$outer$ LANGUAGE plpgsql;

-- Creates an sequence table;
-- param `_table` is the created table's name
--
-- # Example
--
-- ```sql
-- SELECT ls_create_sequence_table('table_name');
-- ```
CREATE OR REPLACE FUNCTION ls_create_sequence_table
(_table text) RETURNS VOID AS $$
BEGIN
	EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %s 
		(
			domain VARCHAR(32) UNIQUE PRIMARY KEY NOT NULL,
			sequence BIGINT NOT NULL
		)', _table);
	EXECUTE format('CREATE INDEX %1$s_domain_idx ON %1$s (domain)', _table);
END;
$$ LANGUAGE plpgsql;

-- Sets up a trigger for the given table when send create, update, delete occurs, 
-- a JSON payload with full row information is sent to TG_TABLE_NAME channel using postgres Listen/Notify
--
-- # Example
--
-- ```sql
-- CREATE TABLE users (id SERIAL PRIMARY KEY, updated_at TIMESTAMP NOT NULL DEFAULT NOW());
--
-- SELECT ls_notify('users');
--
-- LISTEN 'users';
-- ```
CREATE OR REPLACE FUNCTION ls_notify
(_tbl regclass) RETURNS VOID AS $$
BEGIN
	EXECUTE format
	('CREATE TRIGGER ls_notify AFTER INSERT OR UPDATE OR DELETE ON %s
                    FOR EACH ROW EXECUTE PROCEDURE ls_notify_trigger()', _tbl);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ls_notify_trigger
() RETURNS TRIGGER AS $$
DECLARE
  record RECORD;
  payload JSON;
BEGIN
  IF (TG_OP = 'DELETE') THEN
    record = OLD;
  ELSE
    record = NEW;
  END IF;

  payload = json_build_object('origin', TG_TABLE_NAME,
                              'op', TG_OP,
                              'record', row_to_json(record));
  PERFORM pg_notify(TG_TABLE_NAME, payload::text);

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Sets up a trigger for the given table when send create, update, delete occurs, 
-- a sparse JSON payload with only the table name and operation is sent to TG_TABLE_NAME channel using postgres Listen/Notify
--
-- # Example
--
-- ```sql
-- CREATE TABLE users (id SERIAL PRIMARY KEY, updated_at TIMESTAMP NOT NULL DEFAULT NOW());
--
-- SELECT ls_notify_sparse('users');
--
-- LISTEN 'users';
-- ```
CREATE OR REPLACE FUNCTION ls_notify_sparse
(_tbl regclass) RETURNS VOID AS $$
BEGIN
	EXECUTE format
	('CREATE TRIGGER ls_notify_sparse AFTER INSERT OR UPDATE OR DELETE ON %s
                    FOR EACH ROW EXECUTE PROCEDURE ls_notify_sparse_trigger()', _tbl);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ls_notify_sparse_trigger
() RETURNS TRIGGER AS $$
DECLARE
  payload JSON;
BEGIN
  payload = json_build_object('origin', TG_TABLE_NAME,
                              'op', TG_OP);
  PERFORM pg_notify(TG_TABLE_NAME, payload::text);

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
