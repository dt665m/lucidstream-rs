-- Commits compare-and-swap the stored aggregate version instead of locking the
-- aggregate row first. A writer that loses a race matches no row, so it can
-- never overwrite the winner's state, and it receives the same optimistic
-- concurrency result (or idempotent success for an exact retry) as before.
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
                written BIGINT;
				new_event JSONB;
                new_event_id UUID;
			BEGIN
                event_len := cardinality(events);
                IF event_len = 0 OR event_len != cardinality(event_ids) THEN
                    RAISE EXCEPTION ''event length or event_id length invalid'';
                END IF;

                UPDATE %1$s_aggregates SET version = updated_version, current_state = state
                    WHERE aggregate_id = expect_aggregate_id AND version = expect_version;
                GET DIAGNOSTICS written = ROW_COUNT;
                IF written = 0 THEN
                    -- A concurrent creator makes this wait for its outcome, then do nothing.
                    INSERT INTO %1$s_aggregates (aggregate_id, version, current_state)
                        VALUES (expect_aggregate_id, updated_version, state)
                        ON CONFLICT (aggregate_id) DO NOTHING;
                    GET DIAGNOSTICS written = ROW_COUNT;
                END IF;

                IF written = 0 THEN
                    -- Another commit won. Only an exact retry of an earlier successful
                    -- commit finds all of its events stored with matching contents.
                    SELECT version INTO saved_version FROM %1$s_aggregates WHERE aggregate_id = expect_aggregate_id;
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
                    new_event_id := event_ids[i];
                    IF EXISTS(SELECT FROM %1$s_events WHERE id = new_event_id) THEN
                        RAISE EXCEPTION ''event id reused: %%'', new_event_id;
                    END IF;
                END LOOP;

                FOR i IN 1..event_len LOOP
                    new_event := events[i];
                    new_event_id := event_ids[i];
                    INSERT INTO %1$s_events (aggregate_id, id, version, md5short, data)
                        VALUES (expect_aggregate_id, new_event_id, expect_version+i, md5short(new_event::text), new_event);
                END LOOP;
			END;
			$inner$ LANGUAGE plpgsql;', _domainName);
END;
$outer$ LANGUAGE plpgsql;

-- Upgrade every existing domain's commit procedure in place.
DO $$
DECLARE
    domain_name TEXT;
BEGIN
    FOR domain_name IN
        SELECT left(p.proname, -length('_commit'))
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.prokind = 'p'
          AND n.nspname = current_schema()
          AND p.proname LIKE '%\_commit'
          AND to_regclass(format('%I', left(p.proname, -length('_commit')) || '_aggregates')) IS NOT NULL
          AND to_regclass(format('%I', left(p.proname, -length('_commit')) || '_events')) IS NOT NULL
    LOOP
        PERFORM ls_new_commit_proc(domain_name);
    END LOOP;
END;
$$;
