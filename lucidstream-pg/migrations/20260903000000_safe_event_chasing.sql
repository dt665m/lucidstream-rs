-- BIGSERIAL gaps are valid. A chaser must never repair or rewind the sequence.
-- Safe-head capture instead fences writers and proves that no lower event can
-- become visible after a projection advances its checkpoint.
CREATE OR REPLACE FUNCTION ls_check_sequence_integrity(_domain text) RETURNS VOID
AS $$
DECLARE
    events_table TEXT;
    sequence_table REGCLASS;
    cache_size BIGINT;
BEGIN
    events_table := format('%I', _domain || '_events');
    sequence_table := pg_get_serial_sequence(events_table, 'sequence')::regclass;

    IF sequence_table IS NULL THEN
        RAISE EXCEPTION 'event sequence does not exist for domain %', _domain;
    END IF;

    SELECT seqcache INTO cache_size
    FROM pg_sequence
    WHERE seqrelid = sequence_table;

    IF cache_size != 1 THEN
        RAISE EXCEPTION 'event sequence % must use CACHE 1, found CACHE %', sequence_table, cache_size;
    END IF;
END;
$$ LANGUAGE plpgsql;
