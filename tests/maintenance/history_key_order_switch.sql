/*
 * history_key_order_switch.sql
 * ============================
 * history_key_order_enabled(), the kill switch of finalize's key-order rewrite
 * of the balance and savings history (custom.btracker_order_history_by_key):
 *
 *   - unset or empty: on;
 *   - on/off and the other boolean spellings are honoured;
 *   - a value that is not a boolean raises, naming the setting.
 *
 * Expects psql variable `schema` (the application schema). The setting is
 * changed transaction-locally in a transaction that is rolled back. Run under
 * `psql -v ON_ERROR_STOP=on`.
 */

\set ON_ERROR_STOP on
SET search_path TO :"schema", public;

BEGIN;
DO
$$
DECLARE
  __case RECORD;
  __raised BOOLEAN := FALSE;
BEGIN
  FOR __case IN
    SELECT * FROM (VALUES ('', TRUE), ('  ', TRUE), ('on', TRUE), ('true', TRUE), ('off', FALSE), ('OFF', FALSE), ('false', FALSE), ('0', FALSE)) v(value, expected)
  LOOP
    PERFORM set_config('custom.btracker_order_history_by_key', __case.value, true);
    IF history_key_order_enabled() IS DISTINCT FROM __case.expected THEN
      RAISE EXCEPTION 'custom.btracker_order_history_by_key = %: expected %, got %',
        quote_literal(__case.value), __case.expected, history_key_order_enabled();
    END IF;
  END LOOP;

  PERFORM set_config('custom.btracker_order_history_by_key', 'sometimes', true);
  BEGIN
    PERFORM history_key_order_enabled();
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM NOT LIKE '%custom.btracker_order_history_by_key%' THEN
      RAISE EXCEPTION 'unexpected error for a non-boolean value: %', SQLERRM;
    END IF;
    __raised := TRUE;
  END;
  IF NOT __raised THEN
    RAISE EXCEPTION 'custom.btracker_order_history_by_key = ''sometimes'' did not raise';
  END IF;

  RAISE NOTICE 'PASS: custom.btracker_order_history_by_key defaults on, honours on/off and rejects other values';
END
$$;
ROLLBACK;
