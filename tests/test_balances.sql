SET search_path TO btracker_app, public;
SET ROLE btracker_owner;

CREATE OR REPLACE FUNCTION btracker_app.find_accounts_with_balances() 
RETURNS TABLE (
    name TEXT,
    has_orders INTEGER,
    has_escrow INTEGER,
    has_conversions INTEGER
) LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    RETURN QUERY 
    SELECT 
        av.name::TEXT,
        CASE WHEN EXISTS (SELECT 1 FROM btracker_app.account_open_orders WHERE account = av.id) THEN 1 ELSE 0 END,
        CASE WHEN EXISTS (SELECT 1 FROM btracker_app.account_escrow_transfers WHERE account = av.id) THEN 1 ELSE 0 END,
        CASE WHEN EXISTS (SELECT 1 FROM btracker_app.account_pending_conversions WHERE account = av.id) THEN 1 ELSE 0 END
    FROM hive.accounts_view av
    WHERE EXISTS (SELECT 1 FROM btracker_app.account_open_orders WHERE account = av.id)
       OR EXISTS (SELECT 1 FROM btracker_app.account_escrow_transfers WHERE account = av.id)
       OR EXISTS (SELECT 1 FROM btracker_app.account_pending_conversions WHERE account = av.id);
END;
$$;

CREATE OR REPLACE FUNCTION btracker_app.get_special_balances(account_name TEXT)
RETURNS TABLE (
    balance_type TEXT,
    amount NUMERIC,
    currency TEXT
) LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    RETURN QUERY
    WITH account_id AS (
        SELECT id FROM hive.accounts_view WHERE name = account_name
    )
    SELECT 
        'Open Orders'::TEXT,
        SUM(o.amount)::NUMERIC,
        CASE WHEN o.nai = 13 THEN 'HIVE' ELSE 'HBD' END::TEXT
    FROM btracker_app.account_open_orders o, account_id
    WHERE o.account = account_id.id
    GROUP BY o.nai
    UNION ALL
    SELECT 
        'Escrow'::TEXT,
        CASE 
            WHEN t.type = 'HIVE' THEN t.hive_amount
            ELSE t.hbd_amount
        END::NUMERIC,
        t.type::TEXT
    FROM (
        SELECT 
            e.hive_amount,
            e.hbd_amount,
            unnest(ARRAY['HIVE', 'HBD']) as type
        FROM btracker_app.account_escrow_transfers e, account_id
        WHERE e.account = account_id.id
    ) t
    UNION ALL
    -- Get pending conversions
    SELECT 
        'Pending Conversion'::TEXT,
        c.amount::NUMERIC,
        'HBD'::TEXT
    FROM btracker_app.account_pending_conversions c, account_id
    WHERE c.account = account_id.id;
END;
$$;

CREATE OR REPLACE FUNCTION btracker_app.check_special_balances_data()
RETURNS TABLE (
    table_name TEXT,
    row_count BIGINT
) LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    RETURN QUERY
    SELECT 'account_open_orders'::TEXT, COUNT(*)::BIGINT 
    FROM btracker_app.account_open_orders
    UNION ALL
    SELECT 'account_escrow_transfers'::TEXT, COUNT(*)::BIGINT 
    FROM btracker_app.account_escrow_transfers
    UNION ALL
    SELECT 'account_pending_conversions'::TEXT, COUNT(*)::BIGINT 
    FROM btracker_app.account_pending_conversions;
END;
$$;

RESET ROLE;