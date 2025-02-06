from datetime import datetime


class BalanceTracker:
    def __init__(self, Db):
        self.Db = Db

    @staticmethod
    def insert_space(param):
        return param.replace("%20", " ")

    def find_matching_accounts(self, _partial_account_name):
        psql_cmd = """
            SELECT json_agg(account_query.accounts
            ORDER BY
            account_query.name_lengths,
            account_query.accounts)

            FROM (
                SELECT
                ha.name AS accounts,
                LENGTH(ha.name) AS name_lengths
                FROM
                hive.accounts_view ha
                WHERE
                ha.name LIKE '{_partial_account_name}%'
                ORDER BY
                accounts,
                name_lengths
                LIMIT 50
            ) account_query
            """.format(_partial_account_name=_partial_account_name)

        return self.Db.query(psql_cmd)

    def get_balance_for_coin_by_block(self, _account_name, _coin_type, _start_block, _end_block):
        __block_increment = int((_end_block - _start_block) / 1000)

        psql_cmd = """
        SELECT to_jsonb(result) FROM (
            SELECT
            json_agg(block_step) AS block,
            json_agg(CASE WHEN balance IS NULL THEN 0 ELSE balance END) AS balance
            FROM (
            SELECT
                block_step,
                first_value(balance) OVER (PARTITION BY value_partition_reverse) AS balance
            FROM (
                SELECT
                block_step,
                balance,
                SUM(CASE WHEN balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY block_step DESC) AS value_partition_reverse
                FROM (
                SELECT
                    block_step,
                    first_value(balance) OVER (PARTITION BY value_partition) AS balance
                FROM (
                    SELECT
                    steps.block_step AS block_step,
                    distinct_values.balance AS balance,
                    SUM(CASE WHEN distinct_values.balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY steps.block_step) AS value_partition
                    FROM (
                    SELECT DISTINCT ON (block_step)
                        block_step,
                        balance
                    FROM (
                        SELECT 
                        block_step,
                        max(balance) OVER (ORDER BY block_step) AS balance
                        FROM (
                        SELECT
                            row_number() OVER (ORDER BY abh.source_op_block) AS id,
                            ((((abh.source_op_block - 1 - {_start_block}) / {__block_increment})::INT + 1) * {__block_increment} + {_start_block})::BIGINT AS block_step,
                            abh.balance::BIGINT AS balance
                        FROM
                            btracker_app.account_balance_history abh
                        WHERE 
                            abh.account = '{_account_name}' AND
                            abh.nai = '{_coin_type}' AND
                            abh.source_op_block >= {_start_block} AND
                            abh.source_op_block <= {_end_block}
                        ORDER BY abh.source_op_block ASC
                        ) hive_query
                    ORDER BY block_step, id DESC
                    ) last_block_values 
                    ) distinct_values
                    RIGHT JOIN (
                    SELECT
                        generate_series({_start_block}, {_end_block} + {__block_increment}, {__block_increment}) AS block_step,
                        null AS balance
                    ) steps
                    ON distinct_values.block_step = steps.block_step
                ) join_tables
                ) fill_balance_bottom
            ) invert_value_partition
            ORDER BY block_step ASC
            ) fill_balance_top
        ) result;
        """.format(_account_name=_account_name, _coin_type=_coin_type, _start_block=_start_block, _end_block=_end_block, __block_increment=__block_increment)

        return self.Db.query(psql_cmd)

    def get_balance_for_coin_by_time(self, _account_name, _coin_type, _start_time, _end_time):
        _start_time = self.insert_space(_start_time)
        _end_time = self.insert_space(_end_time)

        __time_increment = str((datetime.strptime(
            _end_time, "%Y-%m-%d %H:%M:%S") - datetime.strptime(_start_time, "%Y-%m-%d %H:%M:%S")) / 1000)

        psql_cmd = """
        SELECT to_jsonb(result) FROM (
            SELECT
            json_agg(time_step) AS time,
            json_agg(CASE WHEN balance IS NULL THEN 0 ELSE balance END) AS balance
            FROM (
            SELECT
                time_step,
                first_value(balance) OVER (PARTITION BY value_partition_reverse) AS balance
            FROM (
                SELECT
                time_step,
                balance,
                SUM(CASE WHEN balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY time_step DESC) AS value_partition_reverse
                FROM (
                SELECT
                    time_step,
                    first_value(balance) OVER (PARTITION BY value_partition) AS balance
                FROM (
                    SELECT
                    steps.time_step AS time_step,
                    distinct_values.balance AS balance,
                    SUM(CASE WHEN distinct_values.balance IS NULL THEN 0 ELSE 1 END) OVER (ORDER BY steps.time_step) AS value_partition
                    FROM (
                    SELECT DISTINCT ON (time_step)
                        time_step,
                        balance
                    FROM (
                        SELECT
                        time_step,
                        max(balance) OVER (ORDER BY time_step) AS balance
                        FROM (
                        SELECT
                            row_number() OVER (ORDER BY time_query.created_at) AS id,
                            (( (SELECT extract( EPOCH FROM ((time_query.created_at - '00:00:01'::TIME - '{_start_time}'::TIMESTAMP)::TIME / (SELECT extract(EPOCH FROM ('{__time_increment}'::INTERVAL)::TIME))) ))::INT + 1 ) * ('{__time_increment}'::INTERVAL)::TIME + '{_start_time}'::TIMESTAMP) AS time_step,
                            hive_query.balance AS balance
                        FROM (
                            SELECT
                            abh.source_op_block::BIGINT AS block,
                            abh.balance::BIGINT AS balance
                            FROM
                            btracker_app.account_balance_history abh
                            WHERE
                            abh.account = '{_account_name}' AND
                            abh.nai = '{_coin_type}'
                            ORDER BY abh.source_op_block ASC
                        ) hive_query
                        LEFT JOIN (
                            SELECT
                            num AS block,
                            created_at::TIMESTAMP
                            FROM
                            hive.blocks_view
                        ) time_query
                        ON hive_query.block = time_query.block
                        ) add_timestamps
                    ORDER BY time_step, id DESC
                    ) last_time_values
                    ) distinct_values
                    RIGHT JOIN (
                    SELECT
                        generate_series('{_start_time}'::TIMESTAMP, '{_end_time}'::TIMESTAMP + ('{__time_increment}'::INTERVAL)::TIME, ('{__time_increment}'::INTERVAL)::TIME)::TIMESTAMP AS time_step,
                        null AS balance
                    ) steps
                    ON distinct_values.time_step = steps.time_step
                ) join_tables
                ) fill_balance_bottom
            ) invert_value_partition
            ORDER BY time_step ASC
            ) fill_balance_top
        ) result;
        """.format(_account_name=_account_name, _coin_type=_coin_type, _start_time=_start_time, _end_time=_end_time, __time_increment=__time_increment)

        return self.Db.query(psql_cmd)

    def get_account_special_balances(self, _account_name):
        psql_cmd = """
        WITH account_id AS (
            SELECT id FROM hive.accounts_view WHERE name = '{_account_name}'
        ),
        open_orders AS (
            SELECT 
                SUM(CASE WHEN nai = 13 THEN amount END) as hive_in_orders,
                SUM(CASE WHEN nai = 37 THEN amount END) as hbd_in_orders
            FROM btracker_app.account_open_orders
            WHERE account = (SELECT id FROM account_id)
        ),
        escrow_transfers AS (
            SELECT 
                SUM(hive_amount) as hive_in_escrow,
                SUM(hbd_amount) as hbd_in_escrow
            FROM btracker_app.account_escrow_transfers
            WHERE account = (SELECT id FROM account_id)
        ),
        pending_conversions AS (
            SELECT 
                SUM(amount) as pending_conversion_amount
            FROM btracker_app.account_pending_conversions
            WHERE account = (SELECT id FROM account_id)
        ),
        pending_savings AS (
            SELECT 
                SUM(CASE WHEN nai = 13 THEN balance END) as hive_pending_savings,
                SUM(CASE WHEN nai = 37 THEN balance END) as hbd_pending_savings
            FROM btracker_app.transfer_saving_id
            WHERE account = (SELECT id FROM account_id)
            AND complete_date IS NULL
        )
        SELECT json_build_object(
            'open_orders', json_build_object(
                'hive', COALESCE(o.hive_in_orders, 0),
                'hbd', COALESCE(o.hbd_in_orders, 0)
            ),
            'escrow_transfers', json_build_object(
                'hive', COALESCE(e.hive_in_escrow, 0),
                'hbd', COALESCE(e.hbd_in_escrow, 0)
            ),
            'pending_conversions', COALESCE(p.pending_conversion_amount, 0),
            'pending_savings_withdrawals', json_build_object(
                'hive', COALESCE(s.hive_pending_savings, 0),
                'hbd', COALESCE(s.hbd_pending_savings, 0)
            )
        )
        FROM open_orders o
        CROSS JOIN escrow_transfers e
        CROSS JOIN pending_conversions p
        CROSS JOIN pending_savings s;
        """.format(_account_name=_account_name)

        return self.Db.query(psql_cmd)
