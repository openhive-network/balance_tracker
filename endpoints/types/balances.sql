SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.balance:
  type: object
  properties:
    hbd_balance:
      type: integer
      x-sql-datatype: BIGINT
      description: number of HIVE backed dollars the account has
    hive_balance:
      type: integer
      x-sql-datatype: BIGINT
      description: account''s HIVE balance
    vesting_shares:
      type: string
      description: account''s VEST balance
    vesting_balance_hive:
      type: integer
      x-sql-datatype: BIGINT
      description: >- 
        the VEST balance, presented in HIVE, 
        is calculated based on the current HIVE price
    post_voting_power_vests:
      type: string
      description: >-
        account''s VEST balance - delegated VESTs + reveived VESTs,
        presented in HIVE,calculated based on the current HIVE price
    delegated_vests:
      type: string
      description: >-
        VESTS delegated to another user, 
        account''s power is lowered by delegated VESTS
    received_vests:
      type: string
      description: >-
        VESTS received from another user, 
        account''s power is increased by received VESTS
    curation_rewards:
      type: string
      description: rewards obtained by posting and commenting expressed in VEST
    posting_rewards:
      type: string
      description: curator''s reward expressed in VEST
    hbd_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        not yet claimed HIVE backed dollars 
        stored in hbd reward balance
    hive_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        not yet claimed HIVE 
        stored in hive reward balance
    vests_rewards:
      type: string
      description: >-
        not yet claimed VESTS 
        stored in vest reward balance
    hive_vesting_rewards:
      type: integer
      x-sql-datatype: BIGINT
      description: >-
        the reward vesting balance, denominated in HIVE, 
        is determined by the prevailing HIVE price at the time of reward reception
    hbd_savings:
      type: integer
      x-sql-datatype: BIGINT
      description: saving balance of HIVE backed dollars
    hive_savings:
      type: integer
      x-sql-datatype: BIGINT
      description: HIVE saving balance
    savings_withdraw_requests:
      type: integer
      description: >-
        number representing how many payouts are pending 
        from user''s saving balance
    vesting_withdraw_rate:
      type: string
      description: >-
        received until the withdrawal is complete, 
        with each installment amounting to 1/13 of the withdrawn total
    to_withdraw:
      type: string
      description: the remaining total VESTS needed to complete withdrawals
    withdrawn:
      type: string
      description: the total VESTS already withdrawn from active withdrawals
    withdraw_routes:
      type: integer
      description: list of account receiving the part of a withdrawal
    delayed_vests:
      type: string
      description: blocked VESTS by a withdrawal
    conversion_pending_amount_hbd:
      type: integer
      x-sql-datatype: BIGINT
      description: "total HBD currently pending conversion"
    conversion_pending_count_hbd:
      type: integer
      description: "number of HBD conversion requests"
    conversion_pending_amount_hive:
      type: integer
      x-sql-datatype: BIGINT
      description: "total HIVE currently pending conversion"
    conversion_pending_count_hive:
      type: integer
      description: "number of HIVE conversion requests"
    open_orders_hbd_count:
      type: integer
      description: "count of open HBD orders"
    open_orders_hive_count:
      type: integer
      description: "count of open HIVE orders"
    open_orders_hive_amount:
      type: integer
      x-sql-datatype: BIGINT
      description: "total amount of HIVE in open orders"
    open_orders_hbd_amount:
      type: integer
      x-sql-datatype: BIGINT
      description: "total amount of HBD in open orders"


 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.balance CASCADE;
CREATE TYPE btracker_backend.balance AS (
    "hbd_balance" BIGINT,
    "hive_balance" BIGINT,
    "vesting_shares" TEXT,
    "vesting_balance_hive" BIGINT,
    "post_voting_power_vests" TEXT,
    "delegated_vests" TEXT,
    "received_vests" TEXT,
    "curation_rewards" TEXT,
    "posting_rewards" TEXT,
    "hbd_rewards" BIGINT,
    "hive_rewards" BIGINT,
    "vests_rewards" TEXT,
    "hive_vesting_rewards" BIGINT,
    "hbd_savings" BIGINT,
    "hive_savings" BIGINT,
    "savings_withdraw_requests" INT,
    "vesting_withdraw_rate" TEXT,
    "to_withdraw" TEXT,
    "withdrawn" TEXT,
    "withdraw_routes" INT,
    "delayed_vests" TEXT,
    "conversion_pending_amount_hbd" BIGINT,
    "conversion_pending_count_hbd" INT,
    "conversion_pending_amount_hive" BIGINT,
    "conversion_pending_count_hive" INT,
    "open_orders_hbd_count" INT,
    "open_orders_hive_count" INT,
    "open_orders_hive_amount" BIGINT,
    "open_orders_hbd_amount" BIGINT
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.balances:
  type: object
  properties:
    balance:
      type: string
      description: aggregated account''s balance
    savings_balance:
      type: string
      description: aggregated account''s savings balance
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.balances CASCADE;
CREATE TYPE btracker_backend.balances AS (
    "balance" TEXT,
    "savings_balance" TEXT
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.aggregated_history:
  type: object
  properties:
    date:
      type: string
      format: date-time
      description: date of the balance aggregation
    balance:
      $ref: '#/components/schemas/btracker_backend.balances'
      description: aggregated account''s balance
    prev_balance:
      $ref: '#/components/schemas/btracker_backend.balances'
      description: aggregated account''s balance from the previous day/month/year
    min_balance:
      $ref: '#/components/schemas/btracker_backend.balances'
      description: minimum account''s balance in the aggregation period
    max_balance:
      $ref: '#/components/schemas/btracker_backend.balances'
      description: maximum account''s balance in the aggregation period
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.aggregated_history CASCADE;
CREATE TYPE btracker_backend.aggregated_history AS (
    "date" TIMESTAMP,
    "balance" btracker_backend.balances,
    "prev_balance" btracker_backend.balances,
    "min_balance" btracker_backend.balances,
    "max_balance" btracker_backend.balances
);
-- openapi-generated-code-end


/** openapi:components:schemas
btracker_backend.balance_history:
  type: object
  properties:
    block_num:
      type: integer
      description: block number
    operation_id:
      type: string
      description: >-
        unique operation identifier with
        an encoded block number and operation type id
    op_type_id:
      type: integer
      description: operation type identifier
    balance:
      type: string
      description: The closing balance
    prev_balance:
      type: string
      description: Balance in previous day/month/year
    balance_change:
      type: string
      description: Diffrence between balance and prev_balance
    timestamp:
      type: string
      format: date-time
      description: Creation date
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.balance_history CASCADE;
CREATE TYPE btracker_backend.balance_history AS (
    "block_num" INT,
    "operation_id" TEXT,
    "op_type_id" INT,
    "balance" TEXT,
    "prev_balance" TEXT,
    "balance_change" TEXT,
    "timestamp" TIMESTAMP
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.operation_history:
  type: object
  properties:
    total_operations:
      type: integer
      description: Total number of operations
    total_pages:
      type: integer
      description: Total number of pages
    operations_result:
      type: array
      items:
        $ref: '#/components/schemas/btracker_backend.balance_history'
      description: List of operation results
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.operation_history CASCADE;
CREATE TYPE btracker_backend.operation_history AS (
    "total_operations" INT,
    "total_pages" INT,
    "operations_result" btracker_backend.balance_history[]
);
-- openapi-generated-code-end


RESET ROLE;
