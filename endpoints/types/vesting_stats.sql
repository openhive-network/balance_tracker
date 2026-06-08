SET ROLE btracker_owner;

/** openapi:components:schemas
btracker_backend.vesting_filter:
  type: string
  enum:
    - all
    - power_up
    - power_down_init
    - power_down_fill
    - power_down_route_received
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.vesting_filter CASCADE;
CREATE TYPE btracker_backend.vesting_filter AS ENUM (
    'all',
    'power_up',
    'power_down_init',
    'power_down_fill',
    'power_down_route_received'
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.vesting_stats:
  type: object
  properties:
    date:
      type: string
      format: date-time
      description: end of the time bucket (day, month or year)
    power_up_count:
      type: integer
      description: number of transfer_to_vesting operations in the period
    power_up_hive:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: total HIVE moved into VESTS via transfer_to_vesting (HIVE NAI/precision)
    power_down_init_count:
      type: integer
      description: number of withdraw_vesting operations (cancellations excluded)
    power_down_init_vests:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: total VESTS scheduled for withdrawal via withdraw_vesting (VESTS NAI/precision)
    power_down_fill_count:
      type: integer
      description: number of fill_vesting_withdraw operations (weekly tranches)
    power_down_fill_vests:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: total VESTS withdrawn (includes routed-to-VESTS) (VESTS NAI/precision)
    power_down_fill_hive:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: realised HIVE delivered by fill_vesting_withdraw (excludes routed-to-VESTS) (HIVE NAI/precision)
    power_down_route_received_count:
      type: integer
      description: number of fill_vesting_withdraw tranches received via a withdraw route from another account (to<>from). Per-account only; 0 in the global stats.
    power_down_route_received_hive:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: HIVE received from another account's routed power-down (auto_vest=false) (HIVE NAI/precision). Per-account only.
    power_down_route_received_vests:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: VESTS received from another account's routed power-down (auto_vest=true) (VESTS NAI/precision). Per-account only.
    last_block_num:
      type: integer
      description: last block number contributing to the period
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.vesting_stats CASCADE;
CREATE TYPE btracker_backend.vesting_stats AS (
    "date" TIMESTAMP,
    "power_up_count" INT,
    "power_up_hive" btracker_backend.amount,
    "power_down_init_count" INT,
    "power_down_init_vests" btracker_backend.amount,
    "power_down_fill_count" INT,
    "power_down_fill_vests" btracker_backend.amount,
    "power_down_fill_hive" btracker_backend.amount,
    "power_down_route_received_count" INT,
    "power_down_route_received_hive" btracker_backend.amount,
    "power_down_route_received_vests" btracker_backend.amount,
    "last_block_num" INT
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.array_of_vesting_stats:
  type: array
  items:
    $ref: '#/components/schemas/btracker_backend.vesting_stats'
*/

/** openapi:components:schemas
btracker_backend.vesting_history_event:
  type: object
  properties:
    block_num:
      type: integer
      description: block number where the operation was included
    operation_id:
      type: string
      description: unique operation identifier with encoded block number and position
    op_type_id:
      type: integer
      x-sql-datatype: SMALLINT
      description: operation type identifier
    direction:
      $ref: '#/components/schemas/btracker_backend.vesting_filter'
      description: |
        which kind of vesting flow this row represents: power_up, power_down_init,
        power_down_fill (the account's own power-down), or power_down_route_received
        (a fill routed to this account from another account's power-down, to<>from).
        Note: power_down_fill and power_down_route_received share op_type_id 56 — branch on this field.
    amount_hive:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: HIVE component (power_up amount; power_down_fill/route_received deposited if HIVE); zero-amount HIVE object when not applicable
    amount_vests:
      $ref: '#/components/schemas/btracker_backend.amount'
      description: VESTS component (power_down_init total; power_down_fill withdrawn; route_received deposited if VESTS); zero-amount VESTS object when not applicable
    timestamp:
      type: string
      format: date-time
      description: block timestamp
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.vesting_history_event CASCADE;
CREATE TYPE btracker_backend.vesting_history_event AS (
    "block_num" INT,
    "operation_id" TEXT,
    "op_type_id" SMALLINT,
    "direction" btracker_backend.vesting_filter,
    "amount_hive" btracker_backend.amount,
    "amount_vests" btracker_backend.amount,
    "timestamp" TIMESTAMP
);
-- openapi-generated-code-end

/** openapi:components:schemas
btracker_backend.vesting_history:
  type: object
  properties:
    total_operations:
      type: integer
      description: Total number of vesting operations matching the filter
    total_pages:
      type: integer
      description: Total number of pages
    operations_result:
      type: array
      items:
        $ref: '#/components/schemas/btracker_backend.vesting_history_event'
      description: List of vesting events for the requested page
 */
-- openapi-generated-code-begin
DROP TYPE IF EXISTS btracker_backend.vesting_history CASCADE;
CREATE TYPE btracker_backend.vesting_history AS (
    "total_operations" INT,
    "total_pages" INT,
    "operations_result" btracker_backend.vesting_history_event[]
);
-- openapi-generated-code-end

RESET ROLE;
