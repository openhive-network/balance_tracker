/**
 * Pagination Utilities for Balance Tracker
 * =========================================
 *
 * This file contains all pagination-related functions used by the Balance Tracker API.
 * It implements direction-aware pagination that correctly handles both ascending (ASC)
 * and descending (DESC) sort orders.
 *
 * Overview:
 * ---------
 * The pagination system works with balance history data that is stored sequentially
 * (by balance_seq_no). When users request data in DESC order, they expect the most
 * recent entries first - but the underlying data is stored in ASC order. This file
 * handles the translation between user-facing page numbers and internal offsets.
 *
 * Key Concepts:
 * -------------
 * 1. Page Reversal: For DESC order, page 1 refers to the LAST page of data (highest values)
 * 2. Remainder Handling: When total_count % page_size != 0, one page will be partial
 * 3. Sequence Number Ranges: Functions find seq_no boundaries within block ranges
 *
 * Dependencies:
 * -------------
 * - btracker_backend.sort_direction: ENUM type ('asc', 'desc')
 * - btracker_backend.balance_type: ENUM type ('balance', 'savings_balance')
 * - btracker_backend.account_balance_history_view: View for liquid balance history
 * - btracker_backend.account_savings_history_view: View for savings balance history
 * - btracker_backend.validate_page(): Exception function for invalid page numbers
 * - hive.blocks_view: HAF view for block metadata (timestamps)
 */

SET ROLE btracker_owner;

/**
 * total_pages()
 * -------------
 * Calculate the total number of pages needed to display a given number of items.
 *
 * This is a simple ceiling division: ceil(count / page_size)
 *
 * @param _ops_count  Total number of items to paginate
 * @param _page_size  Number of items per page
 * @returns           Total number of pages (minimum 1 if count > 0)
 *
 * Example:
 *   total_pages(25, 10) = 3  -- Pages: [10, 10, 5]
 *   total_pages(20, 10) = 2  -- Pages: [10, 10]
 *   total_pages(0, 10)  = 0  -- No pages needed
 */
CREATE OR REPLACE FUNCTION btracker_backend.total_pages(
    _ops_count INT,
    _page_size INT
)
RETURNS INT -- noqa: LT01, CP05
LANGUAGE 'plpgsql' IMMUTABLE
AS
$$
BEGIN
  RETURN (
    CASE
      WHEN (_ops_count % _page_size) = 0 THEN
        _ops_count / _page_size
      ELSE
        (_ops_count / _page_size) + 1
    END
  );
END
$$;

/**
 * calculate_pages_return
 * ----------------------
 * Return type for calculate_pages() function.
 *
 * Fields:
 * - rest_of_division: Remainder when count % limit != 0 (size of partial page)
 * - total_pages:      Total number of pages available
 * - page_num:         Internal page number after direction adjustment
 * - offset_filter:    Number of rows to skip (for OFFSET clause)
 * - limit_filter:     Number of rows to return (for LIMIT clause)
 */
DROP TYPE IF EXISTS btracker_backend.calculate_pages_return CASCADE;
CREATE TYPE btracker_backend.calculate_pages_return AS
(
    rest_of_division INT,
    total_pages INT,
    page_num INT,
    offset_filter INT,
    limit_filter INT
);

/**
 * calculate_pages()
 * -----------------
 * Core pagination function that calculates offset and limit for SQL queries.
 * Handles both ASC and DESC sort orders with proper page reversal.
 *
 * CRITICAL: This function implements direction-aware pagination!
 * ---------------------------------------------------------------------------
 * When sorting in DESC order, users expect page 1 to show the most recent data.
 * But internally, data is ordered by ascending seq_no. This function translates
 * between user expectations and internal query parameters.
 *
 * Algorithm Overview:
 * -------------------
 * 1. Calculate remainder: count % limit (determines partial page size)
 * 2. Calculate total pages: ceil(count / limit)
 * 3. For DESC: Reverse page number (page 1 becomes last page internally)
 * 4. Calculate offset based on direction and page position
 * 5. Adjust limit for partial pages (first page in DESC, last page in ASC)
 *
 * Page Number Reversal (DESC order):
 * ----------------------------------
 * User Page  ->  Internal Page (for 3 total pages)
 *     1      ->      3         (last page = most recent data)
 *     2      ->      2         (middle stays middle)
 *     3      ->      1         (first page = oldest data)
 *
 * Formula: internal_page = total_pages - user_page + 1
 *
 * Remainder Handling:
 * -------------------
 * When count is not evenly divisible by limit, one page will be partial.
 * - For DESC: Page 1 (most recent) gets the partial page
 * - For ASC:  Last page (oldest) gets the partial page
 *
 * Offset Calculation:
 * -------------------
 * The offset formula depends on:
 * 1. Whether we're in DESC mode
 * 2. Whether we're on the first internal page
 * 3. Whether there's a remainder
 *
 * For DESC (not page 1, with remainder):
 *   offset = ((internal_page - 2) * limit) + remainder
 *   This accounts for the partial first page.
 *
 * For ASC or DESC page 1:
 *   offset = (internal_page - 1) * limit
 *   Standard pagination offset.
 *
 * @param _count     Total number of items to paginate
 * @param _page      User-requested page number (1-indexed)
 * @param _order_is  Sort direction ('asc' or 'desc')
 * @param _limit     Items per page (page size)
 * @returns          calculate_pages_return with all pagination parameters
 *
 * Example Walkthrough:
 * --------------------
 * Given: 25 records, page_size=10, requesting page=2 with DESC order
 *
 * Step 1: Calculate remainder
 *   __rest_of_division = 25 % 10 = 5
 *
 * Step 2: Calculate total pages
 *   __total_pages = 25 / 10 + 1 = 3
 *   Pages in ASC order: [1]=items 1-10, [2]=items 11-20, [3]=items 21-25
 *   Pages in DESC order: [1]=items 21-25, [2]=items 11-20, [3]=items 1-10
 *
 * Step 3: Reverse page number for DESC
 *   User page 2 -> Internal page = 3 - 2 + 1 = 2
 *
 * Step 4: Calculate offset
 *   Since DESC, internal_page=2 (not 1), and remainder=5:
 *   __offset = ((2-2) * 10) + 5 = 0 + 5 = 5
 *
 * Step 5: Calculate limit
 *   Internal page 2 is not page 1 or total_pages, so full limit:
 *   __limit = 10
 *
 * Result: OFFSET 5 LIMIT 10 -> Returns items 6-15
 *   In DESC display order, this is the middle page (items 11-20 of 25)
 *
 * Another Example (DESC page 1):
 * ------------------------------
 * Given: 25 records, page_size=10, requesting page=1 with DESC order
 *
 * __rest_of_division = 5
 * __total_pages = 3
 * Internal page = 3 - 1 + 1 = 3 (last internal page)
 *
 * But wait - the code checks if __page = 1 AFTER reversal, which happens
 * when user requests the LAST page in DESC. Let me re-analyze...
 *
 * Actually: For DESC page 1, internal __page = 3. The offset calculation:
 *   Since __page (3) != 1 and __rest_of_division != 0:
 *   __offset = ((3-2) * 10) + 5 = 10 + 5 = 15
 *   __limit = 10 (full page, since internal page 3 != 1)
 *
 * Wait, that would return items 16-25, which is correct for DESC page 1!
 *
 * The __page = 1 check in the code refers to when the INTERNAL page is 1,
 * which happens when the user requests the LAST page in DESC mode.
 */
CREATE OR REPLACE FUNCTION btracker_backend.calculate_pages(
    _count INT,
    _page INT,
    _order_is btracker_backend.sort_direction, -- noqa: LT01, CP05
    _limit INT
)
RETURNS btracker_backend.calculate_pages_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql' STABLE
SET JIT = OFF
AS
$$
DECLARE
  __rest_of_division INT;
  __total_pages INT;
  __page INT;
  __offset INT;
  __limit INT;
BEGIN
  __rest_of_division := (_count % _limit)::INT;

  __total_pages := (
    CASE
      WHEN (__rest_of_division = 0) THEN
        _count / _limit
      ELSE
        (_count / _limit) + 1
      END
  )::INT;

  __page := (
    CASE
      WHEN (_page IS NULL) THEN
        1
      WHEN (_page IS NOT NULL) AND _order_is = 'desc' THEN
        __total_pages - _page + 1
      ELSE
        _page
      END
  );

  __offset := (
    CASE
      WHEN _order_is = 'desc' AND __page != 1 AND __rest_of_division != 0 THEN
        ((__page - 2) * _limit) + __rest_of_division
      WHEN __page = 1 THEN
        0
      ELSE
        (__page - 1) * _limit
      END
    );

  __limit := (
      CASE
        WHEN _order_is = 'desc' AND __page = 1             AND __rest_of_division != 0 THEN
          __rest_of_division
        WHEN _order_is = 'asc'  AND __page = __total_pages AND __rest_of_division != 0 THEN
          __rest_of_division
        ELSE
          _limit
        END
    );

  PERFORM btracker_backend.validate_page(_page, __total_pages);

  RETURN (__rest_of_division, __total_pages, __page, __offset, __limit)::btracker_backend.calculate_pages_return;
END
$$;

/**
 * balance_history_range_return
 * ----------------------------
 * Return type for balance history range functions.
 *
 * Fields:
 * - count:    Total number of balance history entries in the range
 * - from_seq: Starting sequence number (inclusive)
 * - to_seq:   Ending sequence number (inclusive)
 *
 * The sequence numbers can be used directly in WHERE clauses:
 *   WHERE balance_seq_no BETWEEN from_seq AND to_seq
 */
DROP TYPE IF EXISTS btracker_backend.balance_history_range_return CASCADE;
CREATE TYPE btracker_backend.balance_history_range_return AS
(
    count INT,
    from_seq INT,
    to_seq INT
);

/**
 * balance_history_range()
 * -----------------------
 * Router function that dispatches to the appropriate range function
 * based on balance type (liquid vs savings).
 *
 * This function provides a unified interface for finding sequence number
 * boundaries, regardless of whether querying liquid balances or savings.
 *
 * @param _account_id   Account ID to query
 * @param _coin_type    NAI code (13=HBD, 21=HIVE, 37=VESTS)
 * @param _balance_type 'balance' for liquid, 'savings_balance' for savings
 * @param _from         Starting block number (NULL for earliest)
 * @param _to           Ending block number (NULL for latest)
 * @returns             balance_history_range_return with count and seq_no bounds
 */
CREATE OR REPLACE FUNCTION btracker_backend.balance_history_range(
    _account_id INT,
    _coin_type INT,
    _balance_type btracker_backend.balance_type,
    _from INT,
    _to INT
)
RETURNS btracker_backend.balance_history_range_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql' STABLE
SET JIT = OFF
AS
$$
BEGIN
  IF _balance_type = 'balance' THEN
    RETURN btracker_backend.bh_balance(_account_id, _coin_type, _from, _to);
  END IF;

  RETURN btracker_backend.bh_savings_balance(_account_id, _coin_type, _from, _to);
END
$$;

/**
 * bh_balance()
 * ------------
 * Find the sequence number range for liquid balance history within a block range.
 *
 * This function is crucial for pagination because it:
 * 1. Converts block number boundaries to sequence number boundaries
 * 2. Counts total entries in the range (for page calculations)
 * 3. Handles NULL boundaries (open-ended ranges)
 *
 * Algorithm:
 * ----------
 * For _to boundary:
 *   1. Find the last block <= _to that has history entries
 *   2. Within that block, find the MAX sequence number
 *
 * For _from boundary:
 *   1. Find the first block >= _from that has history entries
 *   2. Within that block, find the MIN sequence number
 *
 * Why two-step lookup?
 * --------------------
 * A single block may have multiple balance changes (multiple seq_no values).
 * We need to find the boundary BLOCK first, then find the appropriate seq_no
 * within that block (MIN for _from, MAX for _to).
 *
 * Example:
 * --------
 * Block 100: seq_no 1, 2, 3
 * Block 200: seq_no 4, 5
 * Block 300: seq_no 6, 7, 8
 *
 * Query: _from=150, _to=250
 *   - First block >= 150 with data: 200, min seq = 4
 *   - Last block <= 250 with data: 200, max seq = 5
 *   - Result: count=2, from_seq=4, to_seq=5
 *
 * @param _account_id  Account ID to query
 * @param _coin_type   NAI code (13=HBD, 21=HIVE, 37=VESTS)
 * @param _from        Starting block number (NULL for earliest)
 * @param _to          Ending block number (NULL for latest)
 * @returns            balance_history_range_return with count and seq_no bounds
 */
CREATE OR REPLACE FUNCTION btracker_backend.bh_balance(
    _account_id INT,
    _coin_type INT,
    _from INT,
    _to INT
)
RETURNS btracker_backend.balance_history_range_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql' STABLE
SET JIT = OFF
AS
$$
DECLARE
  __to_seq INT;
  __from_seq INT;
BEGIN
  IF _to IS NULL THEN
    __to_seq := (
      SELECT
        ab.balance_seq_no
      FROM btracker_backend.account_balance_history_view ab
      WHERE ab.account = _account_id
        AND ab.nai = _coin_type
      ORDER BY ab.balance_seq_no DESC LIMIT 1
    );
  ELSE
    __to_seq := (
      WITH last_block AS (
        SELECT
          ab.source_op_block
        FROM btracker_backend.account_balance_history_view ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block <= _to
        ORDER BY ab.source_op_block DESC LIMIT 1
      ),
      get_sequence AS (
        SELECT
          ab.balance_seq_no
        FROM btracker_backend.account_balance_history_view ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block = (SELECT source_op_block FROM last_block)
      )
      SELECT MAX(gs.balance_seq_no)
      FROM get_sequence gs
    );
  END IF;

  IF _from IS NULL THEN
    __from_seq := (
      SELECT
        ab.balance_seq_no
      FROM btracker_backend.account_balance_history_view ab
      WHERE ab.account = _account_id
        AND ab.nai = _coin_type
      ORDER BY ab.balance_seq_no ASC LIMIT 1
    );
  ELSE
    __from_seq := (
      WITH first_block AS (
        SELECT
          ab.source_op_block
        FROM btracker_backend.account_balance_history_view ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block >= _from
        ORDER BY ab.source_op_block ASC LIMIT 1
      ),
      get_sequence AS (
        SELECT
          ab.balance_seq_no
        FROM btracker_backend.account_balance_history_view ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block = (SELECT source_op_block FROM first_block)
      )
      SELECT MIN(gs.balance_seq_no)
      FROM get_sequence gs
    );
  END IF;

  RETURN (
    COALESCE((__to_seq - __from_seq + 1), 0),
    COALESCE(__from_seq, 0),
    COALESCE(__to_seq, 0)
  )::btracker_backend.balance_history_range_return;
END
$$;

/**
 * bh_savings_balance()
 * --------------------
 * Find the sequence number range for savings balance history within a block range.
 *
 * Identical logic to bh_balance(), but queries account_savings_history_view
 * instead of account_balance_history_view.
 *
 * Note: VESTS cannot have savings balances (validated by validate_balance_history()).
 * Only HBD (NAI 13) and HIVE (NAI 21) are valid coin types for savings queries.
 *
 * @param _account_id  Account ID to query
 * @param _coin_type   NAI code (13=HBD, 21=HIVE only - VESTS not valid)
 * @param _from        Starting block number (NULL for earliest)
 * @param _to          Ending block number (NULL for latest)
 * @returns            balance_history_range_return with count and seq_no bounds
 *
 * @see bh_balance() for detailed algorithm explanation
 */
CREATE OR REPLACE FUNCTION btracker_backend.bh_savings_balance(
    _account_id INT,
    _coin_type INT,
    _from INT,
    _to INT
)
RETURNS btracker_backend.balance_history_range_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql' STABLE
SET JIT = OFF
AS
$$
DECLARE
  __to_seq INT;
  __from_seq INT;
BEGIN
  IF _to IS NULL THEN
    __to_seq := (
      SELECT
        ab.balance_seq_no
      FROM btracker_backend.account_savings_history_view ab
      WHERE ab.account = _account_id
        AND ab.nai = _coin_type
      ORDER BY ab.balance_seq_no DESC LIMIT 1
    );
  ELSE
    __to_seq := (
      WITH last_block AS (
        SELECT
          ab.source_op_block
        FROM btracker_backend.account_savings_history_view ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block <= _to
        ORDER BY ab.source_op_block DESC LIMIT 1
      ),
      get_sequence AS (
        SELECT
          ab.balance_seq_no
        FROM btracker_backend.account_savings_history_view ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block = (SELECT source_op_block FROM last_block)
      )
      SELECT MAX(gs.balance_seq_no)
      FROM get_sequence gs
    );
  END IF;

  IF _from IS NULL THEN
    __from_seq := (
      SELECT
        ab.balance_seq_no
      FROM btracker_backend.account_savings_history_view ab
      WHERE ab.account = _account_id
        AND ab.nai = _coin_type
      ORDER BY ab.balance_seq_no ASC LIMIT 1
    );
  ELSE
    __from_seq := (
      WITH first_block AS (
        SELECT
          ab.source_op_block
        FROM btracker_backend.account_savings_history_view ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block >= _from
        ORDER BY ab.source_op_block ASC LIMIT 1
      ),
      get_sequence AS (
        SELECT
          ab.balance_seq_no
        FROM btracker_backend.account_savings_history_view ab
        WHERE ab.account = _account_id
          AND ab.nai = _coin_type
          AND ab.source_op_block = (SELECT source_op_block FROM first_block)
      )
      SELECT MIN(gs.balance_seq_no)
      FROM get_sequence gs
    );
  END IF;

  RETURN (
    COALESCE((__to_seq - __from_seq + 1), 0),
    COALESCE(__from_seq, 0),
    COALESCE(__to_seq, 0)
  )::btracker_backend.balance_history_range_return;
END
$$;

/**
 * aggregated_history_paging_return
 * ---------------------------------
 * Return type for aggregated history paging function.
 *
 * Fields:
 * - from_block:     Validated starting block number
 * - to_block:       Validated ending block number (capped at current block)
 * - from_timestamp: Block timestamp truncated to granularity (day/month)
 * - to_timestamp:   Block timestamp truncated to granularity (day/month)
 *
 * The timestamps are truncated to the specified granularity for proper
 * grouping with aggregated history tables (balance_history_by_day, etc.).
 */
DROP TYPE IF EXISTS btracker_backend.aggregated_history_paging_return CASCADE;
CREATE TYPE btracker_backend.aggregated_history_paging_return AS
(
    from_block INT,
    to_block INT,
    from_timestamp TIMESTAMP,
    to_timestamp TIMESTAMP
);

/**
 * aggregated_history_block_range()
 * ---------------------------------
 * Convert block range to timestamp range for aggregated history queries.
 *
 * This function is used when querying balance_history_by_day or
 * balance_history_by_month tables. It:
 * 1. Validates and adjusts block boundaries
 * 2. Converts blocks to timestamps
 * 3. Truncates timestamps to the requested granularity
 *
 * Block Boundary Handling:
 * ------------------------
 * - _to: If NULL, uses current block. If > current block, caps to current.
 * - _from: If NULL, queries first block in hive.blocks_view (supports pruned HAF)
 *
 * Timestamp Truncation:
 * ---------------------
 * Uses DATE_TRUNC with the specified granularity:
 * - 'day': Truncates to start of day (00:00:00)
 * - 'month': Truncates to start of month (first day, 00:00:00)
 *
 * This is essential for matching against aggregated history tables,
 * which store data keyed by truncated timestamps.
 *
 * Pruned HAF Support:
 * -------------------
 * In pruned HAF installations, block 1 may not exist. This function
 * queries the actual first available block from hive.blocks_view
 * instead of assuming block 1.
 *
 * @param _from          Starting block (NULL for earliest available)
 * @param _to            Ending block (NULL for current block)
 * @param _current_block Current processed block number
 * @param _granularity   Time truncation granularity ('day' or 'month')
 * @returns              aggregated_history_paging_return with blocks and timestamps
 */
CREATE OR REPLACE FUNCTION btracker_backend.aggregated_history_block_range(
    _from INT,
    _to INT,
    _current_block INT,
    _granularity TEXT
)
RETURNS btracker_backend.aggregated_history_paging_return -- noqa: LT01, CP05
LANGUAGE 'plpgsql' IMMUTABLE
SET JIT = OFF
AS
$$
DECLARE
  _from_timestamp TIMESTAMP;
  _to_timestamp TIMESTAMP;
BEGIN

  -- setting _from and _to based on current block
  _to := (
    CASE
      WHEN (_to IS NULL) THEN
        _current_block
      WHEN (_to IS NOT NULL) AND (_current_block < _to) THEN
        _current_block
      ELSE
        _to
      END
  );

  _from := (
    CASE
      WHEN (_from IS NULL) THEN
        (SELECT b.num FROM hive.blocks_view b ORDER BY b.num ASC LIMIT 1) -- in pruned haf first block might not be 1
      ELSE
        _from
      END
  );

  -- setting timestamps based on granularity
  _from_timestamp := DATE_TRUNC(_granularity, (SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _from)::TIMESTAMP);

  _to_timestamp   := DATE_TRUNC(_granularity, (SELECT b.created_at FROM hive.blocks_view b WHERE b.num = _to)::TIMESTAMP);


  RETURN (_from, _to, _from_timestamp, _to_timestamp)::btracker_backend.aggregated_history_paging_return;
END
$$;

RESET ROLE;
