SET ROLE btracker_owner;

-- Hardfork-era constants for withdrawal calculations
-- These are Hive blockchain protocol constants that changed at specific hardforks
--
-- These values are protocol-defined and will never change for historical blocks.
-- Using IMMUTABLE functions allows PostgreSQL to optimize/inline the results.

-- Pre-HF1 VESTS precision multiplier
-- Before HF1, VESTS were stored with lower precision. This multiplier converts
-- pre-HF1 VESTS amounts to the current precision scale.
-- Post-HF1: No conversion needed (returns 1)
-- Pre-HF1: Multiply by 1,000,000 to match current precision
CREATE OR REPLACE FUNCTION btracker_backend.vests_precision_multiplier(_is_post_hf1 BOOLEAN)
RETURNS INT LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  RETURN CASE WHEN _is_post_hf1 THEN 1 ELSE 1000000 END;
END;
$$;

-- Withdrawal rate divisor (number of weekly installments)
-- Determines how many weeks a power-down takes to complete
-- Pre-HF16: 104 weeks (2 years)
-- Post-HF16: 13 weeks (~3 months)
CREATE OR REPLACE FUNCTION btracker_backend.withdrawal_rate_weeks(_is_post_hf16 BOOLEAN)
RETURNS INT LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  RETURN CASE WHEN _is_post_hf16 THEN 13 ELSE 104 END;
END;
$$;

-- Hardfork number constants
-- These are the Hive blockchain hardfork identifiers used in withdrawal processing
-- HF1: Changed VESTS precision
CREATE OR REPLACE FUNCTION btracker_backend.hf_vests_precision()
RETURNS INT LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  RETURN 1;
END;
$$;

-- HF16: Changed withdrawal rate from 104 to 13 weeks
CREATE OR REPLACE FUNCTION btracker_backend.hf_withdraw_rate()
RETURNS INT LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  RETURN 16;
END;
$$;

-- HF24: Introduced delayed voting
CREATE OR REPLACE FUNCTION btracker_backend.hf_delayed_voting()
RETURNS INT LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
  RETURN 24;
END;
$$;

RESET ROLE;
