CREATE OR REPLACE FUNCTION btracker_app.process_comment_operation(body jsonb, _timestamp TIMESTAMP, is_root_post BOOLEAN)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
_account INT := (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'author');
BEGIN
IF is_root_post = TRUE THEN

    INSERT INTO btracker_app.account_posts
      (
      account,
      last_post,
      last_root_post
      ) 
      SELECT
        _account,
        _timestamp,
        _timestamp

      ON CONFLICT ON CONSTRAINT pk_account_posts
      DO UPDATE SET
          last_post = EXCLUDED.last_post,
          last_root_post = EXCLUDED.last_root_post;

ELSE

    INSERT INTO btracker_app.account_posts
      (
      account,
      last_post
      ) 
      SELECT
        _account,
        _timestamp

      ON CONFLICT ON CONSTRAINT pk_account_posts
      DO UPDATE SET
          last_post = EXCLUDED.last_post,
          last_root_post = EXCLUDED.last_root_post;
END IF;

END
$$
;

CREATE OR REPLACE FUNCTION btracker_app.process_vote_operation(body jsonb, _timestamp TIMESTAMP)
RETURNS VOID
LANGUAGE 'plpgsql'
AS
$$
DECLARE
_account INT := (SELECT id FROM hive.btracker_app_accounts_view WHERE name = (body)->'value'->>'voter');
BEGIN

  INSERT INTO btracker_app.account_posts
    (
    account,
    last_vote_time
    ) 
    SELECT
      _account,
      _timestamp

    ON CONFLICT ON CONSTRAINT pk_account_posts
    DO UPDATE SET
        last_vote_time = EXCLUDED.last_vote_time;
END
$$
;