SET ROLE btracker_owner;

/** openapi
openapi: 3.1.0
info:
  title: Balance tracker
  description: >-
    Balance tracker is an API for querying information about
    account balances and their history included in Hive blocks.
  license:
    name: MIT License
    url: https://opensource.org/license/mit
  version: 1.27.5
externalDocs:
  description: Balance tracker gitlab repository
  url: https://gitlab.syncad.com/hive/balance_tracker
tags:
  - name: Accounts
    description: Informations about account balances

servers:
  - url: /balance-api
 */

DO $__$
DECLARE 
  __schema_name VARCHAR;
  __swagger_url TEXT;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;
  __swagger_url := current_setting('custom.swagger_url')::TEXT;

CREATE SCHEMA IF NOT EXISTS btracker_endpoints AUTHORIZATION btracker_owner;

EXECUTE FORMAT(
'create or replace function btracker_endpoints.root() returns json as $_$
declare
-- openapi-spec
-- openapi-generated-code-begin
  openapi json = $$
{
  "openapi": "3.1.0",
  "info": {
    "title": "Balance tracker",
    "description": "Balance tracker is an API for querying information about account balances and their history included in Hive blocks.",
    "license": {
      "name": "MIT License",
      "url": "https://opensource.org/license/mit"
    },
    "version": "1.27.5"
  },
  "externalDocs": {
    "description": "Balance tracker gitlab repository",
    "url": "https://gitlab.syncad.com/hive/balance_tracker"
  },
  "tags": [
    {
      "name": "Accounts",
      "description": "Informations about account balances"
    }
  ],
  "servers": [
    {
      "url": "/balance-api"
    }
  ],
  "components": {
    "schemas": {
      "btracker_endpoints.nai_type": {
        "type": "string",
        "enum": [
          "HBD",
          "HIVE",
          "VESTS"
        ]
      },
      "btracker_endpoints.sort_direction": {
        "type": "string",
        "enum": [
          "asc",
          "desc"
        ]
      },
      "btracker_endpoints.balances": {
        "type": "object",
        "properties": {
          "hbd_balance": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "number of HIVE backed dollars the account has"
          },
          "hive_balance": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "account''s HIVE balance"
          },
          "vesting_shares": {
            "type": "string",
            "description": "account''s VEST balance"
          },
          "vesting_balance_hive": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "the VEST balance, presented in HIVE,  is calculated based on the current HIVE price"
          },
          "post_voting_power_vests": {
            "type": "string",
            "description": "account''s VEST balance - delegated VESTs + reveived VESTs, presented in HIVE,calculated based on the current HIVE price"
          },
          "delegated_vests": {
            "type": "string",
            "description": "VESTS delegated to another user,  account''s power is lowered by delegated VESTS"
          },
          "received_vests": {
            "type": "string",
            "description": "VESTS received from another user,  account''s power is increased by received VESTS"
          },
          "curation_rewards": {
            "type": "string",
            "description": "rewards obtained by posting and commenting expressed in VEST"
          },
          "posting_rewards": {
            "type": "string",
            "description": "curator''s reward expressed in VEST"
          },
          "hbd_rewards": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "not yet claimed HIVE backed dollars  stored in hbd reward balance"
          },
          "hive_rewards": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "not yet claimed HIVE  stored in hive reward balance"
          },
          "vests_rewards": {
            "type": "string",
            "description": "not yet claimed VESTS  stored in vest reward balance"
          },
          "hive_vesting_rewards": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "the reward vesting balance, denominated in HIVE,  is determined by the prevailing HIVE price at the time of reward reception"
          },
          "hbd_savings": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "saving balance of HIVE backed dollars"
          },
          "hive_savings": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "HIVE saving balance"
          },
          "savings_withdraw_requests": {
            "type": "integer",
            "description": "number representing how many payouts are pending  from user''s saving balance"
          },
          "vesting_withdraw_rate": {
            "type": "string",
            "description": "received until the withdrawal is complete,  with each installment amounting to 1/13 of the withdrawn total"
          },
          "to_withdraw": {
            "type": "string",
            "description": "the remaining total VESTS needed to complete withdrawals"
          },
          "withdrawn": {
            "type": "string",
            "description": "the total VESTS already withdrawn from active withdrawals"
          },
          "withdraw_routes": {
            "type": "integer",
            "description": "list of account receiving the part of a withdrawal"
          },
          "delayed_vests": {
            "type": "string",
            "description": "blocked VESTS by a withdrawal"
          }
        }
      }
    }
  },
  "paths": {
    "/accounts/{account-name}/balance": {
      "get": {
        "tags": [
          "Accounts"
        ],
        "summary": "Account balances",
        "description": "Lists account hbd, hive and vest balances\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_balances(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/accounts/blocktrades/balance''`\n",
        "operationId": "btracker_endpoints.get_account_balances",
        "parameters": [
          {
            "in": "path",
            "name": "account-name",
            "required": true,
            "schema": {
              "type": "string"
            },
            "description": "Name of the account"
          }
        ],
        "responses": {
          "200": {
            "description": "Account balances \n(VEST balances are represented as string due to json limitations)\n\n* Returns `btracker_endpoints.balances`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.balances"
                },
                "example": [
                  {
                    "hbd_balance": 77246982,
                    "hive_balance": 29594875,
                    "vesting_shares": "8172549681941451",
                    "vesting_balance_hive": 2720696229,
                    "post_voting_power_vests": "8172549681941451",
                    "delegated_vests": "0",
                    "received_vests": "0",
                    "curation_rewards": "196115157",
                    "posting_rewards": "65916519",
                    "hbd_rewards": 0,
                    "hive_rewards": 0,
                    "vests_rewards": "0",
                    "hive_vesting_rewards": 0,
                    "hbd_savings": 0,
                    "hive_savings": 0,
                    "savings_withdraw_requests": 0,
                    "vesting_withdraw_rate": "80404818220529",
                    "to_withdraw": "8362101094935031",
                    "withdrawn": "804048182205290",
                    "withdraw_routes": 4,
                    "delayed_vests": "0"
                  }
                ]
              }
            }
          },
          "404": {
            "description": "No such account in the database"
          }
        }
      }
    },
    "/accounts/{account-name}/balance-history": {
      "get": {
        "tags": [
          "Accounts"
        ],
        "summary": "Historical balance change",
        "description": "History of change of `coin-type` balance in given block range\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_balance_history(''blocktrades'', 37, 1 ,2);`\n\nREST call example\n* `GET ''https://%1$s/balance-api/accounts/blocktrades/balance-history?page-size=2''`\n",
        "operationId": "btracker_endpoints.get_balance_history",
        "parameters": [
          {
            "in": "path",
            "name": "account-name",
            "required": true,
            "schema": {
              "type": "string"
            },
            "description": "Name of the account"
          },
          {
            "in": "query",
            "name": "coin-type",
            "required": false,
            "schema": {
              "$ref": "#/components/schemas/btracker_endpoints.nai_type"
            },
            "description": "Coin types:\n\n* HBD\n\n* HIVE\n\n* VESTS\n"
          },
          {
            "in": "query",
            "name": "page",
            "required": false,
            "schema": {
              "type": "integer",
              "default": 1
            },
            "description": "Return page on `page` number, defaults to `1`\n"
          },
          {
            "in": "query",
            "name": "page-size",
            "required": false,
            "schema": {
              "type": "integer",
              "default": 100
            },
            "description": "Return max `page-size` operations per page, defaults to `100`"
          },
          {
            "in": "query",
            "name": "direction",
            "required": false,
            "schema": {
              "$ref": "#/components/schemas/btracker_endpoints.sort_direction",
              "default": "desc"
            },
            "description": "Sort order:\n\n * `asc` - Ascending, from oldest to newest \n\n * `desc` - Descending, from newest to oldest \n"
          },
          {
            "in": "query",
            "name": "from-block",
            "required": false,
            "schema": {
              "type": "integer",
              "x-sql-datatype": "BIGINT",
              "default": null
            },
            "description": "Lower limit of the block range"
          },
          {
            "in": "query",
            "name": "to-block",
            "required": false,
            "schema": {
              "type": "integer",
              "x-sql-datatype": "BIGINT",
              "default": null
            },
            "description": "Upper limit of the block range"
          },
          {
            "in": "query",
            "name": "start-date",
            "required": false,
            "schema": {
              "type": "string",
              "format": "date-time",
              "default": null
            },
            "description": "Lower limit of the time range"
          },
          {
            "in": "query",
            "name": "end-date",
            "required": false,
            "schema": {
              "type": "string",
              "format": "date-time",
              "default": null
            },
            "description": "Upper limit of the time range"
          }
        ],
        "responses": {
          "200": {
            "description": "Balance change\n",
            "content": {
              "application/json": {
                "schema": {
                  "type": "string",
                  "x-sql-datatype": "JSON"
                },
                "example": [
                  "blocktrade",
                  "blocktrades"
                ]
              }
            }
          },
          "404": {
            "description": "No such account in the database"
          }
        }
      }
    }
  }
}
$$;
-- openapi-generated-code-end
begin
  return openapi;
end
$_$ language plpgsql;'
, __swagger_url);

END
$__$;

RESET ROLE;
