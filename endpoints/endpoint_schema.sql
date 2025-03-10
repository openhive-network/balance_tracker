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
  version: 1.27.10
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
    "version": "1.27.10"
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
      "btracker_endpoints.granularity": {
        "type": "string",
        "enum": [
          "daily",
          "monthly",
          "yearly"
        ]
      },
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
    "/accounts/{account-name}/balances": {
      "get": {
        "tags": [
          "Accounts"
        ],
        "summary": "Account balances",
        "description": "Lists account hbd, hive and vest balances\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_balances(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/accounts/blocktrades/balances''`\n",
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
                "example": {
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
        "description": "History of change of `coin-type` balance in given block range\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_balance_history(''blocktrades'', 37, 1 ,2);`\n\nREST call example\n* `GET ''https://%1$s/balance-api/accounts/blocktrades/balance-history?coin-type=VEST&page-size=2''`\n",
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
            "required": true,
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
              "type": "string",
              "default": null
            },
            "description": "Lower limit of the block range, can be represented either by a block-number (integer) or a timestamp (in the format YYYY-MM-DD HH:MI:SS).\n\nThe provided `timestamp` will be converted to a `block-num` by finding the first block \nwhere the block''s `created_at` is more than or equal to the given `timestamp` (i.e. `block''s created_at >= timestamp`).\n\nThe function will interpret and convert the input based on its format, example input:\n\n* `2016-09-15 19:47:21`\n\n* `5000000`\n"
          },
          {
            "in": "query",
            "name": "to-block",
            "required": false,
            "schema": {
              "type": "string",
              "default": null
            },
            "description": "Similar to the from-block parameter, can either be a block-number (integer) or a timestamp (formatted as YYYY-MM-DD HH:MI:SS). \n\nThe provided `timestamp` will be converted to a `block-num` by finding the first block \nwhere the block''s `created_at` is less than or equal to the given `timestamp` (i.e. `block''s created_at <= timestamp`).\n\nThe function will convert the value depending on its format, example input:\n\n* `2016-09-15 19:47:21`\n\n* `5000000`\n"
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
                "example": {
                  "total_operations": 188291,
                  "total_pages": 94146,
                  "operations_result": [
                    {
                      "block_num": 4999992,
                      "operation_id": "21474802120262208",
                      "op_type_id": 64,
                      "balance": "8172549681941451",
                      "prev_balance": "8172546678091286",
                      "balance_change": "3003850165",
                      "timestamp": "2016-09-15T19:46:57"
                    },
                    {
                      "block_num": 4999959,
                      "operation_id": "21474660386343488",
                      "op_type_id": 64,
                      "balance": "8172546678091286",
                      "prev_balance": "8172543674223181",
                      "balance_change": "3003868105",
                      "timestamp": "2016-09-15T19:45:12"
                    }
                  ]
                }
              }
            }
          },
          "404": {
            "description": "No such account in the database"
          }
        }
      }
    },
    "/accounts/{account-name}/aggregated-history": {
      "get": {
        "tags": [
          "Accounts"
        ],
        "summary": "Aggregated account balance history",
        "description": "History of change of `coin-type` balance in given block range\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_balance_aggregation(''blocktrades'', ''VESTS'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/accounts/blocktrades/aggregated-history?coin-type=VESTS''`\n",
        "operationId": "btracker_endpoints.get_balance_aggregation",
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
            "required": true,
            "schema": {
              "$ref": "#/components/schemas/btracker_endpoints.nai_type"
            },
            "description": "Coin types:\n\n* HBD\n\n* HIVE\n\n* VESTS\n"
          },
          {
            "in": "query",
            "name": "granularity",
            "required": false,
            "schema": {
              "$ref": "#/components/schemas/btracker_endpoints.granularity",
              "default": "yearly"
            },
            "description": "granularity types:\n\n* daily\n\n* monthly\n\n* yearly\n"
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
              "type": "string",
              "default": null
            },
            "description": "Lower limit of the block range, can be represented either by a block-number (integer) or a timestamp (in the format YYYY-MM-DD HH:MI:SS).\n\nThe provided `timestamp` will be converted to a `block-num` by finding the first block \nwhere the block''s `created_at` is more than or equal to the given `timestamp` (i.e. `block''s created_at >= timestamp`).\n\nThe function will interpret and convert the input based on its format, example input:\n\n* `2016-09-15 19:47:21`\n\n* `5000000`\n"
          },
          {
            "in": "query",
            "name": "to-block",
            "required": false,
            "schema": {
              "type": "string",
              "default": null
            },
            "description": "Similar to the from-block parameter, can either be a block-number (integer) or a timestamp (formatted as YYYY-MM-DD HH:MI:SS). \n\nThe provided `timestamp` will be converted to a `block-num` by finding the first block \nwhere the block''s `created_at` is less than or equal to the given `timestamp` (i.e. `block''s created_at <= timestamp`).\n\nThe function will convert the value depending on its format, example input:\n\n* `2016-09-15 19:47:21`\n\n* `5000000`\n"
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
                  {
                    "date": "2016-12-31T23:59:59",
                    "prev_balance": "0",
                    "balance": "8172549681941451",
                    "min_balance": "1000000000000",
                    "max_balance": "8436182707535769"
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
    "/accounts/{account-name}/delegations": {
      "get": {
        "tags": [
          "Accounts"
        ],
        "summary": "Account delegations",
        "description": "History of change of `coin-type` balance in given block range\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_balance_delegations(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/accounts/blocktrades/delegations''`\n",
        "operationId": "btracker_endpoints.get_balance_delegations",
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
            "description": "Balance change\n",
            "content": {
              "application/json": {
                "schema": {
                  "type": "string",
                  "x-sql-datatype": "JSON"
                },
                "example": {
                  "outgoing_delegations": [],
                  "incoming_delegations": []
                }
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
