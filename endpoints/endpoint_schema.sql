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
  version: 1.27.11
externalDocs:
  description: Balance tracker gitlab repository
  url: https://gitlab.syncad.com/hive/balance_tracker
tags:
  - name: Accounts
    description: Informations about account balances
  - name: Transfers
    description: Information about transfers and their history
  - name: Other
    description: General API information
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
CREATE SCHEMA IF NOT EXISTS btracker_backend AUTHORIZATION btracker_owner;

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
    "version": "1.27.11"
  },
  "externalDocs": {
    "description": "Balance tracker gitlab repository",
    "url": "https://gitlab.syncad.com/hive/balance_tracker"
  },
  "tags": [
    {
      "name": "Accounts",
      "description": "Informations about account balances"
    },
    {
      "name": "Transfers",
      "description": "Information about transfers and their history"
    },
    {
      "name": "Other",
      "description": "General API information"
    }
  ],
  "servers": [
    {
      "url": "/balance-api"
    }
  ],
  "components": {
    "schemas": {
      "btracker_backend.granularity": {
        "type": "string",
        "enum": [
          "daily",
          "monthly",
          "yearly"
        ]
      },
      "btracker_backend.granularity_hourly": {
        "type": "string",
        "enum": [
          "hourly",
          "daily",
          "monthly",
          "yearly"
        ]
      },
      "btracker_backend.nai_type": {
        "type": "string",
        "enum": [
          "HBD",
          "HIVE",
          "VESTS"
        ]
      },
      "btracker_backend.liquid_nai_type": {
        "type": "string",
        "enum": [
          "HBD",
          "HIVE"
        ]
      },
      "btracker_backend.balance_type": {
        "type": "string",
        "enum": [
          "balance",
          "savings_balance"
        ]
      },
      "btracker_backend.sort_direction": {
        "type": "string",
        "enum": [
          "asc",
          "desc"
        ]
      },
      "btracker_backend.balance": {
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
      },
      "btracker_backend.balances": {
        "type": "object",
        "properties": {
          "balance": {
            "type": "string",
            "description": "aggregated account''s balance"
          },
          "savings_balance": {
            "type": "string",
            "description": "aggregated account''s savings balance"
          }
        }
      },
      "btracker_backend.aggregated_history": {
        "type": "object",
        "properties": {
          "date": {
            "type": "string",
            "format": "date-time",
            "description": "date of the balance aggregation"
          },
          "balance": {
            "$ref": "#/components/schemas/btracker_backend.balances",
            "description": "aggregated account''s balance"
          },
          "prev_balance": {
            "$ref": "#/components/schemas/btracker_backend.balances",
            "description": "aggregated account''s balance from the previous day/month/year"
          },
          "min_balance": {
            "$ref": "#/components/schemas/btracker_backend.balances",
            "description": "minimum account''s balance in the aggregation period"
          },
          "max_balance": {
            "$ref": "#/components/schemas/btracker_backend.balances",
            "description": "maximum account''s balance in the aggregation period"
          }
        }
      },
      "btracker_backend.balance_history": {
        "type": "object",
        "properties": {
          "block_num": {
            "type": "integer",
            "description": "block number"
          },
          "operation_id": {
            "type": "string",
            "description": "unique operation identifier with an encoded block number and operation type id"
          },
          "op_type_id": {
            "type": "integer",
            "description": "operation type identifier"
          },
          "balance": {
            "type": "string",
            "description": "The closing balance"
          },
          "prev_balance": {
            "type": "string",
            "description": "Balance in previous day/month/year"
          },
          "balance_change": {
            "type": "string",
            "description": "Diffrence between balance and prev_balance"
          },
          "timestamp": {
            "type": "string",
            "format": "date-time",
            "description": "Creation date"
          }
        }
      },
      "btracker_backend.operation_history": {
        "type": "object",
        "properties": {
          "total_operations": {
            "type": "integer",
            "description": "Total number of operations"
          },
          "total_pages": {
            "type": "integer",
            "description": "Total number of pages"
          },
          "operations_result": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/btracker_backend.balance_history"
            },
            "description": "List of operation results"
          }
        }
      },
      "btracker_backend.incoming_delegations": {
        "type": "object",
        "properties": {
          "delegator": {
            "type": "string",
            "description": "account name of the delegator"
          },
          "amount": {
            "type": "string",
            "description": "amount of vests delegated"
          },
          "operation_id": {
            "type": "string",
            "description": "unique operation identifier with an encoded block number and operation type id"
          },
          "block_num": {
            "type": "integer",
            "description": "block number"
          }
        }
      },
      "btracker_backend.outgoing_delegations": {
        "type": "object",
        "properties": {
          "delegatee": {
            "type": "string",
            "description": "account name of the delegatee"
          },
          "amount": {
            "type": "string",
            "description": "amount of vests delegated"
          },
          "operation_id": {
            "type": "string",
            "description": "unique operation identifier with an encoded block number and operation type id"
          },
          "block_num": {
            "type": "integer",
            "description": "block number"
          }
        }
      },
      "btracker_backend.delegations": {
        "type": "object",
        "properties": {
          "outgoing_delegations": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/btracker_backend.outgoing_delegations"
            },
            "description": "List of outgoing delegations from the account"
          },
          "incoming_delegations": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/btracker_backend.incoming_delegations"
            },
            "description": "List of incoming delegations to the account"
          }
        }
      },
      "btracker_backend.amount": {
        "type": "object",
        "properties": {
          "nai": {
            "type": "string"
          },
          "amount": {
            "type": "string"
          },
          "precision": {
            "type": "integer"
          }
        }
      },
      "btracker_backend.incoming_recurrent_transfers": {
        "type": "object",
        "properties": {
          "from": {
            "type": "string",
            "description": "Account name of the sender"
          },
          "pair_id": {
            "type": "integer",
            "description": "ID of the pair of accounts"
          },
          "amount": {
            "$ref": "#/components/schemas/btracker_backend.amount",
            "description": "Amount of the transfer with NAI and precision"
          },
          "consecutive_failures": {
            "type": "integer",
            "description": "amount of consecutive failures"
          },
          "remaining_executions": {
            "type": "integer",
            "description": "Remaining executions"
          },
          "recurrence": {
            "type": "integer",
            "description": "Recurrence in hours"
          },
          "memo": {
            "type": "string",
            "description": "Memo message"
          },
          "trigger_date": {
            "type": "string",
            "format": "date-time",
            "description": "Date of the next trigger of the transfer"
          },
          "operation_id": {
            "type": "string",
            "description": "Unique operation identifier with an encoded block number and operation type id"
          },
          "block_num": {
            "type": "integer",
            "description": "Block number"
          }
        }
      },
      "btracker_backend.outgoing_recurrent_transfers": {
        "type": "object",
        "properties": {
          "to": {
            "type": "string",
            "description": "Account name of the receiver"
          },
          "pair_id": {
            "type": "integer",
            "description": "ID of the pair of accounts"
          },
          "amount": {
            "$ref": "#/components/schemas/btracker_backend.amount",
            "description": "Amount of the transfer with NAI and precision"
          },
          "consecutive_failures": {
            "type": "integer",
            "description": "amount of consecutive failures"
          },
          "remaining_executions": {
            "type": "integer",
            "description": "Remaining executions"
          },
          "recurrence": {
            "type": "integer",
            "description": "Recurrence in hours"
          },
          "memo": {
            "type": "string",
            "description": "Memo message"
          },
          "trigger_date": {
            "type": "string",
            "format": "date-time",
            "description": "Date of the next trigger of the transfer"
          },
          "operation_id": {
            "type": "string",
            "description": "Unique operation identifier with an encoded block number and operation type id"
          },
          "block_num": {
            "type": "integer",
            "description": "Block number"
          }
        }
      },
      "btracker_backend.recurrent_transfers": {
        "type": "object",
        "properties": {
          "outgoing_recurrent_transfers": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/btracker_backend.outgoing_recurrent_transfers"
            },
            "description": "List of outgoing recurrent transfers from the account"
          },
          "incoming_recurrent_transfers": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/btracker_backend.incoming_recurrent_transfers"
            },
            "description": "List of incoming recurrent transfers to the account"
          }
        }
      },
      "btracker_backend.transfer_stats": {
        "type": "object",
        "properties": {
          "date": {
            "type": "string",
            "format": "date-time",
            "description": "the time transfers were included in the blockchain"
          },
          "total_transfer_amount": {
            "type": "string",
            "description": "sum of a amount of transfered tokens in the period"
          },
          "average_transfer_amount": {
            "type": "string",
            "description": "average amount of transfered tokens in the period"
          },
          "maximum_transfer_amount": {
            "type": "string",
            "description": "maximum amount of transfered tokens in the period"
          },
          "minimum_transfer_amount": {
            "type": "string",
            "description": "minimum amount of transfered tokens in the period"
          },
          "transfer_count": {
            "type": "integer",
            "description": "number of transfers in the period"
          },
          "last_block_num": {
            "type": "integer",
            "description": "last block number in time range"
          }
        }
      },
      "btracker_backend.array_of_transfer_stats": {
        "type": "array",
        "items": {
          "$ref": "#/components/schemas/btracker_backend.transfer_stats"
        }
      },
      "btracker_backend.array_of_aggregated_history": {
        "type": "array",
        "items": {
          "$ref": "#/components/schemas/btracker_backend.aggregated_history"
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
            "description": "Account balances \n(VEST balances are represented as string due to json limitations)\n\n* Returns `btracker_backend.balance`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_backend.balance"
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
              "$ref": "#/components/schemas/btracker_backend.nai_type"
            },
            "description": "Coin types:\n\n* HBD\n\n* HIVE\n\n* VESTS\n"
          },
          {
            "in": "query",
            "name": "balance-type",
            "required": false,
            "schema": {
              "$ref": "#/components/schemas/btracker_backend.balance_type",
              "default": "balance"
            },
            "description": "Balance types:\n\n* balance\n\n* savings_balance\n"
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
              "$ref": "#/components/schemas/btracker_backend.sort_direction",
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
            "description": "Balance change\n\n* Returns `btracker_backend.operation_history`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_backend.operation_history"
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
        "description": "History of change of `coin-type` balance in given block range with granularity of day/month/year\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_balance_aggregation(''blocktrades'', ''VESTS'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/accounts/blocktrades/aggregated-history?coin-type=VESTS''`\n",
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
              "$ref": "#/components/schemas/btracker_backend.nai_type"
            },
            "description": "Coin types:\n\n* HBD\n\n* HIVE\n\n* VESTS\n"
          },
          {
            "in": "query",
            "name": "granularity",
            "required": false,
            "schema": {
              "$ref": "#/components/schemas/btracker_backend.granularity",
              "default": "yearly"
            },
            "description": "granularity types:\n\n* daily\n\n* monthly\n\n* yearly\n"
          },
          {
            "in": "query",
            "name": "direction",
            "required": false,
            "schema": {
              "$ref": "#/components/schemas/btracker_backend.sort_direction",
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
            "description": "Balance change\n\n* Returns array of `btracker_backend.aggregated_history`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_backend.array_of_aggregated_history"
                },
                "example": [
                  {
                    "date": "2016-12-31T23:59:59",
                    "balance": {
                      "balance": "8172549681941451",
                      "savings_balance": "0"
                    },
                    "prev_balance": {
                      "balance": "0",
                      "savings_balance": "0"
                    },
                    "min_balance": {
                      "balance": "1000000000000",
                      "savings_balance": "0"
                    },
                    "max_balance": {
                      "balance": "8436182707535769",
                      "savings_balance": "0"
                    }
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
        "description": "List of incoming and outgoing delegations\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_balance_delegations(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/accounts/blocktrades/delegations''`\n",
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
            "description": "Incoming and outgoing delegations\n\n* Returns `btracker_backend.delegations`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_backend.delegations"
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
    },
    "/accounts/{account-name}/recurrent-transfers": {
      "get": {
        "tags": [
          "Accounts"
        ],
        "summary": "Account recurrent transfers",
        "description": "List of incoming and outgoing recurrent transfers\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_recurrent_transfers(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/accounts/blocktrades/recurrent-transfers''`\n",
        "operationId": "btracker_endpoints.get_recurrent_transfers",
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
            "description": "Incoming and outgoing recurrent transfers\n\n* Returns `btracker_backend.recurrent_transfers`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_backend.recurrent_transfers"
                },
                "example": {
                  "outgoing_recurrent_transfers": [],
                  "incoming_recurrent_transfers": []
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
    "/transfer-statistics": {
      "get": {
        "tags": [
          "Transfers"
        ],
        "summary": "Aggregated transfer statistics",
        "description": "History of amount of transfers per hour, day, month or year.\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_transfer_statistics(''HBD'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/transfer-statistics''`\n",
        "operationId": "btracker_endpoints.get_transfer_statistics",
        "parameters": [
          {
            "in": "query",
            "name": "coin-type",
            "required": true,
            "schema": {
              "$ref": "#/components/schemas/btracker_backend.liquid_nai_type"
            },
            "description": "Coin types:\n\n* HBD\n\n* HIVE\n"
          },
          {
            "in": "query",
            "name": "granularity",
            "required": false,
            "schema": {
              "$ref": "#/components/schemas/btracker_backend.granularity_hourly",
              "default": "yearly"
            },
            "description": "granularity types:\n\n* hourly\n\n* daily\n\n* monthly\n\n* yearly\n"
          },
          {
            "in": "query",
            "name": "direction",
            "required": false,
            "schema": {
              "$ref": "#/components/schemas/btracker_backend.sort_direction",
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
            "description": "Balance change\n\n* Returns array of `btracker_backend.transfer_stats`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_backend.array_of_transfer_stats"
                },
                "example": [
                  {
                    "date": "2016-12-31T23:59:59",
                    "total_transfer_amount": "69611921266",
                    "average_transfer_amount": "1302405",
                    "maximum_transfer_amount": "18000000",
                    "minimum_transfer_amount": "1",
                    "transfer_count": 54665,
                    "last_block_num": 5000000
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
    "/version": {
      "get": {
        "tags": [
          "Other"
        ],
        "summary": "Get Balance tracker''s version",
        "description": "Get Balance tracker''s last commit hash (versions set by by hash value).\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_btracker_version();`\n\nREST call example\n* `GET ''https://%1$s/balance-api/version''`\n",
        "operationId": "btracker_endpoints.get_btracker_version",
        "responses": {
          "200": {
            "description": "Balance tracker version\n\n* Returns `TEXT`\n",
            "content": {
              "application/json": {
                "schema": {
                  "type": "string"
                },
                "example": "c2fed8958584511ef1a66dab3dbac8c40f3518f0"
              }
            }
          },
          "404": {
            "description": "App not installed"
          }
        }
      }
    },
    "/last-synced-block": {
      "get": {
        "tags": [
          "Other"
        ],
        "summary": "Get last block number synced by balance tracker",
        "description": "Get the block number of the last block synced by balance tracker.\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_btracker_last_synced_block();`\n\nREST call example\n* `GET ''https://%1$s/balance-api/last-synced-block''`\n",
        "operationId": "btracker_endpoints.get_btracker_last_synced_block",
        "responses": {
          "200": {
            "description": "Last synced block by balance tracker\n\n* Returns `INT`\n",
            "content": {
              "application/json": {
                "schema": {
                  "type": "integer"
                },
                "example": 5000000
              }
            }
          },
          "404": {
            "description": "No blocks synced"
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
