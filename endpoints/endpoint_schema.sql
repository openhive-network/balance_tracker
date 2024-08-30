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
  - name: Balance-for-coins
    description: Historical informations about account balances (deprecated API)
  - name: Account-balances
    description: Backend APIs for HAF block explorer, returns informations about account balances
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
      "name": "Balance-for-coins",
      "description": "Historical informations about account balances (deprecated API)"
    },
    {
      "name": "Account-balances",
      "description": "Backend APIs for HAF block explorer, returns informations about account balances"
    }
  ],
  "servers": [
    {
      "url": "/balance-api"
    }
  ],
  "paths": {
    "/balance-for-coins/{account-name}": {
      "get": {
        "tags": [
          "Balance-for-coins"
        ],
        "summary": "Historical balance change",
        "description": "History of change of `coin-type` balance in given block range\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_block(''blocktrades'');`\n\n* `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_block(''initminer'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/balance-for-coins/blocktrades''`\n\n* `GET ''https://%1$s/balance-api/balance-for-coins/initminer''`\n",
        "operationId": "btracker_endpoints.get_balance_for_coin_by_block",
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
              "type": "integer"
            },
            "description": "Coin types:\n\n* 13 - HBD\n\n* 21 - HIVE\n\n* 37 - VESTS\n"
          },
          {
            "in": "query",
            "name": "from-block",
            "required": true,
            "schema": {
              "type": "integer",
              "x-sql-datatype": "BIGINT"
            },
            "description": "Lower limit of the block range"
          },
          {
            "in": "query",
            "name": "to-block",
            "required": true,
            "schema": {
              "type": "integer",
              "x-sql-datatype": "BIGINT"
            },
            "description": "Upper limit of the block range"
          }
        ],
        "responses": {
          "200": {
            "description": "Balance change\n",
            "content": {
              "application/json": {
                "schema": {
                  "type": "string",
                  "x-sql-datatype": "JSONB"
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
    },
    "/balance-for-coins/{account-name}/by-time": {
      "get": {
        "tags": [
          "Balance-for-coins"
        ],
        "summary": "Historical balance change",
        "description": "History of change of `coin-type` balance in time range\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_time(''blocktrades'');`\n\n* `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_time(''initminer'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/balance-for-coins/blocktrades/by-time''`\n\n* `GET ''https://%1$s/balance-api/balance-for-coins/initminer/by-time''`\n",
        "operationId": "btracker_endpoints.get_balance_for_coin_by_time",
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
              "type": "integer"
            },
            "description": "Coin types:\n\n* 13 - HBD\n\n* 21 - HIVE\n\n* 37 - VESTS\n"
          },
          {
            "in": "query",
            "name": "start-date",
            "required": true,
            "schema": {
              "type": "string",
              "format": "date-time"
            },
            "description": "Lower limit of the time range"
          },
          {
            "in": "query",
            "name": "end-date",
            "required": true,
            "schema": {
              "type": "string",
              "format": "date-time"
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
                  "x-sql-datatype": "JSONB"
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
    },
    "/account-balances/{account-name}": {
      "get": {
        "tags": [
          "Account-balances"
        ],
        "summary": "Account balances",
        "description": "Lists account hbd, hive and vest balances\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_balances(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/account-balances/blocktrades''`\n",
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
                    "post_voting_power_vests": "8172549681941451"
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
    "/account-balances/{account-name}/delegations": {
      "get": {
        "tags": [
          "Account-balances"
        ],
        "summary": "Account delegations",
        "description": "Lists current account delegated and received VESTs\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_delegations(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/account-balances/blocktrades/delegations''`\n",
        "operationId": "btracker_endpoints.get_account_delegations",
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
            "description": "Account delegations\n(VEST balances are represented as string due to json limitations)\n\n* Returns `btracker_endpoints.delegations`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.delegations"
                },
                "example": [
                  {
                    "delegated_vests": "0",
                    "received_vests": "0"
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
    "/account-balances/{account-name}/savings": {
      "get": {
        "tags": [
          "Account-balances"
        ],
        "summary": "Account saving balances",
        "description": "Returns current hbd and hive savings\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_savings(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/account-balances/blocktrades/savings''`\n",
        "operationId": "btracker_endpoints.get_account_savings",
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
            "description": "Account saving balance\n(VEST balances are represented as string due to json limitations)\n\n* Returns `btracker_endpoints.savings`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.savings"
                },
                "example": [
                  {
                    "hbd_savings": 0,
                    "hive_savings": 0,
                    "savings_withdraw_requests": 0
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
    "/account-balances/{account-name}/withdrawals": {
      "get": {
        "tags": [
          "Account-balances"
        ],
        "summary": "Account withdrawals",
        "description": "Returns current vest withdrawals, amount withdrawn, amount left to withdraw and \nrate of a withdrawal \n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_withdraws(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/account-balances/blocktrades/withdrawals''`\n",
        "operationId": "btracker_endpoints.get_account_withdraws",
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
            "description": "Account withdrawals\n(VEST balances are represented as string due to json limitations)\n\n* Returns `btracker_endpoints.withdrawals`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.withdrawals"
                },
                "example": [
                  {
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
    "/account-balances/{account-name}/rewards": {
      "get": {
        "tags": [
          "Account-balances"
        ],
        "summary": "Account reward balances",
        "description": "Returns current, not yet claimed rewards obtained by posting and commenting\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_rewards(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/account-balances/blocktrades/rewards''`\n",
        "operationId": "btracker_endpoints.get_account_rewards",
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
            "description": "Account reward balance\n(VEST balances are represented as string due to json limitations)\n\n* Returns `btracker_endpoints.rewards`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.rewards"
                },
                "example": [
                  {
                    "hbd_rewards": 0,
                    "hive_rewards": 0,
                    "vests_rewards": "0",
                    "hive_vesting_rewards": 0
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
    "/account-balances/{account-name}/rewards/info": {
      "get": {
        "tags": [
          "Account-balances"
        ],
        "summary": "Account posting and curation rewards",
        "description": "Returns the sum of the received posting and curation rewards of the account since its creation\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_info_rewards(''blocktrades'');`\n\nREST call example\n* `GET ''https://%1$s/balance-api/account-balances/blocktrades/rewards/info''`\n",
        "operationId": "btracker_endpoints.get_account_info_rewards",
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
            "description": "Account posting and curation rewards\n(VEST balances are represented as string due to json limitations)\n\n* Returns `btracker_endpoints.info_rewards`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.info_rewards"
                },
                "example": [
                  {
                    "curation_rewards": "196115157",
                    "posting_rewards": "65916519"
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
    }
  },
  "components": {
    "schemas": {
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
          }
        }
      },
      "btracker_endpoints.delegations": {
        "type": "object",
        "properties": {
          "delegated_vests": {
            "type": "string",
            "description": "VESTS delegated to another user,  account''s power is lowered by delegated VESTS"
          },
          "received_vests": {
            "type": "string",
            "description": "VESTS received from another user,  account''s power is increased by received VESTS"
          }
        }
      },
      "btracker_endpoints.savings": {
        "type": "object",
        "properties": {
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
          }
        }
      },
      "btracker_endpoints.withdrawals": {
        "type": "object",
        "properties": {
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
      "btracker_endpoints.rewards": {
        "type": "object",
        "properties": {
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
          }
        }
      },
      "btracker_endpoints.info_rewards": {
        "type": "object",
        "properties": {
          "curation_rewards": {
            "type": "string",
            "description": "rewards obtained by posting and commenting expressed in VEST"
          },
          "posting_rewards": {
            "type": "string",
            "description": "curator''s reward expressed in VEST"
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
