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
  - name: Matching-accounts
    description: Api searching for similarly named accounts
servers:
  - url: /%1$s
 */

DO $__$
DECLARE 
  __schema_name VARCHAR;
BEGIN
  SHOW SEARCH_PATH INTO __schema_name;

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
    },
    {
      "name": "Matching-accounts",
      "description": "Api searching for similarly named accounts"
    }
  ],
  "servers": [
    {
      "url": "/%1$s"
    }
  ],
  "paths": {
    "/matching-accounts/{partial-account-name}": {
      "get": {
        "tags": [
          "Matching-accounts"
        ],
        "summary": "Similar accounts",
        "description": "Returns list of similarly named accounts \n\nSQL example\n* `SELECT * FROM btracker_endpoints.find_matching_accounts(''blocktrade'');`\n\n* `SELECT * FROM btracker_endpoints.find_matching_accounts(''initmi'');`\n\nREST call example\n* `GET https://{btracker-host}/%1$s/matching-accounts/blocktrade`\n\n* `GET https://{btracker-host}/%1$s/matching-accounts/initmi`\n",
        "operationId": "btracker_endpoints.find_matching_accounts",
        "parameters": [
          {
            "in": "path",
            "name": "partial-account-name",
            "required": true,
            "schema": {
              "type": "string"
            },
            "description": "Name of the account"
          }
        ],
        "responses": {
          "200": {
            "description": "List of accounts\n",
            "content": {
              "application/json": {
                "schema": {
                  "type": "string"
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
    "/balance-for-coins/{account-name}": {
      "get": {
        "tags": [
          "Balance-for-coins"
        ],
        "summary": "Historical balance change",
        "description": "History of change of `coin-type` balance in given block range\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_block(''blocktrades'');`\n\n* `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_block(''initminer'');`\n\nREST call example\n* `GET https://{btracker-host}/%1$s/balance-for-coins/blocktrades`\n\n* `GET https://{btracker-host}/%1$s/balance-for-coins/initminer`\n",
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
        "description": "History of change of `coin-type` balance in time range\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_time(''blocktrades'');`\n\n* `SELECT * FROM btracker_endpoints.get_balance_for_coin_by_time(''initminer'');`\n\nREST call example\n* `GET https://{btracker-host}/%1$s/balance-for-coins/blocktrades/by-time`\n\n* `GET https://{btracker-host}/%1$s/balance-for-coins/initminer/by-time`\n",
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
        "description": "Lists account hbd, hive and vest balances\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_balances(''blocktrades'');`\n\n* `SELECT * FROM btracker_endpoints.get_account_balances(''initminer'');`\n\nREST call example\n* `GET https://{btracker-host}/%1$s/account-balances/blocktrades`\n\n* `GET https://{btracker-host}/%1$s/account-balances/initminer`\n",
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
            "description": "Account balances\n\n* Returns `btracker_endpoints.account_balance`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.account_balance"
                },
                "example": [
                  {
                    "hbd_balance": 1000,
                    "hive_balance": 1000,
                    "vesting_shares": 1000,
                    "vesting_balance_hive": 1000,
                    "post_voting_power_vests": 1000
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
        "description": "Lists current account delegated and received VESTs\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_delegations(''blocktrades'');`\n\n* `SELECT * FROM btracker_endpoints.get_account_delegations(''initminer'');`\n\nREST call example\n* `GET https://{btracker-host}/%1$s/account-balances/blocktrades/delegations`\n\n* `GET https://{btracker-host}/%1$s/account-balances/initminer/delegations`\n",
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
            "description": "Account delegations\n\n* Returns `btracker_endpoints.vests_balance`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.vests_balance"
                },
                "example": [
                  {
                    "delegated_vests": 1000,
                    "received_vests": 1000
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
        "description": "Returns the sum of the received posting and curation rewards of the account since its creation\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_info_rewards(''blocktrades'');`\n\n* `SELECT * FROM btracker_endpoints.get_account_info_rewards(''initminer'');`\n\nREST call example\n* `GET https://{btracker-host}/%1$s/account-balances/blocktrades/rewards/info`\n\n* `GET https://{btracker-host}/%1$s/account-balances/initminer/rewards/info`\n",
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
            "description": "Account posting and curation rewards\n\n* Returns `btracker_endpoints.account_info_rewards`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.account_info_rewards"
                },
                "example": [
                  {
                    "curation_rewards": 1000,
                    "posting_rewards": 1000
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
        "description": "Returns current, not yet claimed rewards obtained by posting and commenting\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_rewards(''blocktrades'');`\n\n* `SELECT * FROM btracker_endpoints.get_account_rewards(''initminer'');`\n\nREST call example\n* `GET https://{btracker-host}/%1$s/account-balances/blocktrades/rewards`\n\n* `GET https://{btracker-host}/%1$s/account-balances/initminer/rewards`\n",
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
            "description": "Account reward balance\n\n* Returns `btracker_endpoints.account_rewards`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.account_rewards"
                },
                "example": [
                  {
                    "hbd_rewards": 1000,
                    "hive_rewards": 1000,
                    "vests_rewards": 1000,
                    "hive_vesting_rewards": 1000
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
        "description": "Returns current hbd and hive savings\n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_savings(''blocktrades'');`\n\n* `SELECT * FROM btracker_endpoints.get_account_savings(''initminer'');`\n\nREST call example\n* `GET https://{btracker-host}/%1$s/account-balances/blocktrades/savings`\n\n* `GET https://{btracker-host}/%1$s/account-balances/initminer/savings`\n",
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
            "description": "Account saving balance\n\n* Returns `btracker_endpoints.account_savings`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.account_savings"
                },
                "example": [
                  {
                    "hbd_savings": 1000,
                    "hive_savings": 1000,
                    "savings_withdraw_requests": 1
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
        "description": "Returns current vest withdrawals, amount withdrawn, amount left to withdraw and \nrate of a withdrawal \n\nSQL example\n* `SELECT * FROM btracker_endpoints.get_account_withdraws(''blocktrades'');`\n\n* `SELECT * FROM btracker_endpoints.get_account_withdraws(''initminer'');`\n\nREST call example\n* `GET https://{btracker-host}/%1$s/account-balances/blocktrades/withdrawals`\n\n* `GET https://{btracker-host}/%1$s/account-balances/initminer/withdrawals`\n",
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
            "description": "Account withdrawals\n\n* Returns `btracker_endpoints.account_withdrawals`\n",
            "content": {
              "application/json": {
                "schema": {
                  "$ref": "#/components/schemas/btracker_endpoints.account_withdrawals"
                },
                "example": [
                  {
                    "vesting_withdraw_rate": 1000,
                    "to_withdraw": 1000,
                    "withdrawn": 1000,
                    "withdraw_routes": 1,
                    "delayed_vests": 1000
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
      "btracker_endpoints.account_balance": {
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
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "account''s VEST balance"
          },
          "vesting_balance_hive": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "the VEST balance, presented in HIVE,  is calculated based on the current HIVE price"
          },
          "post_voting_power_vests": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "account''s VEST balance - delegated VESTs + reveived VESTs, presented in HIVE,calculated based on the current HIVE price"
          }
        }
      },
      "btracker_endpoints.vests_balance": {
        "type": "object",
        "properties": {
          "delegated_vests": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "VESTS delegated to another user,  account''s power is lowered by delegated VESTS"
          },
          "received_vests": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "VESTS received from another user,  account''s power is increased by received VESTS"
          }
        }
      },
      "btracker_endpoints.account_info_rewards": {
        "type": "object",
        "properties": {
          "curation_rewards": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "rewards obtained by posting and commenting expressed in VEST"
          },
          "posting_rewards": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "curator''s reward expressed in VEST"
          }
        }
      },
      "btracker_endpoints.account_rewards": {
        "type": "object",
        "properties": {
          "hbd_rewards": {
            "type": "integer",
            "x-sql-datatype": "numeric",
            "description": "not yet claimed HIVE backed dollars  stored in hbd reward balance"
          },
          "hive_rewards": {
            "type": "integer",
            "x-sql-datatype": "numeric",
            "description": "not yet claimed HIVE  stored in hive reward balance"
          },
          "vests_rewards": {
            "type": "integer",
            "x-sql-datatype": "numeric",
            "description": "not yet claimed VESTS  stored in vest reward balance"
          },
          "hive_vesting_rewards": {
            "type": "integer",
            "x-sql-datatype": "numeric",
            "description": "the reward vesting balance, denominated in HIVE,  is determined by the prevailing HIVE price at the time of reward reception"
          }
        }
      },
      "btracker_endpoints.account_savings": {
        "type": "object",
        "properties": {
          "hbd_savings": {
            "type": "integer",
            "x-sql-datatype": "numeric",
            "description": "saving balance of HIVE backed dollars"
          },
          "hive_savings": {
            "type": "integer",
            "x-sql-datatype": "numeric",
            "description": "HIVE saving balance"
          },
          "savings_withdraw_requests": {
            "type": "integer",
            "description": "number representing how many payouts are pending  from user''s saving balance"
          }
        }
      },
      "btracker_endpoints.account_withdrawals": {
        "type": "object",
        "properties": {
          "vesting_withdraw_rate": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "received until the withdrawal is complete,  with each installment amounting to 1/13 of the withdrawn total"
          },
          "to_withdraw": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "the remaining total VESTS needed to complete withdrawals"
          },
          "withdrawn": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "the total VESTS already withdrawn from active withdrawals"
          },
          "withdraw_routes": {
            "type": "integer",
            "description": "list of account receiving the part of a withdrawal"
          },
          "delayed_vests": {
            "type": "integer",
            "x-sql-datatype": "BIGINT",
            "description": "blocked VESTS by a withdrawal"
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
,__schema_name);

END
$__$;

RESET ROLE;
