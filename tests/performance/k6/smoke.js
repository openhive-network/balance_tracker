// Smoke test: verify all endpoints respond under minimal load
import http from 'k6/http';
import { check, group, sleep } from 'k6';
import { BASE_URL, headers, TEST_DATA } from './config.js';

export const options = {
  vus: 1,
  iterations: 1,
  thresholds: {
    http_req_failed: ['rate==0'],
    http_req_duration: ['p(95)<5000'],
  },
};

function post(endpoint, params) {
  const body = Object.entries(params)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');
  return http.post(`${BASE_URL}/rpc/${endpoint}`, body || null,
    Object.keys(params).length > 0 ? { headers } : {});
}

export default function () {
  group('version and status', () => {
    let res = post('get_btracker_version', {});
    check(res, { 'version returns 200': (r) => r.status === 200 });
    sleep(0.1);

    res = post('get_btracker_last_synced_block', {});
    check(res, { 'last synced block returns 200': (r) => r.status === 200 });
    sleep(0.1);

    res = post('get_total_value_locked', {});
    check(res, { 'total value locked returns 200': (r) => r.status === 200 });
  });

  group('balance history by block', () => {
    for (const coin of TEST_DATA.coinTypes) {
      const res = post('get_balance_history', {
        'account-name': 'dantheman',
        'coin-type': coin,
        'from-block': 0,
        'to-block': TEST_DATA.toBlock,
      });
      check(res, { [`balance history ${coin} returns 200`]: (r) => r.status === 200 });
      sleep(0.1);
    }
  });

  group('balance and delegations', () => {
    let res = post('get_account_balances', { 'account-name': 'dantheman' });
    check(res, { 'get_balance returns 200': (r) => r.status === 200 });
    sleep(0.1);

    res = post('get_balance_delegations', { 'account-name': 'dantheman' });
    check(res, { 'get_delegations returns 200': (r) => r.status === 200 });
    sleep(0.1);

    res = post('get_recurrent_transfers', { 'account-name': 'dantheman' });
    check(res, { 'recurrent transfers returns 200': (r) => r.status === 200 });
  });

  group('aggregation', () => {
    for (const granularity of ['year', 'month', 'day']) {
      const res = post('get_balance_aggregation', {
        'account-name': 'dantheman',
        'coin-type': 'HIVE',
        'granularity': granularity,
      });
      check(res, { [`aggregation ${granularity} returns 200`]: (r) => r.status === 200 });
      sleep(0.1);
    }
  });

  group('transfer statistics', () => {
    for (const granularity of ['year', 'month', 'day', 'hour']) {
      const res = post('get_transfer_statistics', {
        'account-name': 'dantheman',
        'granularity': granularity,
      });
      check(res, { [`transfer stats ${granularity} returns 200`]: (r) => r.status === 200 });
      sleep(0.1);
    }
  });

  group('top holders', () => {
    for (const coin of TEST_DATA.coinTypes) {
      const res = post('get_top_holders', { 'coin-type': coin });
      check(res, { [`top holders ${coin} returns 200`]: (r) => r.status === 200 });
      sleep(0.1);
    }
  });
}
