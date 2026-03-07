// Load test: sustained traffic across all endpoints
import http from 'k6/http';
import { check, group, sleep } from 'k6';
import { BASE_URL, DEFAULT_THRESHOLDS, headers, TEST_DATA } from './config.js';

const DURATION = __ENV.DURATION || '2m';
const VUS = parseInt(__ENV.VUS || '10');

export const options = {
  stages: [
    { duration: '30s', target: VUS },
    { duration: DURATION, target: VUS },
    { duration: '15s', target: 0 },
  ],
  thresholds: DEFAULT_THRESHOLDS,
};

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function post(endpoint, params) {
  const body = Object.entries(params)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&');
  return http.post(`${BASE_URL}/rpc/${endpoint}`, body || null,
    Object.keys(params).length > 0 ? { headers } : {});
}

export default function () {
  const account = randomItem(TEST_DATA.accounts);
  const coin = randomItem(TEST_DATA.coinTypes);

  group('balance', () => {
    const res = post('get_account_balances', { 'account-name': account });
    check(res, { 'status 200': (r) => r.status === 200 });
  });

  group('balance history', () => {
    post('get_balance_history', {
      'account-name': account,
      'coin-type': coin,
      'from-block': 0,
      'to-block': TEST_DATA.toBlock,
    });
  });

  group('delegations', () => {
    post('get_balance_delegations', { 'account-name': account });
  });

  group('aggregation', () => {
    const granularity = randomItem(['year', 'month', 'day']);
    post('get_balance_aggregation', {
      'account-name': account,
      'coin-type': coin,
      'granularity': granularity,
    });
  });

  group('transfer stats', () => {
    const granularity = randomItem(['year', 'month', 'day', 'hour']);
    post('get_transfer_statistics', {
      'account-name': account,
      'granularity': granularity,
    });
  });

  group('top holders', () => {
    post('get_top_holders', { 'coin-type': coin });
  });

  sleep(0.5);
}
