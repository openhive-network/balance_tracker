// Stress test: push the API beyond normal load to find breaking points
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate } from 'k6/metrics';
import { BASE_URL, headers, TEST_DATA } from './config.js';

const errorRate = new Rate('errors');
const MAX_VUS = parseInt(__ENV.MAX_VUS || '50');

export const options = {
  stages: [
    { duration: '30s', target: Math.floor(MAX_VUS * 0.2) },
    { duration: '1m', target: Math.floor(MAX_VUS * 0.5) },
    { duration: '1m', target: MAX_VUS },
    { duration: '1m', target: MAX_VUS },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<5000'],
    errors: ['rate<0.10'],
  },
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

// Weighted endpoint selection simulating realistic traffic
const ENDPOINTS = [
  { weight: 25, fn: () => post('get_account_balances', { 'account-name': randomItem(TEST_DATA.accounts) }) },
  { weight: 20, fn: () => post('get_balance_history', {
    'account-name': randomItem(TEST_DATA.accounts),
    'coin-type': randomItem(TEST_DATA.coinTypes),
    'from-block': 0, 'to-block': TEST_DATA.toBlock }) },
  { weight: 15, fn: () => post('get_balance_delegations', { 'account-name': randomItem(TEST_DATA.accounts) }) },
  { weight: 10, fn: () => post('get_balance_aggregation', {
    'account-name': randomItem(TEST_DATA.accounts),
    'coin-type': randomItem(TEST_DATA.coinTypes),
    'granularity': randomItem(['year', 'month', 'day']) }) },
  { weight: 10, fn: () => post('get_transfer_statistics', {
    'account-name': randomItem(TEST_DATA.accounts),
    'granularity': randomItem(['year', 'month', 'day', 'hour']) }) },
  { weight: 10, fn: () => post('get_top_holders', { 'coin-type': randomItem(TEST_DATA.coinTypes) }) },
  { weight: 5, fn: () => post('get_recurrent_transfers', { 'account-name': randomItem(TEST_DATA.accounts) }) },
  { weight: 5, fn: () => post('get_btracker_version', {}) },
];

const WEIGHTED = [];
for (const ep of ENDPOINTS) {
  for (let i = 0; i < ep.weight; i++) WEIGHTED.push(ep.fn);
}

export default function () {
  const fn = WEIGHTED[Math.floor(Math.random() * WEIGHTED.length)];
  const res = fn();
  check(res, { 'not server error': (r) => r.status < 500 });
  errorRate.add(res.status >= 500);
  sleep(0.1 + Math.random() * 0.4);
}
