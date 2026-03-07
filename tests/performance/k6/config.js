// Shared configuration for k6 performance tests

export const BASE_URL = __ENV.BASE_URL || 'http://localhost:3000';

export const DEFAULT_THRESHOLDS = {
  http_req_duration: ['p(95)<2000', 'p(99)<5000'],
  http_req_failed: ['rate<0.01'],
};

export const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };

// Test data matching the 5M block CI dataset
export const TEST_DATA = {
  accounts: ['dantheman', 'ned', 'blocktrades', 'steemit', 'smooth'],
  coinTypes: ['HIVE', 'HBD', 'VESTS'],
  fromBlock: 0,
  toBlock: 5000000,
};
