import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

const baseUrl = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const hotelId = __ENV.RUN_ID;
const vus = Number(__ENV.VUS || 20);
const iterations = Number(__ENV.ITERATIONS || 10000);
const measureVisibility = __ENV.MEASURE_VISIBILITY === 'true';
const visibilityTimeoutMs = 30000;
if (!Number.isInteger(vus) || vus < 1 || !Number.isInteger(iterations) || iterations < 1) {
  throw new Error('VUS and ITERATIONS must be positive integers.');
}
if (!hotelId || hotelId.length > 50 || !/^[A-Za-z0-9_-]+$/.test(hotelId)) {
  throw new Error('RUN_ID is required: 1–50 alphanumeric characters, hyphens, or underscores.');
}

const successfulPosts = new Counter('post_successful');
const failedPosts = new Counter('post_errors');
const postLatency = new Trend('post_latency', true);
const getLatency = new Trend('get_latency', true);
const readyGetLatency = new Trend('get_ready_latency', true);
const pendingGetLatency = new Trend('get_pending_latency', true);
const persistenceWait = new Trend('persistence_visibility_wait', true);
const visibilityErrors = new Counter('visibility_errors');
const notReady = new Counter('get_not_ready');
const search = {
  hotelId,
  checkIn: '29/12/2026',
  checkOut: '31/12/2026',
  ages: [30, 29, 1, 3],
};
const payload = JSON.stringify(search);
const expectedAges = JSON.stringify(search.ages);

export const options = {
  scenarios: {
    searches: {
      executor: 'shared-iterations',
      vus,
      iterations,
      maxDuration: '30m',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
  thresholds: {
    iterations: [`count==${iterations}`],
    post_successful: [`count==${iterations}`],
    post_errors: ['count==0'],
    ...(measureVisibility ? { visibility_errors: ['count==0'] } : {}),
  },
};

export function setup() {
  console.log(`Benchmark: hotelId=${hotelId}; ${iterations} POST; ${vus} VUs; target=${baseUrl}`);
}

export default function () {
  const response = http.post(`${baseUrl}/search`, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '15s',
    redirects: 0,
    tags: { name: 'POST /search' },
  });

  postLatency.add(response.timings.duration);
  let searchId;
  try {
    searchId = response.json('searchId');
  } catch (_) {
    // Non-JSON responses count as errors.
  }
  const success = check(response, {
    'HTTP 202 and UUID searchId': (r) => r.status === 202
      && typeof searchId === 'string'
      && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(searchId),
  });
  successfulPosts.add(success ? 1 : 0);
  failedPosts.add(success ? 0 : 1);
  if (measureVisibility) {
    if (success) {
      waitForPersistence(searchId);
    } else {
      visibilityErrors.add(1);
    }
  }
}

function waitForPersistence(searchId) {
  const started = Date.now();
  const deadline = started + visibilityTimeoutMs;
  while (Date.now() < deadline) {
    const response = http.get(`${baseUrl}/count?searchId=${encodeURIComponent(searchId)}`, {
      timeout: `${Math.max(1, deadline - Date.now())}ms`,
      redirects: 0,
      responseCallback: http.expectedStatuses(200, 404),
      tags: { name: 'GET /count' },
    });
    getLatency.add(response.timings.duration);
    if (response.status === 200) {
      readyGetLatency.add(response.timings.duration);
      let valid = false;
      try {
        const body = response.json();
        valid = body.searchId === searchId && body.count >= 1
          && body.search.hotelId === hotelId
          && body.search.checkIn === search.checkIn && body.search.checkOut === search.checkOut
          && JSON.stringify(body.search.ages) === expectedAges;
      } catch (_) {
        valid = false;
      }
      check(response, { 'Persisted search matches submitted payload': () => valid });
      visibilityErrors.add(valid ? 0 : 1);
      if (valid) persistenceWait.add(Date.now() - started);
      return;
    }
    if (response.status !== 404) {
      visibilityErrors.add(1);
      return;
    }
    pendingGetLatency.add(response.timings.duration);
    notReady.add(1);
    sleep(Math.min(0.5, Math.max(0, deadline - Date.now()) / 1000));
  }
  visibilityErrors.add(1);
}
