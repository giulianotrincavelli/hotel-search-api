import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';

const baseUrl = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const hotelId = __ENV.RUN_ID;
if (!hotelId || hotelId.length > 50 || !/^[A-Za-z0-9_-]+$/.test(hotelId)) {
  throw new Error('RUN_ID es obligatorio: 1 a 50 caracteres alfanumericos, guion o guion bajo.');
}

const successfulPosts = new Counter('post_successful');
const failedPosts = new Counter('post_errors');
const payload = JSON.stringify({
  hotelId,
  checkIn: '29/12/2026',
  checkOut: '31/12/2026',
  ages: [30, 29, 1, 3],
});

export const options = {
  scenarios: {
    searches: {
      executor: 'shared-iterations',
      vus: 20,
      iterations: 10000,
      maxDuration: '30m',
    },
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
  thresholds: {
    iterations: ['count==10000'],
    post_successful: ['count==10000'],
    post_errors: ['count==0'],
  },
};

export function setup() {
  console.log(`Benchmark: hotelId=${hotelId}; 10000 POST; 20 VUs; destino=${baseUrl}`);
}

export default function () {
  const response = http.post(`${baseUrl}/search`, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '15s',
    redirects: 0,
  });

  let searchId;
  try {
    searchId = response.json('searchId');
  } catch (_) {
    // Una respuesta no JSON se contabiliza como error, sin abortar la carga.
  }
  const success = check(response, {
    'HTTP 200 y searchId UUID': (r) => r.status === 200
      && typeof searchId === 'string'
      && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(searchId),
  });
  successfulPosts.add(success ? 1 : 0);
  failedPosts.add(success ? 0 : 1);
}
