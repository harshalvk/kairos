import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  scenarios: {
    steady_load: {
      executor: "constant-arrival-rate",
      rate: 50, // 50 requests per second
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 20,
      maxVUs: 100,
    },
  },
  thresholds: {
    http_req_duration: ["p(95)<200"], // 95% of requests under 200ms
    http_req_failed: ["rate<0.01"], // less than 1% failure rate
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080/v1";

export default function () {
  const payload = JSON.stringify({
    type: "send_email",
    payload: { to: "loadtest@example.com" },
    max_attempts: 3,
  });

  const res = http.post(`${BASE_URL}/jobs`, payload, {
    headers: { "Content-Type": "application/json" },
  });

  check(res, {
    "status is 201": (r) => r.status === 201,
    "has job_id": (r) => JSON.parse(r.body).job_id !== undefined,
  });

  sleep(0.01);
}
