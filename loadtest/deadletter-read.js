import http from "k6/http";
import { check } from "k6";

export const options = {
  vus: 10,
  duration: "20s",
  thresholds: {
    http_req_duration: ["p(95)<100"],
    http_req_failed: ["rate<0.01"],
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";

export default function () {
  const res = http.get(`${BASE_URL}/jobs/dead-letter`);
  check(res, {
    "status is 200": (r) => r.status === 200,
  });
}
