# Hotel Search API

Records hotel searches and counts exact matches by hotel, dates, and ages, including age order and duplicates. Each POST generates a unique `searchId` and publishes to Kafka. A consumer persists the search in Oracle asynchronously, so GET may not find it immediately.

Java 25 · Spring Boot 4.1.1 · Spring Kafka · Apache Kafka 4.2.2 · Oracle Database 23ai Free · Maven

## Local execution

Requires Docker Desktop or Docker Engine with Compose v2 and Linux containers. Java and Maven are not required on the host; Docker builds the application. Run from the repository root. No `.env` file is needed.

```sh
docker compose build app
docker compose up -d --wait --wait-timeout 900
docker compose ps
```

Services: `app` (8080), `kafka` (29092), and `oracle` (1521). The app waits for Oracle and Kafka health checks, then may take a few seconds to serve HTTP.

- Swagger UI: http://localhost:8080/swagger-ui/index.html
- OpenAPI JSON: http://localhost:8080/v3/api-docs

```sh
docker compose logs --tail=100 app
docker compose down
```

Stopping retains Oracle data. Container names are fixed; stop an earlier copy from its directory before starting another.

## API

### POST /search

```sh
curl --silent --show-error -i \
  -H 'Content-Type: application/json' \
  -d '{"hotelId":"1234aBc","checkIn":"29/12/2026","checkOut":"31/12/2026","ages":[30,29,1,3]}' \
  http://localhost:8080/search
```

HTTP 202 Accepted after Kafka confirms publication. Example response:

```json
{"searchId":"550e8400-e29b-41d4-a716-446655440000"}
```

### GET /count?searchId=...

Replace the example ID with the one returned by POST.

```sh
SEARCH_ID='550e8400-e29b-41d4-a716-446655440000'
curl --silent --show-error -i "http://localhost:8080/count?searchId=$SEARCH_ID"
```

HTTP 200 once persisted; this example has one matching search:

```json
{
  "searchId": "550e8400-e29b-41d4-a716-446655440000",
  "search": {"hotelId":"1234aBc","checkIn":"29/12/2026","checkOut":"31/12/2026","ages":[30,29,1,3]},
  "count": 1
}
```

GET returns 404 for an unknown or pending search. Retry briefly after POST. Two identical POST requests produce different IDs and count as two matches once persisted.

Invalid requests return 400, Kafka infrastructure failures 503, and internal errors 500.

## Tests

With Java 25 and Maven:

```sh
# Fast: unit tests, MockMvc, and ArchUnit
mvn test '-Dtest=*Test,!*IntegrationTest'

# Full: Kafka, Oracle/Testcontainers, concurrency, and coverage; requires Docker
mvn clean verify -Dsurefire.excludedGroups=
```

Without the override, `mvn clean verify` excludes `oracle-integration` but still runs embedded Kafka tests.

Run fast tests with Docker:

```sh
docker run --rm -v "$PWD:$PWD" -w "$PWD" \
  maven:3-eclipse-temurin-25-noble \
  mvn test '-Dtest=*Test,!*IntegrationTest'
```

See [full-suite Docker commands](docs/TECHNICAL.md#tests-and-coverage) and [technical documentation](docs/TECHNICAL.md) for architecture, persistence, logging, and benchmark results.

## Performance

With the API running, execute 10,000 complete POST/GET iterations with 200 VUs:

```sh
RUN_ID="k6-$(date -u +%Y%m%d%H%M%S)-$$"
docker run --rm -i \
  --network "container:$(docker compose ps -q app)" \
  -e BASE_URL=http://localhost:8080 -e RUN_ID="$RUN_ID" \
  -e VUS=200 -e ITERATIONS=10000 -e MEASURE_VISIBILITY=true \
  grafana/k6:1.0.0 run - < scripts/search-load.js
```

GET polling waits up to 500 ms after each 404. Use a fresh `RUN_ID`; the payload dates must not be in the past. See [benchmark results and Oracle verification](docs/TECHNICAL.md#benchmark-and-monitoring).

Actuator exposes health and metrics at `/actuator/health` and `/actuator/metrics`.
