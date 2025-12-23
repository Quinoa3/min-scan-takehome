# Mini-Scan

Hello!

As you've heard by now, Censys scans the internet at an incredible scale. Processing the results necessitates scaling horizontally across thousands of machines. One key aspect of our architecture is the use of distributed queues to pass data between machines.

---

The `docker-compose.yml` file sets up a toy example of a scanner. It spins up a Google Pub/Sub emulator, creates a topic and subscription, and publishes scan results to the topic. It can be run via `docker compose up`.

Your job is to build the data processing side. It should:

1. Pull scan results from the subscription `scan-sub`.
2. Maintain an up-to-date record of each unique `(ip, port, service)`. This should contain when the service was last scanned and a string containing the service's response.

> **_NOTE_**
> The scanner can publish data in two formats, shown below. In both of the following examples, the service response should be stored as: `"hello world"`.
>
> ```javascript
> {
>   // ...
>   "data_version": 1,
>   "data": {
>     "response_bytes_utf8": "aGVsbG8gd29ybGQ="
>   }
> }
>
> {
>   // ...
>   "data_version": 2,
>   "data": {
>     "response_str": "hello world"
>   }
> }
> ```

Your processing application should be able to be scaled horizontally, but this isn't something you need to actually do. The processing application should use `at-least-once` semantics where ever applicable.

You may write this in any languages you choose, but Go would be preferred.

You may use any data store of your choosing, with `sqlite` being one example. Like our own code, we expect the code structure to make it easy to switch data stores.

Please note that Google Pub/Sub is best effort ordering and we want to keep the latest scan. While the example scanner does not publish scans at a rate where this would be an issue, we expect the application to be able to handle extreme out of orderness. Consider what would happen if the application received a scan that is 24 hours old.

cmd/scanner/main.go should not be modified

---

Please upload the code to a publicly accessible GitHub, GitLab or other public code repository account. This README file should be updated, briefly documenting your solution. Like our own code, we expect testing instructions: whether it’s an automated test framework, or simple manual steps.

To help set expectations, we believe you should aim to take no more than 4 hours on this task.

We understand that you have other responsibilities, so if you think you’ll need more than 5 business days, just let us know when you expect to send a reply.

Please don’t hesitate to ask any follow-up questions for clarification.

---

## Solution Overview

Added a new `processor` service that consumes the `scan-sub` subscription from the Pub/Sub emulator and stores the latest scan per `(ip, port, service)` in Postgres. The processor normalizes both data formats, applies at-least-once semantics, and rejects out-of-order writes via a timestamp-aware `UPSERT`.



Key pieces:
- `pkg/processing`: Parses incoming messages and normalizes the response string for data versions 1 and 2. Also contains logic to handle malformed messages and publish them to a DLQ topic. Finally calls the storage layer to persist the data is no issues.
- `pkg/storage`: Repository interface plus Postgres implementation using a single upsert.
- `cmd/processor`: Subscriber that acknowledges messages only after a successful upsert or successful publishing malformed message to dql; 
- `docker-compose.yml`: Adds Postgres and wires the processor into the existing scanner/emulator stack; creates a `scan-dlq` topic.

## Running locally

1) Start everything:
```bash
docker compose up --build
```
This launches the Pub/Sub emulator, topic/subscription, scanner, Postgres, and the new processor.

2) Inspect stored scans (example):
```bash
docker compose exec postgres psql -U postgres -d scans -c "select ip, port, service, last_scanned, response from scans limit 5;"
```

Environment defaults used by the processor (override as needed):
- `PUBSUB_PROJECT_ID` (default: `test-project`)
- `PUBSUB_SUBSCRIPTION_ID` (default: `scan-sub`)
- `PUBSUB_DLQ_TOPIC` (default: ``) no value disables dlq publishing of malformed messages just acks them from the original topic. 
- `DATABASE_URL` (default: `postgres://postgres:postgres@postgres:5432/scans?sslmode=disable`)
- `PROCESSOR_WORKERS` (default: number of CPUs)
- `PROCESSOR_MAX_OUTSTANDING` (default: `200`)

## Testing

Unit test for message parsing/normalization and handler/DLQ tests:
```bash
go test ./pkg/processing
```
(Run `go mod tidy` first if dependencies are missing.)

Integration test for Postgres upsert behavior (requires a running Postgres reachable at `TEST_DATABASE_URL` or `postgres://postgres:postgres@localhost:5433/scans_test?sslmode=disable`):
```bash
go test ./pkg/storage/postgres
```

To spin up a local Postgres dedicated for tests, you can use the provided docker-compose.test file:
```bash
docker compose -f docker-compose.test.yml up -d
go test ./pkg/storage/postgres
docker compose -f docker-compose.test.yml down
```

## Notes on ordering & durability

- Upserts use `WHERE EXCLUDED.last_scanned >= scans.last_scanned` to drop older/out-of-order messages while keeping at-least-once safety.
- Pub/Sub subscriber runs with multiple goroutines and acknowledges only after the database write succeeds.

## Other Notes
- Go version was updated to 1.23 this happened due to dependencies pulled in for the google pub sub client. Tried to figure out which version of the google pub sub client would allow version to remain 1.20 but was eaiser just to update the docker image versions to 1.23 to match. 
- Malformed messages will be pushed to a dlq and acked in the original topic. These messages cannot be retired without fixing the message or changes to the code so nacking them doesn't make sense. If they can't be pused to the dql (timeout or network issues) then the message is nacked on the original topic. 
- You can disable dlq publishing by not setting the PUBSUB_DLQ_TOPIC. Messages that are malformed will simply be acked if dlq publishing is disabled. 
- Congifuration can be added to google pub sub for number of retries after a nack and can auto push to another dql if to many retries fail. (not configured on the emulator)
