# Lab Observations & Deliverable

## Topic Design (Task 2 — CSV Lab)

- **Topic name:** `transactions-csv`
  - **Justification:** Describes the content (transaction data) and format (CSV). Clear and consistent with the JSON topic naming (`transactions-raw`, `transactions-filtered`).

- **Number of partitions:** 3
  - **Justification:** Allows multiple consumers in the same group to share the load (Task 6). Ordering is preserved per partition; using a key (e.g. `transaction_id`) keeps related messages in the same partition. Three partitions is a reasonable default for a small lab and can be increased to observe rebalancing.

---

## CSV Lab — Observations

- **Offsets & replay:** Restarting the consumer with the same group and `--from-beginning` replays all messages from the start. With `latest` (default), only new messages are read. Different consumer groups each maintain their own offsets, so each group can read the same topic independently.

- **Consumer groups:** With multiple consumers in the same group, partitions are assigned to consumers. Messages are distributed across partitions (and thus across consumers). If partitions ≥ consumers, each consumer gets at least one partition; if consumers > partitions, some consumers stay idle.

- **Ordering:** Within a single partition, order is preserved. With multiple partitions and a key, all messages with the same key go to the same partition, so per-key ordering is preserved. Without a key or with random keys, global order is not guaranteed.

---

## Reflection (CSV Lab)

- **Why does Kafka not care about CSV structure?**  
  Kafka stores messages as byte sequences. It does not parse or interpret payloads; structure (CSV, JSON, etc.) is the producer’s and consumer’s concern.

- **What problems arise from CSV in streaming systems?**  
  No embedded schema, so consumers must agree on column order and types; commas in fields need escaping; type errors and malformed lines are common and require explicit handling.

- **Why are offsets critical?**  
  Offsets let consumers track progress, support replay, and enable at-least-once processing and debugging by reprocessing from a given position.

---

## JSON Lab — Schema Decisions

- **Fields:** `transaction_id` (int), `user_id` (int), `amount` (float), `timestamp` (ISO string). Optional internal `_valid` (bool) for producer-side validation.

- **Topic derivation:**  
  - `transactions-raw`: all events (valid and invalid) for auditing and replay.  
  - `transactions-filtered`: only valid events for downstream pipelines.

- **Malformed records:** Producer validates types (int for ids, float for amount, date for timestamp). Invalid rows are still sent to `transactions-raw` with `_valid: false`; only valid events are sent to `transactions-filtered`. Consumer detects malformed JSON and invalid payloads and prints them for debugging.

---

## Reflection (JSON Lab)

- **Why is JSON better than CSV for streaming?**  
  Self-describing (field names in each message), easy to add/omit fields, standard parsing in every language, and nested structures when needed.

- **What problems does schema evolution introduce?**  
  Old consumers may break if required fields are removed or types change; new optional fields can be ignored by old consumers, but reordering or renaming fields can cause confusion without a schema registry or version field.

- **Why is Kafka often used as a source of truth?**  
  Durable, ordered log per partition; replayable from offsets; multiple consumers can read the same history; supports reprocessing and new derived topics (e.g. raw vs filtered) from one event log.
