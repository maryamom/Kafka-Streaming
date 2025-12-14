#  Part II — Kafka Streaming with JSON Data

This lab builds on the CSV lab and introduces **structured JSON events**.

JSON is self-describing and closer to real-world streaming systems.

---

## Objectives

By the end of this lab, you should be able to:

- Stream structured JSON events
- Handle schema changes
- Design Kafka pipelines for real applications

---

## Tasks

### Task 1 — From CSV to JSON
- Convert CSV rows into JSON objects
- Decide field names and data types

---

### Task 2 — JSON Producer (Python)
- Implement a producer that:
  - Sends JSON messages
  - Ensures valid serialization
  - Handles malformed records

---

### Task 3 — JSON Consumer (Python)
- Parse JSON messages
- Extract fields
- Detect malformed events

---

### Task 4 — Schema Evolution
- Modify JSON structure
- Add and remove fields
- Observe consumer behavior

---

### Task 5 — Topic Derivation
- Create multiple topics:
  - Raw events
  - Filtered events
- Route messages accordingly

---

### Task 6 — Replay & Debugging
- Replay historical JSON data
- Debug incorrect events
- Use Kafka as an event log

---

##  Reflection Questions

- Why is JSON better than CSV for streaming?
- What problems does schema evolution introduce?
- Why is Kafka often used as a source of truth?

---

##  Deliverable
Submit:
- Updated producers and consumers
- Explanation of schema decisions
