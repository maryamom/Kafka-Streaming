#  Part I — Kafka Streaming with CSV Data

This lab introduces **Kafka streaming using CSV data**.
CSV rows are streamed line by line and treated as individual Kafka messages.

Kafka does not interpret CSV structure — it only stores bytes.

---

##  Objectives

By the end of this lab, you should understand:

- How CSV data is streamed into Kafka
- How Kafka handles unstructured data
- How offsets and consumer groups work
- How partitions affect ordering and parallelism

---

##  Dataset

You are given a CSV file containing transaction data.

Tasks:
- Inspect the file
- Identify potential streaming issues
- Decide how each row maps to a Kafka message

---

## Tasks

### Task 1 — Kafka Setup
- Start Kafka using Docker
- Verify the broker is running

---

### Task 2 — Topic Design
- Design a topic for CSV streaming
- Choose:
  - Topic name
  - Number of partitions
- Justify your choices

---

### Task 3 — CSV Producer (Python)
- Implement a Python producer that:
  - Reads the CSV file
  - Sends **one row = one message**
  - Streams rows gradually (not all at once)

---

### Task 4 — CSV Consumer (Python)
- Implement a Python consumer that:
  - Reads messages from Kafka
  - Prints raw CSV rows
  - Tracks offsets

---

### Task 5 — Offsets & Replay
- Restart consumers
- Observe replay behavior
- Change consumer groups

---

### Task 6 — Consumer Groups
- Run multiple consumers in the same group
- Observe message distribution
- Increase partitions and repeat

---

### Task 7 — Ordering Guarantees
- Verify ordering within a partition
- Break ordering using multiple partitions
- Explain observed behavior

---

## Reflection Questions

- Why does Kafka not care about CSV structure?
- What problems arise from CSV in streaming systems?
- Why are offsets critical?

---

##  Deliverable
Submit:
- Your Python producer and consumer
- A short explanation of observations
