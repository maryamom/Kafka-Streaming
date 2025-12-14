# Kafka Streaming — Core Hands-On Lab

This repository contains the **core hands-on Kafka streaming lab** of the course.

The lab is designed to be completed in **two phases**:

1. **Phase 1 — Clean Data**  
   You will first work with well-structured, clean data to understand Kafka streaming fundamentals.

2. **Phase 2 — Dirty Data**  
   You will then repeat and adapt your pipeline using imperfect data, reflecting real-world streaming conditions.

This two-phase approach allows you to first focus on **how Kafka works**, and then on **how Kafka pipelines handle data quality issues**.

---

## Lab Structure

Each phase is split into **two progressive parts**, increasing in complexity.

---

### 🔹 Part I — CSV Streaming  
 `csv_lab/README_CSV.md`

You will:
- Stream CSV data into Kafka
- Understand topics, partitions, offsets, and consumer groups
- Work with untyped, line-based data
- Build a baseline Kafka streaming pipeline

This part is completed first with **clean CSV data**, then revisited using **dirty CSV data**.

---

### 🔹 Part II — JSON Streaming  
 `json_lab/README_JSON.md`

You will:
- Convert CSV records into JSON events
- Stream structured JSON messages
- Design and evolve event schemas
- Adapt your pipeline to handle data quality issues

This part builds directly on Part I and represents a more realistic streaming scenario.

---

## How to Approach This Lab

- Complete **Phase 1 (clean data)** before moving to **Phase 2 (dirty data)**
- Reuse your existing pipeline and adapt it rather than rewriting it
- Focus on understanding how data quality impacts streaming systems
- Expect the second phase to be more challenging

---

##  Requirements

- Docker
- Python 3.9+
- Kafka running via Docker
