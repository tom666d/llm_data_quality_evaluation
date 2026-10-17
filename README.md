# DataGenie — Streaming

**Real-time data quality detection with LLM-generated remediation, built on Kafka, Spark Structured Streaming, and GPT-4o-mini.**

> Streaming extension of *DataGenie*, a Team 6 project for the Big Data Analytics course in the MSBA program at the Carlson School of Management, University of Minnesota.

---

## What it does today

1. **Detect.** Spark Structured Streaming reads loan records from Kafka and checks every 10-second micro-batch for null spikes, statistical outliers, and mixed date formats.
2. **Gather evidence.** For each new `(column, issue_type)` pair, the detector computes observations from the batch, as counts and rates only, with no conclusions:
   - for null values, how other columns are filled in the affected rows and which columns equal the null column;
   - for outliers, how concentrated the outlier values are;
   - for dates, the range of each date format.
3. **Diagnose.** Agent 1 (GPT-4o-mini) states a root-cause hypothesis and the business impact, and names the observations that support it.
4. **Remediate.** Agent 2 writes two remediation options with PySpark code. It is told to set unrecoverable values to null rather than invent them. A static critic checks that each snippet parses and assigns `df`, sends feedback for a retry (up to 2 times), and records the final pass/fail with the suggestions.

Recurring issues are logged every batch, but the LLM runs only once per unique `(column, issue_type)` pair in a session.

---

## Architecture

```
producer.py ── CSV rows (+ injected anomalies) ──▶ Kafka topic: loan-stream
                                                        │
                                                        ▼
detector.py  (Spark Structured Streaming, 10s micro-batches)
  ├── Null Spike            null rate > 30%
  ├── Statistical Outlier   > 5% of rows outside 1.5×IQR
  └── Format Inconsistency  issue_d mixes MMM-YYYY and MM/YYYY
        │ every batch ──▶ data/issues_output.json, data/quality_score.json
        │ new issues only, with batch evidence attached
        ▼
suggester.py
  ├── Agent 1   root cause + business impact, grounded in the evidence
  ├── Agent 2   2 remediation options with PySpark code
  └── Critic    static check (parses, assigns df), retry with feedback
        ▼
data/issues_with_suggestions.json
```

---

## Quick start

Requires Python 3.10+, Java 17, and PySpark 4.0 (the detector pulls `spark-sql-kafka-0-10_2.13:4.0.0`).

```bash
pip install pyspark kafka-python openai python-dotenv pandas

# OpenAI key
echo "OPENAI_API_KEY=sk-..." > .env

# Kafka (macOS)
brew install kafka
brew services start kafka
kafka-topics --create --topic loan-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

The producer replays a LendingClub CSV from `data/demo_lendingclub.csv` (not committed; source: [Kaggle LendingClub dataset](https://www.kaggle.com/datasets/wordsforthewise/lending-club)).

Run both from the project root:

```bash
python src/detection/detector.py   # terminal 1
python src/producer.py             # terminal 2
```

---

## Project structure

```
src/
├── producer.py            # Kafka producer: replays the CSV, injects anomalies
├── detection/detector.py  # Spark Structured Streaming detector
└── llm/suggester.py       # Agent 1 → Agent 2 → Critic
data/                      # runtime outputs (git-ignored)
```

---

## Roadmap

The batch version of DataGenie also had routing, human review, and a feedback loop. These are being rebuilt for streaming:

- [ ] **Approve.** Live Streamlit review dashboard with a per-batch quality trend
- [ ] **Learn.** Approved decisions logged and reused for the same issue later
- [ ] **Route.** Issue-type strategy passed into Agent 2's prompt
- [ ] **Validate.** Critic checks that the generated code actually parses and runs
- [ ] Non-blocking LLM calls, so the stream doesn't stall while agents run
- [ ] Clean source data with time-windowed "incident" anomaly injection

---

## Team

**Team 6, Carlson School of Management, University of Minnesota**

| Name | Role |
|---|---|
| Yung Hsuan Hsieh (Tom) | LLM Engineer |
| Sean Cabaniss | Dashboard Developer |
| Ching-Fen Hung | Data Engineer |
| Yonghui Kim | Integration & QA |
| Omkar Thombare | Business & Presentation |
