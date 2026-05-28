# Data Quality Agent 🛡️

**AI-driven data quality detection and remediation assistant powered by PySpark, Kafka, and LLM agents.**

> This project repository is created in partial fulfillment of the requirements for the Big Data Analytics course offered by the Master of Science in Business Analytics program at the Carlson School of Management, University of Minnesota.

---

## Overview

Poor data quality costs organizations an estimated **$12.9 million per year on average**, yet most data pipelines still rely on manual inspection and ad hoc fixes. **Data Quality Agent** is a real-time agentic AI system that:

1. **Detects** schema drifts, null anomalies, format inconsistencies, and statistical outliers in streaming data using Apache Spark + Kafka
2. **Diagnoses** root causes and business impact via a multi-agent LLM pipeline (GPT-4o-mini)
3. **Generates** ranked, runnable PySpark remediation code with confidence scores
4. **Learns** from human decisions via a RAG feedback loop (FAISS + sentence-transformers)
5. **Surfaces** everything through a human-in-the-loop Streamlit dashboard

---

## Architecture

```
Kafka Producer (CSV + anomaly injection)
        ↓
Kafka Topic: loan-stream
        ↓
Spark Structured Streaming (detector.py)
  ├── Detector 1: Null Spike
  ├── Detector 2: Statistical Outlier
  └── Detector 3: Format Inconsistency
        ↓ (new issues only)
Multi-Agent LLM Pipeline (suggester.py)
  ├── Router Agent     → routes by issue type
  ├── Agent 1          → root cause diagnosis
  ├── RAG Retrieval    → top-K similar past decisions (FAISS)
  ├── Agent 2          → PySpark remediation code generation
  └── Critic Agent     → validates output quality, retries if needed
        ↓
Streamlit Dashboard (app.py)
  └── Human reviews, approves, or declines suggestions
        ↓
historical_decisions.json  ← RAG feedback loop
```

---

## Team

**Team 6 — Carlson School of Management, University of Minnesota**

| Name | Role |
|---|---|
| Yung Hsuan Hsieh (Tom) | LLM Engineer |
| Sean Cabaniss | Dashboard Developer |
| Ching-Fen Hung | Data Engineer |
| Yonghui Kim | Integration & QA |
| Omkar Thombare | Business & Presentation |

---

## Dataset

- **LendingClub Loan Data** — 100K rows, 12 columns (local demo subset)
- Source: [Kaggle LendingClub Dataset](https://www.kaggle.com/datasets/wordsforthewise/lending-club)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka, Spark Structured Streaming |
| Detection | PySpark, IQR-based outlier detection, regex format checks |
| LLM Pipeline | OpenAI GPT-4o-mini, multi-agent pattern |
| RAG | FAISS, sentence-transformers (all-MiniLM-L6-v2) |
| Dashboard | Streamlit |
| Environment | Local (Mac) + Databricks Community Edition |

---

## Installation

```bash
# 1. Clone the repo
git clone https://github.com/tom666d/data-quality-agent.git
cd data-quality-agent

# 2. Install dependencies
pip install pyspark kafka-python openai python-dotenv faiss-cpu sentence-transformers streamlit pandas

# 3. Set up API key
cp .env.example .env
# Edit .env and add your OPENAI_API_KEY

# 4. Install and start Kafka (macOS)
brew install kafka
brew services start kafka

# 5. Create Kafka topic
kafka-topics --create --topic loan-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## Usage

Open three terminals, all from the project root:

**Terminal 1 — Start the detector:**
```bash
python src/detection/detector.py
```

**Terminal 2 — Start the producer (streams CSV + injects anomalies):**
```bash
python src/producer.py
```

**Terminal 3 — Launch the dashboard:**
```bash
streamlit run streamlit_app/app.py
```

---

## Project Structure

```
data-quality-agent/
├── src/
│   ├── detection/
│   │   └── detector.py          # Spark Structured Streaming detector
│   ├── llm/
│   │   └── suggester.py         # Multi-agent LLM + RAG pipeline
│   ├── streamlit_assets/        # UI images and icons
│   └── producer.py              # Kafka producer with anomaly injection
├── streamlit_app/
│   └── app.py                   # Streamlit human-review dashboard
├── data/
│   ├── demo_lendingclub.csv     # Local demo dataset
│   ├── issues_output.json       # Detector output
│   ├── issues_with_suggestions.json  # LLM suggestions
│   └── historical_decisions.json    # RAG feedback store
├── .env                         # API keys (not committed)
├── .gitignore
└── README.md
```

---

## Key Features

### Smart LLM Triggering
The detector tracks `(column, issue_type)` pairs seen in the current session. The LLM pipeline is only triggered when a **genuinely new** combination appears — avoiding redundant API calls on recurring issues.

### Multi-Agent Pipeline
- **Router Agent** — classifies issue type and selects remediation strategy
- **Agent 1** — diagnoses root cause and business impact
- **Agent 2** — generates 3 ranked PySpark remediation options with confidence scores
- **Critic Agent** — validates output format and retries if quality checks fail

### RAG Feedback Loop
After each human decision in the dashboard, the chosen action is stored in `historical_decisions.json`. On subsequent runs, FAISS retrieves the top-3 most similar past cases and injects them into Agent 2's prompt — making suggestions progressively more aligned with analyst preferences.

### Human-in-the-Loop
No fix is ever applied automatically. Every suggestion goes through the Streamlit dashboard where an analyst reviews, approves, or declines before any action is taken.

---

## Acknowledgements

- Carlson School of Management IT Support for cloud resource provisioning
- [LendingClub Dataset](https://www.kaggle.com/datasets/wordsforthewise/lending-club) via Kaggle
- OpenAI GPT-4o-mini API
- Apache Spark, Apache Kafka, FAISS, sentence-transformers
