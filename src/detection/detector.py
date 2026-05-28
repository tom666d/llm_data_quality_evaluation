"""
detector.py
Spark Structured Streaming version of the original batch detector.

Key behaviors:
  1. Known issues tracking — tracks (column, issue_type) pairs seen this session.
     LLM is only triggered when a genuinely NEW combination appears.
  2. Auto LLM trigger — passes only the new issues to run_suggester(),
     so the LLM processes exactly the new findings rather than the full history.

Usage:
    Local      : python detector.py
    Databricks : call run_streaming_detector() from a Job or Notebook
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType,
)

# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BROKER      = "localhost:9092"
TOPIC_NAME        = "loan-stream"
TRIGGER_SEC       = 10
NULL_THRESHOLD    = 0.30
OUTLIER_THRESHOLD = 0.05
MAX_ISSUES_STORED = 50

NULL_COLS    = ["loan_amnt", "funded_amnt", "int_rate", "grade",
                "emp_length", "annual_inc", "loan_status", "purpose"]
NUMERIC_COLS = ["loan_amnt", "funded_amnt", "int_rate", "annual_inc"]

# ── Known issues registry ─────────────────────────────────────────────────────
# Stores (column, issue_type) tuples seen during this streaming session.
# LLM is only triggered when a NEW combination appears.
KNOWN_ISSUES: set[tuple[str, str]] = set()

# ── Schema ────────────────────────────────────────────────────────────────────
LOAN_SCHEMA = StructType([
    StructField("loan_amnt",       DoubleType(), True),
    StructField("funded_amnt",     DoubleType(), True),
    StructField("funded_amnt_inv", DoubleType(), True),
    StructField("int_rate",        DoubleType(), True),
    StructField("grade",           StringType(), True),
    StructField("emp_length",      StringType(), True),
    StructField("annual_inc",      DoubleType(), True),
    StructField("loan_status",     StringType(), True),
    StructField("purpose",         StringType(), True),
    StructField("issue_d",         StringType(), True),
    StructField("revol_util",      DoubleType(), True),
    StructField("tot_cur_bal",     DoubleType(), True),
])


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_output_dir() -> Path:
    if os.path.exists("/dbfs"):
        return Path("/Volumes/workspace/team6/data")
    return Path("data")


def get_project_root() -> Path:
    # detector.py is at src/detection/, so go up 3 levels to reach project root
    return Path(__file__).resolve().parent.parent.parent


# ── Auto-trigger LLM (new issues only) ───────────────────────────────────────
def trigger_suggester(new_issues: list):
    """
    Calls run_suggester() with only the newly detected issues.

    By passing issues_override, the suggester skips reading the full
    issues_output.json and processes only the new findings — avoiding
    redundant LLM calls on issues that were already handled.
    """
    project_root = get_project_root()

    llm_dir = project_root / "src" / "llm"
    if str(llm_dir) not in sys.path:
        sys.path.insert(0, str(llm_dir))

    try:
        from suggester import run_suggester
        print(f"  🤖 Triggering LLM for {len(new_issues)} new issue(s) ...")
        run_suggester(issues_override=new_issues)
        print("  ✅ LLM suggestions generated and appended")
    except Exception as e:
        print(f"  ⚠️  Suggester failed (non-fatal): {e}")


# ── Detection logic ───────────────────────────────────────────────────────────
def detect_issues_in_batch(batch_df: DataFrame, batch_id: int):
    global KNOWN_ISSUES

    total = batch_df.count()
    if total == 0:
        print(f"[Batch {batch_id}] Empty batch — skipping")
        return

    print(f"\n[Batch {batch_id}] Received {total} rows — running detectors ...")
    issues     = []   # all issues found this batch
    new_issues = []   # only the ones not seen before
    timestamp  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ── Detector 1: Null Spike ────────────────────────────────────────────────
    for col_name in NULL_COLS:
        if col_name not in batch_df.columns:
            continue

        null_count = batch_df.filter(F.col(col_name).isNull()).count()
        null_rate  = null_count / total

        if null_rate > NULL_THRESHOLD:
            sample = (
                batch_df.select(col_name).dropna()
                .limit(3).toPandas()[col_name].tolist()
            )
            issue = {
                "batch_id":      batch_id,
                "timestamp":     timestamp,
                "column":        col_name,
                "issue_type":    "Null Spike",
                "severity":      "HIGH",
                "detail":        f"Null rate: {null_rate:.0%} ({null_count}/{total} rows)",
                "sample_values": str(sample),
            }
            issues.append(issue)

            key = (col_name, "Null Spike")
            if key not in KNOWN_ISSUES:
                new_issues.append(issue)
                KNOWN_ISSUES.add(key)
                print(f"  🆕 NEW  Null Spike       | {col_name} | null rate = {null_rate:.0%}")
            else:
                print(f"  ♻️  SEEN Null Spike       | {col_name} | null rate = {null_rate:.0%} — LLM skipped")

    # ── Detector 2: Statistical Outlier ───────────────────────────────────────
    for col_name in NUMERIC_COLS:
        if col_name not in batch_df.columns:
            continue

        quantiles = (
            batch_df.select(col_name).dropna()
            .approxQuantile(col_name, [0.25, 0.75], 0.01)
        )
        if len(quantiles) < 2:
            continue

        q1, q3 = quantiles
        iqr     = q3 - q1
        if iqr == 0:
            continue

        lower, upper  = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        outlier_count = batch_df.filter(
            F.col(col_name).isNotNull() &
            ((F.col(col_name) < lower) | (F.col(col_name) > upper))
        ).count()
        outlier_rate  = outlier_count / total

        if outlier_rate > OUTLIER_THRESHOLD:
            severity = "HIGH" if outlier_rate > 0.20 else "MEDIUM"
            sample   = (
                batch_df.filter(
                    (F.col(col_name) < lower) | (F.col(col_name) > upper)
                ).select(col_name).limit(5).toPandas()[col_name].tolist()
            )
            issue = {
                "batch_id":      batch_id,
                "timestamp":     timestamp,
                "column":        col_name,
                "issue_type":    "Statistical Outlier",
                "severity":      severity,
                "detail": (
                    f"{outlier_count} outlier(s) ({outlier_rate:.1%} of rows). "
                    f"IQR bounds: [{lower:.2f}, {upper:.2f}]"
                ),
                "sample_values": str(sample),
            }
            issues.append(issue)

            key = (col_name, "Statistical Outlier")
            if key not in KNOWN_ISSUES:
                new_issues.append(issue)
                KNOWN_ISSUES.add(key)
                print(f"  🆕 NEW  Outlier          | {col_name} | {outlier_count} outliers ({outlier_rate:.1%})")
            else:
                print(f"  ♻️  SEEN Outlier          | {col_name} | {outlier_count} outliers ({outlier_rate:.1%}) — LLM skipped")

    # ── Detector 3: Format Inconsistency ──────────────────────────────────────
    if "issue_d" in batch_df.columns:
        iso_count = batch_df.filter(
            F.regexp_extract(F.col("issue_d"), r"^[A-Za-z]{3}-\d{4}$", 0) != ""
        ).count()
        us_count = batch_df.filter(
            F.regexp_extract(F.col("issue_d"), r"^\d{2}/\d{4}$", 0) != ""
        ).count()

        if iso_count > 0 and us_count > 0:
            sample = batch_df.select("issue_d").limit(5).toPandas()["issue_d"].tolist()
            issue  = {
                "batch_id":      batch_id,
                "timestamp":     timestamp,
                "column":        "issue_d",
                "issue_type":    "Format Inconsistency",
                "severity":      "MEDIUM",
                "detail": (
                    f"Mixed date formats detected: "
                    f"{iso_count} rows as MMM-YYYY, {us_count} rows as MM/YYYY"
                ),
                "sample_values": str(sample),
            }
            issues.append(issue)

            key = ("issue_d", "Format Inconsistency")
            if key not in KNOWN_ISSUES:
                new_issues.append(issue)
                KNOWN_ISSUES.add(key)
                print(f"  🆕 NEW  Format Mismatch  | issue_d | mixed date formats")
            else:
                print(f"  ♻️  SEEN Format Mismatch  | issue_d — LLM skipped")

    # ── Write all issues to JSON ──────────────────────────────────────────────
    if not issues:
        print(f"  ✅ Batch {batch_id} — no issues detected")
        return

    output_dir  = get_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    issues_path = output_dir / "issues_output.json"

    existing = []
    if issues_path.exists():
        with open(issues_path, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []

    combined = (existing + issues)[-MAX_ISSUES_STORED:]

    with open(issues_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)

    # Quality score based on the current batch only
    deductions    = {"HIGH": 8, "MEDIUM": 4, "LOW": 2}
    raw_score     = 100 - sum(deductions.get(i["severity"], 4) for i in issues)
    quality_score = max(0, min(100, raw_score))

    with open(output_dir / "quality_score.json", "w", encoding="utf-8") as f:
        json.dump({"quality_score": quality_score, "batch_id": batch_id}, f)

    with open(output_dir / "df_shape.json", "w", encoding="utf-8") as f:
        json.dump({"total_rows": total, "total_columns": len(batch_df.columns)}, f)

    print(f"  ✅ {len(issues)} issue(s) written | quality score: {quality_score}/100")
    print(f"  📋 Known issues registry: {len(KNOWN_ISSUES)} unique (column, type) pair(s)")

    # ── Trigger LLM only for genuinely new issues ─────────────────────────────
    if new_issues:
        print(f"  🆕 {len(new_issues)} new issue(s) — triggering LLM ...")
        trigger_suggester(new_issues)
    else:
        print(f"  ⏭️  All issues already known — LLM skipped")


# ── Main ──────────────────────────────────────────────────────────────────────
def run_streaming_detector(
    broker:      str = KAFKA_BROKER,
    topic:       str = TOPIC_NAME,
    trigger_sec: int = TRIGGER_SEC,
):
    LOCAL_MODE = not os.path.exists("/dbfs")

    if LOCAL_MODE:
        spark = (
            SparkSession.builder
            .appName("DataQualityStreamDetector")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
            )
            .getOrCreate()
        )
    else:
        from databricks.connect.session import DatabricksSession
        spark = DatabricksSession.builder.serverless().getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"[STREAM] Listening to Kafka topic '{topic}' @ {broker}")
    print(f"[STREAM] Trigger interval: {trigger_sec}s")
    print(f"[STREAM] LLM triggers only on NEW (column, issue_type) combinations\n")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream
        .select(
            F.from_json(F.col("value").cast("string"), LOAN_SCHEMA).alias("data")
        )
        .select("data.*")
    )

    checkpoint_dir = str(get_output_dir() / "_checkpoint")

    query = (
        parsed_stream.writeStream
        .outputMode("append")
        .trigger(processingTime=f"{trigger_sec} seconds")
        .foreachBatch(detect_issues_in_batch)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    print("[STREAM] Streaming query started — press Ctrl+C to stop\n")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[STREAM] Interrupted — stopping query ...")
        query.stop()


if __name__ == "__main__":
    run_streaming_detector()