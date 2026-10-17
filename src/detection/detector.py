"""
detector.py
Spark Structured Streaming detector for the loan-stream Kafka topic.

Key behaviors:
  1. Known issues tracking — tracks (column, issue_type) pairs seen this session.
     LLM is only triggered when a genuinely NEW combination appears.
  2. Auto LLM trigger — passes only the new issues to run_suggester(),
     so the LLM processes exactly the new findings rather than the full history.

Usage (from the project root):
    python src/detection/detector.py
"""

import json
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

# issue_d formats: (label, regex, strptime format)
MMM_YYYY = ("MMM-YYYY", r"^[A-Za-z]{3}-\d{4}$", "%b-%Y")
MM_YYYY  = ("MM/YYYY",  r"^\d{2}/\d{4}$",       "%m/%Y")

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


# ── Paths ─────────────────────────────────────────────────────────────────────
# detector.py is at src/detection/, so go up 3 levels to reach project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR     = PROJECT_ROOT / "data"


# ── Auto-trigger LLM (new issues only) ───────────────────────────────────────
def trigger_suggester(new_issues: list):
    """
    Calls run_suggester() with only the newly detected issues, avoiding
    redundant LLM calls on issues that were already handled.
    """
    llm_dir = PROJECT_ROOT / "src" / "llm"
    if str(llm_dir) not in sys.path:
        sys.path.insert(0, str(llm_dir))

    try:
        from suggester import run_suggester
        print(f"  🤖 Triggering LLM for {len(new_issues)} new issue(s) ...")
        run_suggester(new_issues)
        print("  ✅ LLM suggestions generated and appended")
    except Exception as e:
        print(f"  ⚠️  Suggester failed (non-fatal): {e}")


# ── Evidence for the LLM ──────────────────────────────────────────────────────
# Facts computed from the batch, so the agents reason from the data instead of
# only a column name and a few samples. Computed for NEW issues only, since
# those are the only ones sent to the LLM.
def null_spike_evidence(batch_df: DataFrame, col_name: str) -> list[str]:
    evidence  = []
    others    = [f for f in batch_df.schema.fields if f.name != col_name]
    null_rows = batch_df.filter(F.col(col_name).isNull())

    # Share of null rows in which each other column is still populated
    present = null_rows.agg(*[
        F.avg(F.col(f.name).isNotNull().cast("int")).alias(f.name) for f in others
    ]).first().asDict()
    co_missing = [name for name, rate in present.items() if rate is not None and rate < 0.5]
    if co_missing:
        evidence.append(
            f"Rows missing {col_name} are mostly missing {', '.join(co_missing)} too, "
            f"which points to whole records being lost rather than one field."
        )
    else:
        evidence.append(
            f"Rows missing {col_name} still have every other field populated, "
            f"so the loss is isolated to this column."
        )

    # Same-type columns that match this one wherever both are present → backfill sources
    col_type  = batch_df.schema[col_name].dataType
    same_type = [f.name for f in others if f.dataType == col_type]
    if same_type:
        match = batch_df.agg(*[
            F.avg((F.col(col_name) == F.col(c)).cast("int")).alias(c) for c in same_type
        ]).first().asDict()
        for c in same_type:
            if match[c] is not None and match[c] >= 0.95:
                note = (
                    "candidate backfill source" if present[c] >= 0.8
                    else "could recover only part of the missing values"
                )
                evidence.append(
                    f"{c} equals {col_name} in {match[c]:.0%} of rows where both are present, "
                    f"and is populated in {present[c]:.0%} of rows where {col_name} is null "
                    f"({note})."
                )
    return evidence


def outlier_evidence(
    batch_df: DataFrame, col_name: str,
    q1: float, q3: float, lower: float, upper: float, outlier_count: int,
) -> list[str]:
    outliers = batch_df.filter((F.col(col_name) < lower) | (F.col(col_name) > upper))
    evidence = [f"The middle 50% of {col_name} lies between {q1:,} and {q3:,}."]

    top = outliers.groupBy(col_name).count().orderBy(F.desc("count")).first()
    if top["count"] / outlier_count >= 0.5:
        value, n = top[col_name], top["count"]
        evidence.append(
            f"{n} of {outlier_count} outliers ({n / outlier_count:.0%}) are the exact value {value:,}; "
            f"a repeated exact value suggests a placeholder or sentinel, not genuine extremes."
        )
        rest = outliers.filter(F.col(col_name) != value).agg(
            F.count(col_name).alias("n"), F.min(col_name).alias("lo"), F.max(col_name).alias("hi"),
        ).first()
        if rest["n"]:
            evidence.append(
                f"The other {rest['n']} outliers range from {rest['lo']:,} to {rest['hi']:,} "
                f"and may be legitimate values."
            )
    else:
        stats = outliers.agg(
            F.countDistinct(col_name).alias("d"), F.min(col_name).alias("lo"), F.max(col_name).alias("hi"),
        ).first()
        evidence.append(
            f"Outliers are spread across {stats['d']} distinct values from {stats['lo']:,} "
            f"to {stats['hi']:,}, with no single repeated value."
        )
    return evidence


def _parse_date(value: str, fmt: str) -> datetime | None:
    try:
        return datetime.strptime(value, fmt)
    except ValueError:
        return None


def date_format_evidence(batch_df: DataFrame, iso_count: int, us_count: int) -> list[str]:
    evidence = []
    dates    = {}
    for label, pattern, py_fmt in (MMM_YYYY, MM_YYYY):
        counts = (
            batch_df.filter(F.col("issue_d").rlike(pattern))
            .groupBy("issue_d").count().orderBy(F.desc("count")).collect()
        )
        parsed = sorted(d for d in (_parse_date(r["issue_d"], py_fmt) for r in counts) if d)
        dates[label] = parsed

        n_rows = sum(r["count"] for r in counts)
        top    = ", ".join(f"'{r['issue_d']}' x{r['count']}" for r in counts[:3])
        span   = f", spanning {parsed[0]:%b %Y} to {parsed[-1]:%b %Y}" if parsed else ""
        evidence.append(f"{n_rows} rows use {label} ({len(counts)} distinct value(s){span}); most common: {top}.")

    majority, minority = (MMM_YYYY[0], MM_YYYY[0]) if iso_count >= us_count else (MM_YYYY[0], MMM_YYYY[0])
    if dates[majority] and dates[minority]:
        lo, hi = dates[majority][0], dates[majority][-1]
        if not any(lo <= d <= hi for d in dates[minority]):
            evidence.append(
                f"None of the {minority} dates fall within the {majority} range, "
                f"so they are not the same dates written in another format."
            )
    return evidence


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
                "affected_rows": null_count,
                "total_rows":    total,
                "sample_values": str(sample),
            }
            issues.append(issue)

            key = (col_name, "Null Spike")
            if key not in KNOWN_ISSUES:
                issue["evidence"] = null_spike_evidence(batch_df, col_name)
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
                "affected_rows": outlier_count,
                "total_rows":    total,
                "sample_values": str(sample),
            }
            issues.append(issue)

            key = (col_name, "Statistical Outlier")
            if key not in KNOWN_ISSUES:
                issue["evidence"] = outlier_evidence(
                    batch_df, col_name, q1, q3, lower, upper, outlier_count
                )
                new_issues.append(issue)
                KNOWN_ISSUES.add(key)
                print(f"  🆕 NEW  Outlier          | {col_name} | {outlier_count} outliers ({outlier_rate:.1%})")
            else:
                print(f"  ♻️  SEEN Outlier          | {col_name} | {outlier_count} outliers ({outlier_rate:.1%}) — LLM skipped")

    # ── Detector 3: Format Inconsistency ──────────────────────────────────────
    if "issue_d" in batch_df.columns:
        iso_count = batch_df.filter(F.col("issue_d").rlike(MMM_YYYY[1])).count()
        us_count  = batch_df.filter(F.col("issue_d").rlike(MM_YYYY[1])).count()

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
                # The minority format is the one that needs fixing
                "affected_rows": min(iso_count, us_count),
                "total_rows":    total,
                "sample_values": str(sample),
            }
            issues.append(issue)

            key = ("issue_d", "Format Inconsistency")
            if key not in KNOWN_ISSUES:
                issue["evidence"] = date_format_evidence(batch_df, iso_count, us_count)
                new_issues.append(issue)
                KNOWN_ISSUES.add(key)
                print(f"  🆕 NEW  Format Mismatch  | issue_d | mixed date formats")
            else:
                print(f"  ♻️  SEEN Format Mismatch  | issue_d — LLM skipped")

    # ── Write all issues to JSON ──────────────────────────────────────────────
    if not issues:
        print(f"  ✅ Batch {batch_id} — no issues detected")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    issues_path = DATA_DIR / "issues_output.json"

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

    with open(DATA_DIR / "quality_score.json", "w", encoding="utf-8") as f:
        json.dump({"quality_score": quality_score, "batch_id": batch_id}, f)

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
    spark = (
        SparkSession.builder
        .appName("DataQualityStreamDetector")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
        )
        .getOrCreate()
    )
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

    checkpoint_dir = str(DATA_DIR / "_checkpoint")

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