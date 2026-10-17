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
def trigger_suggester(new_issues: list) -> bool:
    """
    Calls run_suggester() with only the newly detected issues, avoiding
    redundant LLM calls on issues that were already handled.
    Returns True on success, so the caller can retry failed issues later.
    """
    llm_dir = PROJECT_ROOT / "src" / "llm"
    if str(llm_dir) not in sys.path:
        sys.path.insert(0, str(llm_dir))

    try:
        from suggester import run_suggester
        print(f"  🤖 Triggering LLM for {len(new_issues)} new issue(s) ...")
        run_suggester(new_issues)
        print("  ✅ LLM suggestions generated and appended")
        return True
    except Exception as e:
        print(f"  ⚠️  Suggester failed (non-fatal): {e}")
        return False


# ── Evidence for the LLM ──────────────────────────────────────────────────────
# Observations computed from the batch: counts and rates only, no conclusions.
# Interpreting them (root cause, fix) is the agents' job. Computed for NEW
# issues only, since those are the only ones sent to the LLM.
def null_spike_evidence(batch_df: DataFrame, col_name: str) -> list[str]:
    evidence = []
    others   = [f.name for f in batch_df.schema.fields if f.name != col_name]
    is_null  = F.col(col_name).isNull()

    # Fill rate of every other column, in rows where col_name is null vs. present
    fill = batch_df.agg(*[
        agg
        for c in others
        for agg in (
            F.avg(F.when(is_null,  F.col(c).isNotNull().cast("int"))).alias(f"{c}__null"),
            F.avg(F.when(~is_null, F.col(c).isNotNull().cast("int"))).alias(f"{c}__present"),
        )
    ]).first().asDict()
    in_null_rows = {c: fill[f"{c}__null"] for c in others}

    low = [
        f"{c} {rate:.0%} (vs {_pct(fill[f'{c}__present'])} where {col_name} is present)"
        for c, rate in in_null_rows.items() if rate < 0.95
    ]
    if low:
        evidence.append(
            f"In rows where {col_name} is null, these columns are under 95% filled: "
            f"{', '.join(low)}. All other columns are at least 95% filled."
        )
    else:
        evidence.append(
            f"In rows where {col_name} is null, every other column is at least "
            f"{min(in_null_rows.values()):.0%} filled."
        )

    # Same-type columns that equal col_name wherever both are present
    col_type  = batch_df.schema[col_name].dataType
    same_type = [f.name for f in batch_df.schema.fields if f.name in others and f.dataType == col_type]
    if same_type:
        match = batch_df.agg(*[
            agg
            for c in same_type
            for agg in (
                F.avg((F.col(col_name) == F.col(c)).cast("int")).alias(f"{c}__rate"),
                F.count(F.col(col_name) == F.col(c)).alias(f"{c}__n"),
            )
        ]).first().asDict()
        for c in same_type:
            rate, n = match[f"{c}__rate"], match[f"{c}__n"]
            if rate is not None and rate >= 0.95:
                evidence.append(
                    f"{c} equals {col_name} in {rate:.0%} of the {n} rows where both are present, "
                    f"and is filled in {in_null_rows[c]:.0%} of rows where {col_name} is null."
                )
    return evidence


def outlier_evidence(
    batch_df: DataFrame, col_name: str,
    q1: float, q3: float, lower: float, upper: float, outlier_count: int,
) -> list[str]:
    outliers = batch_df.filter((F.col(col_name) < lower) | (F.col(col_name) > upper))
    evidence = [f"The middle 50% of {col_name} lies between {q1:,} and {q3:,}."]

    top   = outliers.groupBy(col_name).count().orderBy(F.desc("count")).first()
    value = top[col_name]
    evidence.append(
        f"The most common outlier value is {value:,}: {top['count']} of {outlier_count} "
        f"outliers ({top['count'] / outlier_count:.0%})."
    )

    rest = outliers.filter(F.col(col_name) != value).agg(
        F.count(col_name).alias("n"), F.countDistinct(col_name).alias("d"),
        F.min(col_name).alias("lo"), F.max(col_name).alias("hi"),
    ).first()
    if rest["n"]:
        evidence.append(
            f"The other {rest['n']} outliers take {rest['d']} distinct value(s) "
            f"from {rest['lo']:,} to {rest['hi']:,}."
        )
    return evidence


def _pct(rate: float | None) -> str:
    return "n/a" if rate is None else f"{rate:.0%}"


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
        inside = sum(lo <= d <= hi for d in dates[minority])
        evidence.append(
            f"{inside} of {len(dates[minority])} distinct {minority} value(s) fall within "
            f"the {majority} range ({lo:%b %Y} to {hi:%b %Y})."
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
                print(f"  🆕 NEW  Format Mismatch  | issue_d | mixed date formats")
            else:
                print(f"  ♻️  SEEN Format Mismatch  | issue_d — LLM skipped")

    # ── Quality score (based on the current batch only; written for clean batches too) ──
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    deductions    = {"HIGH": 8, "MEDIUM": 4, "LOW": 2}
    raw_score     = 100 - sum(deductions.get(i["severity"], 4) for i in issues)
    quality_score = max(0, min(100, raw_score))

    with open(DATA_DIR / "quality_score.json", "w", encoding="utf-8") as f:
        json.dump({"quality_score": quality_score, "batch_id": batch_id}, f)

    if not issues:
        print(f"  ✅ Batch {batch_id} — no issues detected | quality score: {quality_score}/100")
        return

    # ── Append issues to the rolling log ──────────────────────────────────────
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

    print(f"  ✅ {len(issues)} issue(s) written | quality score: {quality_score}/100")

    # ── Trigger LLM only for genuinely new issues ─────────────────────────────
    # An issue becomes "known" only after the LLM succeeds, so a failed call
    # (e.g. API outage) is retried on the next batch instead of being dropped.
    if new_issues:
        print(f"  🆕 {len(new_issues)} new issue(s) — triggering LLM ...")
        if trigger_suggester(new_issues):
            KNOWN_ISSUES.update((i["column"], i["issue_type"]) for i in new_issues)
        else:
            print(f"  🔁 {len(new_issues)} issue(s) will be retried on the next batch")
    else:
        print(f"  ⏭️  All issues already known — LLM skipped")

    print(f"  📋 Known issues registry: {len(KNOWN_ISSUES)} unique (column, type) pair(s)")


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