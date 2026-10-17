"""
rules_vs_llm.py
Exploratory comparison that motivated the evaluation (spec §11). NOT part of
the preregistered evaluation: the rules below were written after the LLM
failures had been seen.

Scores three evidence-driven rules and three saved LLM runs (option 1 only)
cell by cell against the pre-injection reference, on the first 500 rows.

Usage (from the project root):
    .venv/bin/python dqeval/exploratory/rules_vs_llm.py
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT / "prototype" / "detection"))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)   # Spark workers must match the driver

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StructField, StructType

import detector

N         = 500
DIRTY_CSV = ROOT / "data" / "demo_lendingclub.csv"    # course demo data, anomalies already injected
CLEAN_CSV = ROOT / "data" / "LendingClub_100k.csv"    # the same rows before injection
RUNS = {   # saved Agent 2 outputs; see README.md for what changed between runs
    "LLM run A (inferred evidence)": HERE / "runs" / "run_A_inferred_evidence.json",
    "LLM run B (facts, 'set null')": HERE / "runs" / "run_B_facts_set_null.json",
    "LLM run C (facts, 'flag')":     HERE / "runs" / "run_C_facts_flag.json",
}

spark = SparkSession.builder.master("local[2]").getOrCreate()
spark.sparkContext.setLogLevel("OFF")
cols   = [f.name for f in detector.LOAN_SCHEMA.fields]
schema = StructType(detector.LOAN_SCHEMA.fields + [StructField("id", LongType())])

dirty = pd.read_csv(DIRTY_CSV, usecols=cols + ["id"], nrows=N)[cols + ["id"]]
truth = pd.read_csv(CLEAN_CSV, usecols=cols + ["id"], nrows=N).set_index("id")
rows  = dirty.astype(object).where(pd.notna(dirty), None).values.tolist()
base  = spark.createDataFrame(rows, schema=schema).cache()
orig  = dirty.set_index("id")


# ── Rules: decide only from facts the evidence step also computes ────────────
def rule_null(df, col="loan_amnt"):
    same = [f.name for f in df.schema.fields if f.name not in (col, "id") and f.dataType == df.schema[col].dataType]
    for c in same:
        eq   = df.agg(F.avg((F.col(col) == F.col(c)).cast("int"))).first()[0]
        fill = df.filter(F.col(col).isNull()).agg(F.avg(F.col(c).isNotNull().cast("int"))).first()[0]
        if eq is not None and eq >= 0.99 and fill >= 0.99:
            return df.withColumn(col, F.coalesce(F.col(col), F.col(c)))          # recoverable → restore
    return df.withColumn(f"{col}_quarantine", F.col(col).isNull())               # unrecoverable → flag


def rule_outlier(df, col="annual_inc"):
    q1, q3 = df.select(col).dropna().approxQuantile(col, [0.25, 0.75], 0.01)
    lo, hi = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
    out = df.filter((F.col(col) < lo) | (F.col(col) > hi))
    top = out.groupBy(col).count().orderBy(F.desc("count")).first()
    if top and top["count"] / out.count() >= 0.5:                               # one repeated value dominates
        return df.withColumn(f"{col}_quarantine", F.col(col) == top[col])        # flag it, keep the rest as-is
    return df


def rule_format(df, col="issue_d"):
    def months(pattern, fmt):
        vals = [r[0] for r in df.filter(F.col(col).rlike(pattern)).select(col).distinct().collect()]
        return vals, [datetime.strptime(v, fmt) for v in vals]
    maj_vals, maj = months(r"^[A-Za-z]{3}-\d{4}$", "%b-%Y")
    min_vals, mnr = months(r"^\d{2}/\d{4}$", "%m/%Y")
    if maj and mnr and not any(min(maj) <= d <= max(maj) for d in mnr):         # values outside known range
        return df.withColumn(f"{col}_quarantine", F.col(col).isin(min_vals))
    mapping = {v: datetime.strptime(v, "%m/%Y").strftime("%b-%Y") for v in min_vals}   # representation only
    return df.replace(mapping, subset=[col])


RULES = {"loan_amnt": rule_null, "annual_inc": rule_outlier, "issue_d": rule_format}


# ── Scoring against the pre-injection values ─────────────────────────────────
def score(col, out):
    out  = out.set_index("id").reindex(orig.index)
    same = lambda a, b: (a == b) | (a.isna() & b.isna())
    bad  = ~same(orig[col], truth[col])                       # cells the injection corrupted
    new_cols = [c for c in out.columns if c not in orig.columns]
    flag = out[new_cols].fillna(False).astype(bool).any(axis=1) if new_cols else pd.Series(False, index=out.index)
    kept = same(out[col], orig[col])
    return dict(
        bad        = int(bad.sum()),
        restored   = int((bad & same(out[col], truth[col])).sum()),
        flagged    = int((bad & kept & flag).sum()),
        fabricated = int((bad & ~kept & ~same(out[col], truth[col])).sum()),   # overwritten with a wrong value, incl. null
        harmed     = int((~bad & ~same(out[col], truth[col])).sum()),
        false_flag = int((~bad & flag).sum()),
    )


def fmt(s):
    if isinstance(s, str):
        return s
    parts = [f"{s['restored']} restored" if s["restored"] else "", f"{s['flagged']} flagged" if s["flagged"] else "",
             f"{s['fabricated']} fabricated" if s["fabricated"] else "",
             f"{s['harmed']} correct cells changed" if s["harmed"] else "",
             f"{s['false_flag']} correct cells flagged" if s["false_flag"] else ""]
    return f"of {s['bad']} bad: " + ", ".join(p for p in parts if p) if any(parts) else f"of {s['bad']} bad: nothing done"


def run_llm(path, option):
    res = {}
    for r in json.load(open(path)):
        col = r["input"]["column"]
        s = next(x for x in r["remediation"]["suggestions"] if x["option"] == option)
        ns = {"df": base}
        try:
            exec(s["pyspark_code"], ns)
            res[col] = score(col, ns["df"].toPandas())
        except Exception as e:
            res[col] = f"CRASH ({type(e).__name__})"
    return res


arms = {"Rules (3 functions, no LLM)": {c: score(c, f(base).toPandas()) for c, f in RULES.items()}}
for name, path in RUNS.items():
    arms[name] = run_llm(path, option=1)

for name, res in arms.items():
    print(f"\n{name}")
    for col in ["loan_amnt", "annual_inc", "issue_d"]:
        print(f"  {col:11} {fmt(res[col])}")
spark.stop()
