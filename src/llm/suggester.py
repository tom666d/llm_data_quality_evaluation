"""
suggester.py (RAG version)
Two-agent LLM pipeline for data quality remediation suggestions.

RAG flow:
  historical_decisions.json
        ↓
  sentence-transformers (embed each record)
        ↓
  FAISS index
        ↓
  query with current issue → top-K similar past cases
        ↓
  inject into LLM prompt as grounded context

Key parameter:
  run_suggester(issues_override=None)
    - None (default)   : reads all issues from issues_output.json  [manual / batch mode]
    - list of issues   : processes only those issues               [streaming mode]

Install requirements:
  pip install faiss-cpu sentence-transformers
"""

import json
import os
import re
from pathlib import Path
from typing import Any

import openai
from dotenv import load_dotenv

# ── RAG dependencies ──────────────────────────────────────────────────────────
try:
    import faiss
    from sentence_transformers import SentenceTransformer
    RAG_AVAILABLE = True
except ImportError:
    RAG_AVAILABLE = False
    print("[WARN] faiss-cpu or sentence-transformers not installed.")
    print("       RAG disabled — LLM will run without historical context.")
    print("       Run: pip install faiss-cpu sentence-transformers")

# ── Embedding model (loaded once, reused across all calls) ────────────────────
_EMBED_MODEL = None

def get_embed_model():
    global _EMBED_MODEL
    if _EMBED_MODEL is None and RAG_AVAILABLE:
        print("[RAG] Loading embedding model (first run may take a moment)...")
        _EMBED_MODEL = SentenceTransformer("all-MiniLM-L6-v2")
    return _EMBED_MODEL


# =========================
# RAG: Build vector store
# =========================
def build_vector_store(
    records: list[dict[str, Any]],
) -> tuple[Any, list[dict[str, Any]]] | tuple[None, None]:
    """
    Converts historical decision records into a FAISS vector index.

    Each record is embedded as:
      "{issue_type} | {column} | {chosen_action} | {detail_summary}"

    Returns (faiss_index, flat_records) or (None, None) if RAG is unavailable.
    """
    if not RAG_AVAILABLE or not records:
        return None, None

    model = get_embed_model()
    texts = [
        f"{r.get('issue_type', '')} | {r.get('column', '')} | "
        f"{r.get('chosen_action', '')} | {r.get('detail_summary', '')}"
        for r in records
    ]

    embeddings = model.encode(texts, convert_to_numpy=True).astype("float32")
    faiss.normalize_L2(embeddings)

    index = faiss.IndexFlatIP(embeddings.shape[1])
    index.add(embeddings)

    print(f"[RAG] Vector store built — {len(records)} historical record(s) indexed")
    return index, records


# =========================
# RAG: Query similar cases
# =========================
def query_similar_cases(
    issue:   dict[str, Any],
    index:   Any,
    records: list[dict[str, Any]],
    k:       int = 3,
) -> list[dict[str, Any]]:
    """
    Returns the top-K most similar historical decisions for a given issue.
    """
    if not RAG_AVAILABLE or index is None or not records:
        return []

    model      = get_embed_model()
    query_text = (
        f"{issue.get('issue_type', '')} | "
        f"{issue.get('column', '')} | "
        f"{issue.get('detail', '')}"
    )

    query_vec = model.encode([query_text], convert_to_numpy=True).astype("float32")
    faiss.normalize_L2(query_vec)

    scores, idxs = index.search(query_vec, min(k, len(records)))

    results = []
    for score, idx in zip(scores[0], idxs[0]):
        if idx < 0:
            continue
        record = dict(records[idx])
        record["_similarity"] = round(float(score), 4)
        results.append(record)

    return results


# =========================
# RAG: Format context
# =========================
def format_rag_context(similar_cases: list[dict[str, Any]]) -> str:
    """Formats retrieved cases into a prompt-ready string."""
    if not similar_cases:
        return "No similar historical decisions found."

    lines = []
    for i, case in enumerate(similar_cases, start=1):
        lines.append(
            f"{i}. [Similarity: {case.get('_similarity', 'N/A')}] "
            f"Column: {case.get('column')} | "
            f"Issue: {case.get('issue_type')} | "
            f"Chosen Action: {case.get('chosen_action')} | "
            f"Rationale: {case.get('chosen_rationale')} | "
            f"Confidence: {case.get('chosen_confidence')} | "
            f"Caveats: {case.get('chosen_caveats')}"
        )
    return "\n".join(lines)


# =========================
# Helper functions
# =========================
def load_df_shape(path: Path) -> dict[str, int]:
    if not path.exists():
        raise FileNotFoundError(f"df_shape file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        shape_info = json.load(f)
    total_rows    = shape_info.get("total_rows")
    total_columns = shape_info.get("total_columns")
    if not isinstance(total_rows, int) or total_rows <= 0:
        raise ValueError("df_shape.json must contain a positive integer 'total_rows'")
    if not isinstance(total_columns, int) or total_columns <= 0:
        raise ValueError("df_shape.json must contain a positive integer 'total_columns'")
    return {"total_rows": total_rows, "total_columns": total_columns}


def extract_affected_rows(detail: str) -> int | None:
    if not detail:
        return None
    m = re.search(r"\((\d+)\s*/\s*(\d+)\s*rows\)", detail)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s+outlier\(s\)", detail, re.IGNORECASE)
    if m:
        return int(m.group(1))
    counts = re.findall(r"(\d+)\s+rows", detail, re.IGNORECASE)
    return min(int(x) for x in counts) if counts else None


def extract_affected_rows_percent(detail: str) -> float | None:
    if not detail:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)%\s+of\s+rows", detail, re.IGNORECASE)
    if m:
        return float(m.group(1))
    m = re.search(r"Null rate:\s*(\d+(?:\.\d+)?)%", detail, re.IGNORECASE)
    return float(m.group(1)) if m else None


def compute_affected_rows_percent(detail: str, total_rows: int) -> float | None:
    pct = extract_affected_rows_percent(detail)
    if pct is not None:
        return round(pct, 2)
    rows = extract_affected_rows(detail)
    if rows is None or total_rows <= 0:
        return None
    return round((rows / total_rows) * 100, 2)


def severity_to_score(severity: str) -> int:
    return {"LOW": 3, "MEDIUM": 6, "HIGH": 9}.get(str(severity).upper(), 5)


def affected_rows_percent_to_score(pct: float | None) -> int:
    if pct is None: return 5
    if pct < 5:     return 1
    if pct < 15:    return 3
    if pct < 30:    return 5
    if pct < 50:    return 7
    return 9


def compute_priority_score(severity: str, pct: float | None) -> int:
    score = round(0.6 * severity_to_score(severity) + 0.4 * affected_rows_percent_to_score(pct))
    return max(1, min(10, score))


def compute_total_affected_rows(issues_list: list[dict], total_rows: int) -> int:
    total_pct = sum(
        compute_affected_rows_percent(i.get("detail", ""), total_rows) or 0.0
        for i in issues_list
    )
    return round((min(total_pct, 100.0) / 100) * total_rows)


def compute_quality_scores_for_issue(
    current_issue: dict, issues_list: list[dict], total_rows: int
) -> dict[str, float]:
    total_affected_before = compute_total_affected_rows(issues_list, total_rows)
    current_pct           = compute_affected_rows_percent(current_issue.get("detail", ""), total_rows) or 0.0
    current_rows          = round((current_pct / 100) * total_rows)
    affected_after        = max(0, total_affected_before - current_rows)
    before = round(((total_rows - total_affected_before) / total_rows) * 100, 2) if total_rows else 0.0
    after  = round(((total_rows - affected_after) / total_rows) * 100, 2) if total_rows else 0.0
    return {"before": before, "after": after, "delta": round(after - before, 2)}


def load_historical_records(path: Path) -> list[dict[str, Any]]:
    """Returns a flat list of all historical decision records."""
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
        if not content:
            return []
        data = json.loads(content)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            flat = []
            for records in data.values():
                flat.extend(records)
            return flat
    except json.JSONDecodeError:
        pass
    return []


def route_issue(issue: dict[str, Any]) -> str:
    return {
        "Null Spike":           "imputation",
        "Statistical Outlier":  "statistical",
        "Format Inconsistency": "standardization",
    }.get(issue.get("issue_type", ""), "general")


# =========================
# Agent 1: Root Cause
# =========================
def diagnose_root_cause(client, issue: dict[str, Any]) -> dict[str, Any]:
    prompt = f"""
You are Agent 1 in a two-agent data quality remediation workflow.

Analyze the issue and return the likely root cause and business impact.

Issue:
Column: {issue['column']}
Issue Type: {issue['issue_type']}
Severity: {issue['severity']}
Detail: {issue['detail']}
Sample Values: {issue['sample_values']}

Instructions:
- Explain the likely upstream or operational cause. Do NOT simply restate the issue type.
- business_impact: downstream effect on analytics, reporting, or modeling.
- Keep both fields to 1-2 short sentences.
- Return JSON only.

Return exactly:
{{
  "root_cause": "string",
  "business_impact": "string"
}}
"""
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a precise data quality diagnosis agent. Return valid JSON only."},
            {"role": "user",   "content": prompt},
        ],
    )
    return json.loads(response.choices[0].message.content)


# =========================
# Agent 2: Remediation (RAG-enhanced)
# =========================
def generate_remediation(
    client,
    issue:           dict[str, Any],
    diagnosis:       dict[str, Any],
    rag_context:     str,
    critic_feedback: str = "",
) -> dict[str, Any]:

    critic_block = ""
    if critic_feedback:
        critic_block = f"""
Previous attempt rejected by the quality checker:
{critic_feedback}
Please fix and regenerate.
"""

    prompt = f"""
You are Agent 2 in a two-agent data quality remediation workflow.

Generate remediation suggestions with runnable PySpark fix code,
informed by the issue, Agent 1 diagnosis, and similar past decisions retrieved via RAG.

Original issue:
Column: {issue['column']}
Issue Type: {issue['issue_type']}
Severity: {issue['severity']}
Detail: {issue['detail']}
Sample Values: {issue['sample_values']}

Agent 1 diagnosis:
Root Cause: {diagnosis['root_cause']}
Business Impact: {diagnosis['business_impact']}
Priority Score: {diagnosis['priority_score']}
Affected Rows Percent: {diagnosis['affected_rows_percent']}

Similar past decisions (retrieved via RAG — use as preference signals):
{rag_context}
{critic_block}
Requirements:
1. Generate exactly 3 remediation suggestions.
2. Options 1 and 2 must be actual remediation actions.
3. Option 3 must always be "Decline changes".
4. Each suggestion must include: option, action, confidence, rationale, caveats, pyspark_code.

Field rules:
- option: integer (1, 2, or 3)
- action: short remediation title
- confidence: integer 0-100
- rationale: 1-2 concise sentences
- caveats: 1 concise sentence
- pyspark_code: runnable PySpark code referencing a DataFrame named df (not pseudo-code)

Confidence scoring:
- 80-100: standard practice, safe to apply directly
- 60-79: generally valid but context-dependent
- 40-59: usable but has clear trade-offs
- below 40: risky or not recommended

Important:
- Prefer strategies consistent with RAG-retrieved past decisions.
- Higher similarity score = stronger preference signal.
- Return JSON only.

Special rule for option 3:
- action must be exactly "Decline changes"
- pyspark_code must be "# No change applied to df"
- confidence can be 100

Return exactly:
{{
  "suggestions": [
    {{"option": 1, "action": "string", "confidence": 85, "rationale": "string", "caveats": "string", "pyspark_code": "string"}},
    {{"option": 2, "action": "string", "confidence": 65, "rationale": "string", "caveats": "string", "pyspark_code": "string"}},
    {{"option": 3, "action": "Decline changes", "confidence": 100, "rationale": "string", "caveats": "string", "pyspark_code": "# No change applied to df"}}
  ]
}}
"""
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a PySpark remediation agent. Return valid JSON only."},
            {"role": "user",   "content": prompt},
        ],
    )
    return json.loads(response.choices[0].message.content)


# =========================
# Critic Agent
# =========================
def critic_check(remediation: dict[str, Any]) -> tuple[bool, str]:
    suggestions = remediation.get("suggestions", [])
    if len(suggestions) != 3:
        return False, f"Expected 3 suggestions, got {len(suggestions)}"
    for s in suggestions:
        num = s.get("option", "?")
        if not s.get("pyspark_code", "").strip():
            return False, f"Option {num} missing pyspark_code"
        if s.get("option") != 3 and "df" not in s.get("pyspark_code", ""):
            return False, f"Option {num} pyspark_code does not reference 'df'"
        if not isinstance(s.get("confidence"), int):
            return False, f"Option {num} confidence must be an integer"
    option3 = next((s for s in suggestions if s.get("option") == 3), None)
    if not option3 or option3.get("action") != "Decline changes":
        return False, "Option 3 must be 'Decline changes'"
    return True, "OK"


def generate_remediation_with_critic(
    client, issue, diagnosis, rag_context, max_retries=2
) -> dict[str, Any]:
    feedback = ""
    for attempt in range(max_retries + 1):
        result = generate_remediation(client, issue, diagnosis, rag_context, feedback)
        for idx, s in enumerate(result.get("suggestions", []), start=1):
            s["option"] = s.get("option", idx)
        passed, feedback = critic_check(result)
        if passed:
            if attempt > 0:
                print(f"  Critic passed on attempt {attempt + 1}")
            return result
        print(f"  Critic failed (attempt {attempt + 1}): {feedback}")
    print("  Critic: max retries reached, returning last result")
    return result


def build_output_record(issue, diagnosis, remediation, quality_score) -> dict:
    return {
        "input": {
            "column":        issue["column"],
            "issue_type":    issue["issue_type"],
            "severity":      issue["severity"],
            "detail":        issue["detail"],
            "sample_values": issue["sample_values"],
        },
        "diagnosis": {
            "root_cause":            diagnosis["root_cause"],
            "business_impact":       diagnosis["business_impact"],
            "priority_score":        diagnosis["priority_score"],
            "affected_rows_percent": diagnosis["affected_rows_percent"],
        },
        "remediation": {"suggestions": remediation.get("suggestions", [])},
        "quality_score": quality_score,
    }


# =========================
# Main pipeline
# =========================
def run_suggester(issues_override: list | None = None):
    """
    Args:
        issues_override: if provided, process only these issues (streaming mode).
                         if None, read all issues from issues_output.json (batch/manual mode).
    """
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")

    client = openai.OpenAI(api_key=api_key)

    LOCAL_MODE = not os.path.exists("/dbfs")
    data_dir   = project_root / "data" if LOCAL_MODE else Path("/Volumes/workspace/team6/data")

    output_path     = data_dir / "issues_with_suggestions.json"
    historical_path = data_dir / "historical_decisions.json"
    df_shape_path   = data_dir / "df_shape.json"

    # ── Decide which issues to process ───────────────────────────────────────
    if issues_override is not None:
        # Streaming mode: only process the new issues passed in
        issues = issues_override
        print(f"[STREAM MODE] Processing {len(issues)} new issue(s) passed from detector")
    else:
        # Batch/manual mode: read everything from disk
        input_path = data_dir / "issues_output.json"
        if not input_path.exists():
            raise FileNotFoundError(f"issues_output.json not found: {input_path}")
        with open(input_path, "r", encoding="utf-8") as f:
            issues = json.load(f)
        print(f"[BATCH MODE] Loaded {len(issues)} issue(s) from issues_output.json")

    df_shape   = load_df_shape(df_shape_path)
    TOTAL_ROWS = df_shape["total_rows"]
    print(f"[RULE] Dataframe shape: rows={TOTAL_ROWS}, columns={df_shape['total_columns']}\n")

    # ── Build RAG vector store ────────────────────────────────────────────────
    all_records           = load_historical_records(historical_path)
    faiss_index, flat_recs = build_vector_store(all_records)

    if faiss_index is None:
        print("[RAG] No vector store available — LLM will run without historical context\n")

    print("[AI] Running RAG-enhanced agent pipeline...\n")

    # ── Load existing suggestions to append to (streaming mode) ──────────────
    existing_results = []
    if output_path.exists():
        with open(output_path, "r", encoding="utf-8") as f:
            try:
                existing_results = json.load(f)
            except json.JSONDecodeError:
                existing_results = []

    new_results = []
    for issue in issues:
        print(f"Processing: [{issue['severity']}] {issue['column']} — {issue['issue_type']}")
        print(f"  Router -> strategy: {route_issue(issue)}")

        # Agent 1: root cause diagnosis
        root_cause_result = diagnose_root_cause(client, issue)
        affected_pct      = compute_affected_rows_percent(issue.get("detail", ""), TOTAL_ROWS)
        priority_score    = compute_priority_score(issue.get("severity", ""), affected_pct)

        diagnosis_result = {
            "root_cause":            root_cause_result.get("root_cause"),
            "business_impact":       root_cause_result.get("business_impact"),
            "priority_score":        priority_score,
            "affected_rows_percent": affected_pct,
        }
        print(f"  Agent 1 done -> priority={priority_score}, affected={affected_pct}%")

        # RAG retrieval
        similar_cases = query_similar_cases(issue, faiss_index, flat_recs or [], k=3)
        rag_context   = format_rag_context(similar_cases)
        print(f"  RAG -> retrieved {len(similar_cases)} similar case(s)")

        # Agent 2: remediation suggestions
        remediation_result = generate_remediation_with_critic(
            client, issue, diagnosis_result, rag_context
        )
        print(f"  Agent 2 done -> {len(remediation_result.get('suggestions', []))} suggestion(s)")

        quality_score_result = compute_quality_scores_for_issue(issue, issues, TOTAL_ROWS)
        print(f"  Quality score -> before={quality_score_result['before']}, after={quality_score_result['after']}\n")

        new_results.append(
            build_output_record(issue, diagnosis_result, remediation_result, quality_score_result)
        )

    # ── Merge with existing results and save ──────────────────────────────────
    # In streaming mode, append new results to what's already in the file.
    # In batch mode, replace everything.
    if issues_override is not None:
        combined_results = existing_results + new_results
    else:
        combined_results = new_results

    combined_results.sort(key=lambda x: x["diagnosis"]["priority_score"], reverse=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(combined_results, f, indent=2, ensure_ascii=False)

    print(f"[OK] issues_with_suggestions.json saved ({len(combined_results)} total record(s))")
    return combined_results


if __name__ == "__main__":
    run_suggester()