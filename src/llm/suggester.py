"""
suggester.py
Two-agent LLM pipeline for data quality remediation suggestions.

Flow (per issue):
  Agent 1  → root cause + business impact
  Agent 2  → two remediation options with PySpark fix code
  Critic   → static check (format, syntax, assigns df); on failure, feedback goes
             back to Agent 2 (max 2 retries). The final pass/fail is recorded.

Called by detector.py with only the newly detected issues:
  run_suggester(new_issues)
Results are appended to data/issues_with_suggestions.json.
"""

import ast
import json
import os
from pathlib import Path
from typing import Any

import openai
from dotenv import load_dotenv

# suggester.py is at src/llm/, so go up 3 levels to reach project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH  = PROJECT_ROOT / "data" / "issues_with_suggestions.json"
MODEL        = "gpt-4o-mini"
NUM_OPTIONS  = 2


# =========================
# Priority scoring
# =========================
def compute_affected_rows_percent(issue: dict[str, Any]) -> float | None:
    affected = issue.get("affected_rows")
    total    = issue.get("total_rows")
    if affected is None or not total:
        return None
    return round(affected / total * 100, 2)


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


def evidence_block(issue: dict[str, Any]) -> str:
    """Formats the detector's batch evidence as a prompt-ready bullet list."""
    evidence = issue.get("evidence") or []
    return "\n".join(f"- {e}" for e in evidence) or "- None available"


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

Evidence computed from the batch:
{evidence_block(issue)}

Instructions:
- The evidence lists observations, not conclusions. State the most likely root cause
  as a hypothesis and name the observation(s) that support it.
- Do NOT simply restate the issue type.
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
        model=MODEL,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a precise data quality diagnosis agent. Return valid JSON only."},
            {"role": "user",   "content": prompt},
        ],
    )
    return json.loads(response.choices[0].message.content)


# =========================
# Agent 2: Remediation
# =========================
def generate_remediation(
    client,
    issue:           dict[str, Any],
    diagnosis:       dict[str, Any],
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
informed by the issue, the batch evidence, and the Agent 1 diagnosis.

Original issue:
Column: {issue['column']}
Issue Type: {issue['issue_type']}
Severity: {issue['severity']}
Detail: {issue['detail']}
Sample Values: {issue['sample_values']}

Evidence computed from the batch:
{evidence_block(issue)}

Agent 1 diagnosis:
Root Cause: {diagnosis['root_cause']}
Business Impact: {diagnosis['business_impact']}
Priority Score: {diagnosis['priority_score']}
Affected Rows Percent: {diagnosis['affected_rows_percent']}
{critic_block}
Requirements:
1. Generate exactly {NUM_OPTIONS} remediation suggestions, best option first.
2. Each suggestion must include: option, action, rationale, caveats, pyspark_code.
3. Let the evidence drive the fix: prefer recovering true values (e.g. from a matching column)
   over generic statistical imputation.
4. If the true values cannot be recovered from the data, set them to null rather than
   inventing, imputing, or reformatting them.

Field rules:
- option: integer (1 or 2)
- action: short remediation title
- rationale: 1-2 concise sentences
- caveats: 1 concise sentence
- pyspark_code: runnable PySpark code, including every import it uses, that reassigns
  a DataFrame named df (not pseudo-code)

Return JSON only, exactly:
{{
  "suggestions": [
    {{"option": 1, "action": "string", "rationale": "string", "caveats": "string", "pyspark_code": "string"}},
    {{"option": 2, "action": "string", "rationale": "string", "caveats": "string", "pyspark_code": "string"}}
  ]
}}
"""
    response = client.chat.completions.create(
        model=MODEL,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are a PySpark remediation agent. Return valid JSON only."},
            {"role": "user",   "content": prompt},
        ],
    )
    return json.loads(response.choices[0].message.content)


# =========================
# Critic
# =========================
def critic_check(remediation: dict[str, Any]) -> tuple[bool, str]:
    suggestions = remediation.get("suggestions", [])
    if len(suggestions) != NUM_OPTIONS:
        return False, f"Expected {NUM_OPTIONS} suggestions, got {len(suggestions)}"
    for s in suggestions:
        num  = s.get("option", "?")
        code = str(s.get("pyspark_code") or "")
        if not code.strip():
            return False, f"Option {num} missing pyspark_code"
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return False, f"Option {num} pyspark_code has a syntax error: {e.msg} (line {e.lineno})"
        assigns_df = any(
            isinstance(node, ast.Name) and node.id == "df" and isinstance(node.ctx, ast.Store)
            for node in ast.walk(tree)
        )
        if not assigns_df:
            return False, f"Option {num} pyspark_code never assigns its result to df"
    return True, "OK"


def generate_remediation_with_critic(
    client, issue, diagnosis, max_retries=2
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Returns (remediation, critic_status); the status records whether the final output passed."""
    feedback = ""
    for attempt in range(max_retries + 1):
        result = generate_remediation(client, issue, diagnosis, feedback)
        for idx, s in enumerate(result.get("suggestions", []), start=1):
            s["option"] = s.get("option", idx)
        passed, feedback = critic_check(result)
        if passed:
            if attempt > 0:
                print(f"  Critic passed on attempt {attempt + 1}")
            return result, {"passed": True, "attempts": attempt + 1, "feedback": feedback}
        print(f"  Critic failed (attempt {attempt + 1}): {feedback}")
    print("  Critic: max retries reached, returning last result marked as failed")
    return result, {"passed": False, "attempts": max_retries + 1, "feedback": feedback}


# =========================
# Main pipeline
# =========================
def run_suggester(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Runs the agent pipeline on `issues` and appends the results to OUTPUT_PATH."""
    load_dotenv(PROJECT_ROOT / ".env")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")

    client = openai.OpenAI(api_key=api_key)

    print(f"[AI] Running agent pipeline on {len(issues)} issue(s)...\n")

    new_results = []
    for issue in issues:
        print(f"Processing: [{issue['severity']}] {issue['column']} — {issue['issue_type']}")

        # Agent 1: root cause diagnosis
        root_cause_result = diagnose_root_cause(client, issue)
        affected_pct      = compute_affected_rows_percent(issue)
        priority_score    = compute_priority_score(issue.get("severity", ""), affected_pct)

        diagnosis_result = {
            "root_cause":            root_cause_result.get("root_cause"),
            "business_impact":       root_cause_result.get("business_impact"),
            "priority_score":        priority_score,
            "affected_rows_percent": affected_pct,
        }
        print(f"  Agent 1 done -> priority={priority_score}, affected={affected_pct}%")

        # Agent 2 + Critic: remediation suggestions
        remediation_result, critic_status = generate_remediation_with_critic(
            client, issue, diagnosis_result
        )
        print(f"  Agent 2 done -> {len(remediation_result.get('suggestions', []))} suggestion(s)\n")

        new_results.append({
            "input":       issue,
            "diagnosis":   diagnosis_result,
            "remediation": {
                "suggestions": remediation_result.get("suggestions", []),
                "critic":      critic_status,
            },
        })

    # ── Append to existing results and save ───────────────────────────────────
    existing_results = []
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            try:
                existing_results = json.load(f)
            except json.JSONDecodeError:
                existing_results = []

    combined_results = existing_results + new_results
    combined_results.sort(key=lambda x: x["diagnosis"]["priority_score"], reverse=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(combined_results, f, indent=2, ensure_ascii=False)

    print(f"[OK] issues_with_suggestions.json saved ({len(combined_results)} total record(s))")
    return combined_results
