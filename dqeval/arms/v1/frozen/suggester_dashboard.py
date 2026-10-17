import json
import os
import re
from pathlib import Path
from typing import Any

import openai
from dotenv import load_dotenv


# =========================
# Helper functions
# =========================
def load_df_shape(path: Path) -> dict[str, int]:
    """
    Load dataframe shape metadata from JSON.
    """
    if not path.exists():
        raise FileNotFoundError(f"df_shape file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        shape_info = json.load(f)

    total_rows = shape_info.get("total_rows")
    total_columns = shape_info.get("total_columns")

    if not isinstance(total_rows, int) or total_rows <= 0:
        raise ValueError("df_shape.json must contain a positive integer 'total_rows'")

    if not isinstance(total_columns, int) or total_columns <= 0:
        raise ValueError("df_shape.json must contain a positive integer 'total_columns'")

    return {
        "total_rows": total_rows,
        "total_columns": total_columns
    }


def extract_affected_rows(detail: str) -> int | None:
    if not detail:
        return None

    match_fraction = re.search(r"\((\d+)\s*/\s*(\d+)\s*rows\)", detail)
    if match_fraction:
        return int(match_fraction.group(1))

    match_outlier = re.search(r"(\d+)\s+outlier\(s\)", detail, re.IGNORECASE)
    if match_outlier:
        return int(match_outlier.group(1))

    row_counts = re.findall(r"(\d+)\s+rows", detail, re.IGNORECASE)
    if row_counts:
        counts = [int(x) for x in row_counts]
        return min(counts)

    return None


def extract_affected_rows_percent(detail: str) -> float | None:
    if not detail:
        return None

    match_percent_of_rows = re.search(r"(\d+(?:\.\d+)?)%\s+of\s+rows", detail, re.IGNORECASE)
    if match_percent_of_rows:
        return float(match_percent_of_rows.group(1))

    match_null_rate = re.search(r"Null rate:\s*(\d+(?:\.\d+)?)%", detail, re.IGNORECASE)
    if match_null_rate:
        return float(match_null_rate.group(1))

    return None


def compute_affected_rows_percent(detail: str, total_rows: int) -> float | None:
    percent_from_detail = extract_affected_rows_percent(detail)
    if percent_from_detail is not None:
        return round(percent_from_detail, 2)

    affected_rows = extract_affected_rows(detail)
    if affected_rows is None or total_rows <= 0:
        return None

    return round((affected_rows / total_rows) * 100, 2)


def severity_to_score(severity: str) -> int:
    severity_map = {
        "LOW": 3,
        "MEDIUM": 6,
        "HIGH": 9,
    }
    return severity_map.get(str(severity).upper(), 5)


def affected_rows_percent_to_score(affected_rows_percent: float | None) -> int:
    if affected_rows_percent is None:
        return 5
    if affected_rows_percent < 5:
        return 1
    if affected_rows_percent < 15:
        return 3
    if affected_rows_percent < 30:
        return 5
    if affected_rows_percent < 50:
        return 7
    return 9


def compute_priority_score(severity: str, affected_rows_percent: float | None) -> int:
    severity_score = severity_to_score(severity)
    affected_score = affected_rows_percent_to_score(affected_rows_percent)

    weighted_score = round(0.6 * severity_score + 0.4 * affected_score)
    weighted_score = max(1, min(10, weighted_score))
    return weighted_score


def compute_total_affected_rows(
    issues_list: list[dict[str, Any]],
    total_rows: int
) -> int:
    total_affected_percent = 0.0

    for issue in issues_list:
        issue_percent = compute_affected_rows_percent(issue.get("detail", ""), total_rows)
        if issue_percent is not None:
            total_affected_percent += issue_percent

    total_affected_percent = min(total_affected_percent, 100.0)
    total_affected_rows = round((total_affected_percent / 100) * total_rows)

    return total_affected_rows


def compute_quality_scores_for_issue(
    current_issue: dict[str, Any],
    issues_list: list[dict[str, Any]],
    total_rows: int
) -> dict[str, float]:
    total_affected_rows_before = compute_total_affected_rows(issues_list, total_rows)

    current_issue_percent = compute_affected_rows_percent(
        current_issue.get("detail", ""),
        total_rows
    ) or 0.0
    current_issue_affected_rows = round((current_issue_percent / 100) * total_rows)

    affected_rows_after = max(0, total_affected_rows_before - current_issue_affected_rows)

    good_rows_before = total_rows - total_affected_rows_before
    good_rows_after = total_rows - affected_rows_after

    before = round((good_rows_before / total_rows) * 100, 2) if total_rows > 0 else 0.0
    after = round((good_rows_after / total_rows) * 100, 2) if total_rows > 0 else 0.0
    delta = round(after - before, 2)

    return {
        "before": before,
        "after": after,
        "delta": delta,
    }


def load_historical_feedback(path: Path) -> dict[str, list[dict[str, Any]]]:
    if not path.exists():
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                return {}
            data = json.loads(content)

            if isinstance(data, list):
                converted = {}
                for record in data:
                    itype = record.get("issue_type", "unknown")
                    converted.setdefault(itype, []).append(record)
                return converted

            if isinstance(data, dict):
                return data

            return {}

    except json.JSONDecodeError:
        return {}


def format_historical_context(issue_type: str, history: dict[str, list[dict[str, Any]]]) -> str:
    records = history.get(issue_type, [])
    if not records:
        return "No historical decisions are available for this issue type."

    lines = []
    for idx, record in enumerate(records[-5:], start=1):
        lines.append(
            f"{idx}. Column: {record.get('column')}; "
            f"Chosen Action: {record.get('chosen_action')}; "
            f"Rationale: {record.get('chosen_rationale')}; "
            f"Caveats: {record.get('chosen_caveats')}; "
            f"Confidence: {record.get('chosen_confidence')}; "
            f"Detail Summary: {record.get('detail_summary')}"
        )

    return "\n".join(lines)


def route_issue(issue: dict[str, Any]) -> str:
    routing_map = {
        "Null Spike": "imputation",
        "Statistical Outlier": "statistical",
        "Format Inconsistency": "standardization",
    }
    return routing_map.get(issue.get("issue_type", ""), "general")


# =========================
# Agent 1: Root Cause only
# =========================
def diagnose_root_cause(client, issue: dict[str, Any]) -> dict[str, Any]:
    prompt = f"""
You are Agent 1 in a two-agent data quality remediation workflow.

Your task is to analyze the issue and return:
- likely root cause
- likely business impact

Issue input:
Column: {issue['column']}
Issue Type: {issue['issue_type']}
Severity: {issue['severity']}
Detail: {issue['detail']}
Sample Values: {issue['sample_values']}

Instructions:
- Explain the likely upstream or operational cause.
- Do NOT simply repeat the issue type.
- Be specific and concise.
- business_impact should explain the likely downstream impact on analytics, reporting, or modeling.
- Keep both fields concise (1-2 short sentences each).
- Return JSON only.

Return exactly this format:
{{
  "root_cause": "string",
  "business_impact": "string"
}}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": "You are a precise data quality diagnosis agent. Return valid JSON only."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    return json.loads(response.choices[0].message.content)


# =========================
# Agent 2: Remediation
# =========================
def generate_remediation(
    client,
    issue: dict[str, Any],
    diagnosis: dict[str, Any],
    historical_context: str,
    critic_feedback: str = ""
) -> dict[str, Any]:

    critic_feedback_block = ""
    if critic_feedback:
        critic_feedback_block = f"""
Previous attempt was rejected by the quality checker:
{critic_feedback}
Please fix the issue and regenerate.
"""

    prompt = f"""
You are Agent 2 in a two-agent data quality remediation workflow.

Your task is to generate remediation suggestions with runnable PySpark fix code,
based on the original issue, Agent 1 diagnosis, and historical remediation preferences.

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

Historical decisions for this issue type:
{historical_context}
{critic_feedback_block}
Requirements:
1. Generate exactly 3 remediation suggestions.
2. Option 1 and Option 2 must be actual remediation actions.
3. Option 3 must always be "Decline changes".
4. Each suggestion must include:
   - option
   - action
   - confidence
   - rationale
   - caveats
   - pyspark_code

Field rules:
- option: integer (1, 2, or 3)
- action: short remediation title
- confidence: integer from 0 to 100
- rationale: one or two concise sentences
- caveats: one concise sentence describing assumptions, limitations, or risks
- pyspark_code:
  - must be runnable PySpark-style code
  - must reference a dataframe named df
  - must be relevant to the issue
  - must not be pseudo-code
  - must be copyable as a code block string

Confidence definition:
- 80–100: highly reliable, standard practice, safe to apply directly
- 60–79: generally valid but may depend on data context
- 40–59: usable but has clear trade-offs or risks
- below 40: weak, risky, or not recommended in most cases

Confidence scoring guidance:
- Give higher confidence to standard, widely used, low-risk fixes
- Give lower confidence if the method may drop rows, distort values, or require strong assumptions
- If the fix is conservative and preserves data safely, confidence should usually be higher
- If the fix may cause data loss, confidence should usually be lower

Important:
- The suggestions must be informed by Agent 1 diagnosis.
- Use historical decisions as preference signals when reasonable.
- If similar issue types were solved before, prefer consistent remediation strategies unless there is a strong reason not to.
- Historical decisions should influence both action selection and confidence scoring.
- Avoid generic advice with no executable action.
- Make the code practical and directly tied to the issue.
- Return JSON only.

Special rule for option 3:
- option 3 must always represent "Decline changes"
- action must be exactly "Decline changes"
- rationale should explain that no remediation will be applied and the decision is still captured for audit purposes
- caveats should explain that the issue remains unresolved
- pyspark_code should be a no-op comment such as "# No change applied to df"
- confidence can be 100 because this is a user decision rather than a technical remediation estimate

Return exactly this format:
{{
  "suggestions": [
    {{
      "option": 1,
      "action": "string",
      "confidence": 85,
      "rationale": "string",
      "caveats": "string",
      "pyspark_code": "string"
    }},
    {{
      "option": 2,
      "action": "string",
      "confidence": 65,
      "rationale": "string",
      "caveats": "string",
      "pyspark_code": "string"
    }},
    {{
      "option": 3,
      "action": "Decline changes",
      "confidence": 100,
      "rationale": "string",
      "caveats": "string",
      "pyspark_code": "# No change applied to df"
    }}
  ]
}}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": "You are a PySpark remediation agent. Return valid JSON only."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
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
        option_num = s.get("option", "?")

        if not s.get("pyspark_code") or s["pyspark_code"].strip() == "":
            return False, f"Option {option_num} missing pyspark_code"
        if s.get("option") != 3 and "df" not in s.get("pyspark_code", ""):
            return False, f"Option {option_num} pyspark_code does not reference 'df'"
        if not isinstance(s.get("confidence"), int):
            return False, f"Option {option_num} confidence must be an integer"

    option3 = next((s for s in suggestions if s.get("option") == 3), None)
    if not option3 or option3.get("action") != "Decline changes":
        return False, "Option 3 must be 'Decline changes'"

    return True, "OK"


def generate_remediation_with_critic(
    client,
    issue: dict[str, Any],
    diagnosis: dict[str, Any],
    historical_context: str,
    max_retries: int = 2
) -> dict[str, Any]:
    feedback = ""

    for attempt in range(max_retries + 1):
        result = generate_remediation(client, issue, diagnosis, historical_context, feedback)

        suggestions = result.get("suggestions", [])
        for idx, s in enumerate(suggestions, start=1):
            s["option"] = s.get("option", idx)

        passed, feedback = critic_check(result)

        if passed:
            if attempt > 0:
                print(f"  Critic passed on attempt {attempt + 1}")
            return result

        print(f"  Critic failed (attempt {attempt + 1}): {feedback}")

    print(f"  Critic: max retries reached, returning last result")
    return result


# =========================
# Build layered output
# =========================
def build_output_record(
    issue: dict[str, Any],
    diagnosis: dict[str, Any],
    remediation: dict[str, Any],
    quality_score: dict[str, float]
) -> dict[str, Any]:
    return {
        "input": {
            "column": issue["column"],
            "issue_type": issue["issue_type"],
            "severity": issue["severity"],
            "detail": issue["detail"],
            "sample_values": issue["sample_values"]
        },
        "diagnosis": {
            "root_cause": diagnosis["root_cause"],
            "business_impact": diagnosis["business_impact"],
            "priority_score": diagnosis["priority_score"],
            "affected_rows_percent": diagnosis["affected_rows_percent"]
        },
        "remediation": {
            "suggestions": remediation.get("suggestions", [])
        },
        "quality_score": quality_score
    }


# =========================
# Main callable pipeline
# =========================
def run_suggester():
    # Path and environment setup
    project_root = Path(__file__).resolve().parents[2]
    load_dotenv(project_root / ".env")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set")

    client = openai.OpenAI(api_key=api_key)

    LOCAL_MODE = not os.path.exists("/dbfs")

    if LOCAL_MODE:
        data_dir = project_root / "data"
    else:
        data_dir = Path("/Volumes/workspace/team6/data")

    input_path = data_dir / "issues_output.json"
    output_path = data_dir / "issues_with_suggestions.json"
    historical_path = data_dir / "historical_decisions.json"
    df_shape_path = data_dir / "df_shape.json"

    if not input_path.exists():
        raise FileNotFoundError(f"issues_output.json not found: {input_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        issues = json.load(f)

    df_shape = load_df_shape(df_shape_path)
    TOTAL_ROWS = df_shape["total_rows"]
    TOTAL_COLUMNS = df_shape["total_columns"]

    print(f"[FILE] Loaded {len(issues)} issue(s) from issues_output.json")
    print(f"[RULE] Loaded dataframe shape: rows={TOTAL_ROWS}, columns={TOTAL_COLUMNS}\n")
    print("[AI] Running Week 2 Agent pipeline...\n")

    historical_feedback = load_historical_feedback(historical_path)
    results = []

    for issue in issues:
        print(f"Processing: [{issue['severity']}] {issue['column']} — {issue['issue_type']}")

        strategy = route_issue(issue)
        print(f"  Router -> strategy: {strategy}")

        root_cause_result = diagnose_root_cause(client, issue)

        affected_rows_percent = compute_affected_rows_percent(issue.get("detail", ""), TOTAL_ROWS)
        priority_score = compute_priority_score(issue.get("severity", ""), affected_rows_percent)

        diagnosis_result = {
            "root_cause": root_cause_result.get("root_cause"),
            "business_impact": root_cause_result.get("business_impact"),
            "priority_score": priority_score,
            "affected_rows_percent": affected_rows_percent
        }

        print(
            f"  Agent 1 done -> priority_score={diagnosis_result['priority_score']}, "
            f"affected_rows_percent={diagnosis_result['affected_rows_percent']}"
        )

        historical_context = format_historical_context(issue["issue_type"], historical_feedback)

        remediation_result = generate_remediation_with_critic(
            client,
            issue,
            diagnosis_result,
            historical_context
        )
        print(f"  Agent 2 done -> generated {len(remediation_result.get('suggestions', []))} suggestion(s)")

        quality_score_result = compute_quality_scores_for_issue(
            issue,
            issues,
            TOTAL_ROWS
        )
        print(
            f"  Quality score -> before={quality_score_result['before']}, "
            f"after={quality_score_result['after']}, "
            f"delta={quality_score_result['delta']}"
        )

        output_record = build_output_record(
            issue=issue,
            diagnosis=diagnosis_result,
            remediation=remediation_result,
            quality_score=quality_score_result
        )
        results.append(output_record)

        print()

    results = sorted(
        results,
        key=lambda x: x["diagnosis"]["priority_score"],
        reverse=True
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print("[OK] issues_with_suggestions.json saved")
    return results


if __name__ == "__main__":
    run_suggester()