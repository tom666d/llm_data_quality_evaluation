"""
agent.py
Arm A4: a single tool-using agent (spec §6.2). It starts from the same facts and
guidance as A3-sql, then investigates with tools before it submits one rule
candidate.

Tools:
  run_sql   read-only SELECT over the tables `alert_window` (the alerted window) and
            `baseline` (clean nearby windows); visible columns only
  dry_run   applies a candidate to the window and to the baseline and reports how
            many values it would change and rows it would quarantine, by feed,
            with sample rows; no ground truth is involved
  submit    the final answer; an invalid candidate is sent back with its problems

Budget per incident (spec §6.4): MAX_TOOL_CALLS tool calls, MAX_TOKENS tokens,
MAX_SECONDS seconds. If it runs out before a valid submission, the run fails.
"""

import json
import time

import openai
import pandas as pd
import sqlglot
from dotenv import load_dotenv
from sqlglot import exp

from .. import contract, data
from ..contract import QUARANTINE_COLUMN, RuleCandidate
from ..incidents import connect
from ..llm_usage import UsageTracker, tracking
from . import Arm, Case, Proposal
from .facts import describe
from .llm import INSTRUCTIONS, MODEL

MAX_TOOL_CALLS = 20
MAX_TOKENS     = 60_000
MAX_SECONDS    = 300
MAX_TOOL_CHARS = 3_000     # tool results are truncated to keep the context small

SYSTEM = ("You are a data-quality engineer on call for a loan data pipeline. Investigate one alert with the "
          "tools, then submit exactly one rule candidate with the submit tool.")

AGENT_NOTE = """You have tools:
- run_sql: a read-only SELECT over the tables `alert_window` (the alerted window) and `baseline` (clean nearby windows).
- dry_run: shows what a candidate would change in the window and in the baseline. A candidate that touches
  baseline rows would also touch clean data later.
- submit: your final answer, with the fields above.
Investigate before you submit, and dry-run your candidate. You have at most {calls} tool calls."""

DECISION_FIELDS = {
    "decision":  {"type": "string", "enum": list(contract.DECISIONS)},
    "scope":     {"type": ["string", "null"], "description": "SQL boolean over one row; null for KEEP"},
    "new_value": {"type": ["string", "null"], "description": "SQL expression; only for RESTORE or STANDARDIZE"},
}
TOOLS = [
    {"type": "function", "function": {
        "name": "run_sql", "description": "Run one read-only SELECT over the tables alert_window and baseline.",
        "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}}},
    {"type": "function", "function": {
        "name": "dry_run", "description": "Show what a rule candidate would change, without submitting it.",
        "parameters": {"type": "object", "properties": DECISION_FIELDS, "required": ["decision"]}}},
    {"type": "function", "function": {
        "name": "submit", "description": "Submit the final rule candidate.",
        "parameters": {"type": "object", "required": ["decision", "rationale"],
                       "properties": {**DECISION_FIELDS, "rationale": {"type": "string"}}}}},
]


# ── Tools ─────────────────────────────────────────────────────────────────────
def run_sql(case: Case, query: str) -> str:
    try:
        statements = sqlglot.parse(query, read="duckdb")
    except sqlglot.errors.ParseError as e:
        return f"error: the query does not parse: {str(e).splitlines()[0]}"
    if len(statements) != 1 or not isinstance(statements[0], (exp.Select, exp.Union)):
        return "error: only a single SELECT is allowed"
    con = connect()
    con.register("alert_window", case.window)   # "window" is reserved in DuckDB
    con.register("baseline", case.baseline)
    try:
        result = con.execute(query).df()
    except Exception as e:
        return f"error: {type(e).__name__}: {str(e).splitlines()[0]}"
    text = result.head(20).to_csv(index=False)
    return (text[:MAX_TOOL_CHARS] + f"\n({len(result)} rows)").strip()


def _changed(before: pd.Series, after: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(before) and pd.api.types.is_numeric_dtype(after):
        same = (before - after).abs() <= 0.01
    else:
        same = before.astype(str) == after.astype(str)
    return ~(same | (before.isna() & after.isna()))


def dry_run(case: Case, args: dict) -> str:
    candidate = RuleCandidate(args.get("decision", ""), case.column, args.get("scope"), args.get("new_value"))
    problems  = contract.validate(candidate, case.column)
    if problems:
        return "invalid candidate: " + "; ".join(problems)
    lines, col = [], case.column
    for name, table in (("window", case.window), ("baseline", case.baseline)):
        try:
            out = contract.apply(table, candidate)
        except Exception as e:
            return f"error applying it to {name}: {type(e).__name__}: {str(e).splitlines()[0]}"
        touched = _changed(table[col].reset_index(drop=True), out[col]) | out[QUARANTINE_COLUMN]
        by_feed = out.loc[touched, "source_system"].value_counts().to_dict()
        lines.append(f"{name}: {int(_changed(table[col].reset_index(drop=True), out[col]).sum())} values changed, "
                     f"{int(out[QUARANTINE_COLUMN].sum())} rows quarantined, of {len(table)} rows; by feed {by_feed}")
        if name == "window" and touched.any():
            sample = pd.DataFrame({"id": table["id"].values, "source_system": table["source_system"].values,
                                   "before": table[col].values, "after": out[col].values,
                                   "quarantined": out[QUARANTINE_COLUMN].values})[touched.values].head(5)
            lines.append(sample.to_csv(index=False).strip())
    return "\n".join(lines)[:MAX_TOOL_CHARS]


# ── The loop ──────────────────────────────────────────────────────────────────
class Agent(Arm):
    """A4: one agent with run_sql, dry_run, and submit, under a fixed budget."""
    name     = "A4"
    uses_llm = True

    def __init__(self, model: str = MODEL):
        load_dotenv(data.PROJECT_ROOT / ".env")
        self.model = model

    def propose(self, case: Case) -> Proposal:
        tracker, started, tool_calls = UsageTracker(), time.time(), 0
        messages = [{"role": "system", "content": SYSTEM},
                    {"role": "user", "content": describe(case) + "\n\n"
                     + INSTRUCTIONS.format(column=case.column, columns=", ".join(data.VISIBLE_COLUMNS)) + "\n"
                     + AGENT_NOTE.format(calls=MAX_TOOL_CALLS)}]
        candidate, error = None, "budget exhausted before a valid submission"

        with tracking(tracker):
            client = openai.OpenAI()
            while (tool_calls < MAX_TOOL_CALLS and tracker.input_tokens + tracker.output_tokens < MAX_TOKENS
                   and time.time() - started < MAX_SECONDS):
                message = client.chat.completions.create(model=self.model, messages=messages,
                                                         tools=TOOLS).choices[0].message
                calls = message.tool_calls or []
                messages.append({"role": "assistant", "content": message.content, "tool_calls": [
                    {"id": c.id, "type": "function", "function": {"name": c.function.name,
                                                                  "arguments": c.function.arguments}}
                    for c in calls]} if calls else {"role": "assistant", "content": message.content})
                if not calls:
                    messages.append({"role": "user", "content": "Use the tools; finish with the submit tool."})
                    continue
                for call in calls:
                    tool_calls += 1
                    try:
                        args = json.loads(call.function.arguments or "{}")
                    except json.JSONDecodeError:
                        args = {}
                    if call.function.name == "submit":
                        submitted = RuleCandidate(args.get("decision", ""), case.column, args.get("scope"),
                                                  args.get("new_value"), args.get("rationale", ""))
                        problems  = contract.validate(submitted, case.column)
                        result    = "accepted" if not problems else "invalid: " + "; ".join(problems)
                        if not problems:
                            candidate, error = submitted, None
                    elif call.function.name == "run_sql":
                        result = run_sql(case, args.get("query", ""))
                    elif call.function.name == "dry_run":
                        result = dry_run(case, args)
                    else:
                        result = f"error: unknown tool {call.function.name}"
                    messages.append({"role": "tool", "tool_call_id": call.id, "content": result})
                if candidate:
                    break

        return Proposal(candidate, error=error, llm_calls=tracker.calls, input_tokens=tracker.input_tokens,
                        output_tokens=tracker.output_tokens,
                        trace=[{"models": sorted(m for m in tracker.models if m), "tool_calls": tool_calls,
                                "seconds": round(time.time() - started, 1), "messages": messages[2:]}])
