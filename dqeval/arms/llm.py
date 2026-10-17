"""
llm.py
Arm A3-sql: one structured LLM call per alert (spec §6.2, scope as reduced in the
changelog). The model gets the facts summary and returns a diagnosis plus one
rule candidate in the contract format. An invalid candidate is sent back with
the contract problems, up to MAX_RETRIES times. No tools.
"""

import json

import openai
from dotenv import load_dotenv

from .. import contract, data
from ..llm_usage import UsageTracker, tracking
from . import Arm, Case, Proposal
from .facts import describe

MODEL       = "gpt-4o-mini"
MAX_RETRIES = 2

SYSTEM = ("You are a data-quality engineer on call for a loan data pipeline. You receive facts about one alert "
          "and propose exactly one rule candidate. Reply with JSON only.")

INSTRUCTIONS = """Propose one rule candidate as JSON:
{{"diagnosis": "...", "decision": "RESTORE | STANDARDIZE | QUARANTINE | KEEP", "column": "{column}",
  "scope": "SQL boolean over one row, null for KEEP", "new_value": "SQL expression, only for RESTORE or STANDARDIZE",
  "rationale": "..."}}

Decisions:
- RESTORE: the true values can be rebuilt from other columns of the same row.
- STANDARDIZE: same meaning, different encoding; rewrite to the encoding the baseline uses.
- QUARANTINE: the values are wrong and cannot be rebuilt; the rows are flagged and the values are kept.
- KEEP: the data is real, only unusual; change nothing.

Rules:
- scope and new_value are DuckDB SQL expressions over a single row: no subqueries, aggregates, window
  functions, or tables. If you need a number from the data, such as a threshold, write it as a constant.
- Columns you may use: {columns}.
- Touch only the rows that are wrong; every other row must stay exactly as it is.
- If another column can rebuild the values (see "Baseline columns equal to it", or a value derivable
  from another column), RESTORE from it instead of quarantining.
- If the values look like real data that is merely unusual, spread over every feed, choose KEEP.
- When rewriting values, use exactly the encoding the baseline uses, including spaces and capitalization.
"""


def ask(client, model: str, messages: list[dict]) -> str:
    response = client.chat.completions.create(model=model, response_format={"type": "json_object"}, messages=messages)
    return response.choices[0].message.content


class LLMPipeline(Arm):
    """A3-sql: one call with the facts, contract output, retries on invalid candidates."""
    name     = "A3-sql"
    uses_llm = True

    def __init__(self, model: str = MODEL):
        load_dotenv(data.PROJECT_ROOT / ".env")
        self.model = model

    def propose(self, case: Case) -> Proposal:
        tracker  = UsageTracker()
        messages = [{"role": "system", "content": SYSTEM},
                    {"role": "user", "content": describe(case) + "\n\n" + INSTRUCTIONS.format(
                        column=case.column, columns=", ".join(data.VISIBLE_COLUMNS))}]
        candidate, problems = None, ["no answer"]
        with tracking(tracker):
            client = openai.OpenAI()
            for _ in range(MAX_RETRIES + 1):
                text = ask(client, self.model, messages)
                try:
                    candidate = contract.parse(text)
                    problems  = contract.validate(candidate, case.column)
                except (ValueError, TypeError, AttributeError) as e:
                    candidate, problems = None, [f"the reply is not a JSON object: {e}"]
                if not problems:
                    break
                messages += [{"role": "assistant", "content": text},
                             {"role": "user", "content": "That candidate is invalid: " + "; ".join(problems)
                                                         + ". Reply with the corrected JSON only."}]
        return Proposal(candidate, error=None if candidate else "; ".join(problems),
                        llm_calls=tracker.calls, input_tokens=tracker.input_tokens,
                        output_tokens=tracker.output_tokens,
                        trace=[{"models": sorted(m for m in tracker.models if m), "exchanges": tracker.exchanges}])
