# Provenance of DataGenie v1 (arm A3-v1)

The files in `frozen/` are copied byte for byte from the Team 6 course repository at commit `b01ef2e` (2026-04-26, "Add instruction"), the state the course project was submitted in. **Do not edit them.** Arm A3-v1 runs them unchanged (spec §6.3). The team repository itself is not modified.

| File here | Source path at `b01ef2e` | SHA-256 |
|---|---|---|
| `frozen/suggester_dashboard.py` | `src/llm/suggester_dashboard.py` | `0e4329a410ae66b032c4384c4243dff2f1ec015cc1efdae575ef4e9e1f1dd014` |
| `frozen/historical_decisions.json` | `data/historical_decisions.json` | `4963ed8d7e7d8736cef6d6d25f87c084a659c3bd40c86227b017cfc6f3762018` |

## Authorship

DataGenie v1 is the work of Team 6: Sean Cabaniss, Yung Hsuan Hsieh, Ching-Fen Hung, Yonghui Kim, and Omkar Thombare.

- **`suggester_dashboard.py`** is the dashboard copy of the team's LLM pipeline, `src/llm/suggester.py`. That pipeline was built by Yung Hsuan Hsieh (Tom) and Ching-Fen Hung. The dashboard copy was created and adapted by Sean Cabaniss and Omkar Thombare.
- **`historical_decisions.json`** holds 7 decisions logged through the team's dashboard. All of them are for the "Null Spike" issue type.

## How A3-v1 runs it

For each alert, `dqeval/arms/v1/__init__.py` does the following:
1. It lays out a temporary directory exactly as `run_suggester()` expects: `src/llm/suggester_dashboard.py`, `data/issues_output.json`, `data/df_shape.json`, and `data/historical_decisions.json`.
2. It turns the alert into the issue format of v1's own detector. That is the `column`, `issue_type`, `severity`, `detail`, and `sample_values` fields, plus the `df_shape` that v1 requires.
3. It calls v1's own `run_suggester()`.
4. It takes **option 1** of the result as v1's candidate.

The only instrumentation is a wrapper around the OpenAI client that counts calls and tokens and records the model snapshot. Prompts, sampling settings, the critic, and the historical-decision context are left unchanged.
