# Exploratory comparison (before the spec)

This comparison is why the project became an evaluation. It is preserved so that anyone can reproduce it. It is **not** part of the preregistered evaluation in `docs/EVAL_SPEC.md`: the rules were written after the LLM failures had been seen, and the sample is tiny.

## Setup

- **Data.** The first 500 rows of the course demo data, which has three anomalies already injected: a null spike in `loan_amnt`, the sentinel value `9999999` in `annual_inc`, and `issue_d` overwritten with `01/2020`. Each cell is scored against the same rows before injection.
- **Rules.** Three functions in `rules_vs_llm.py`. Each decides only from facts that the prototype's evidence step also computes.
- **LLM.** Three saved runs of the prototype's Agent 1 → Agent 2 pipeline (`gpt-4o-mini`, one run each). Only option 1 is scored.

| Run | Prompt version (commit) | What changed |
|---|---|---|
| A | `3fbd0f9` | The evidence included interpretations, e.g. "suggests a sentinel" |
| B | `3cc97d2` | The evidence states observations only; Agent 2 is told to null unrecoverable values |
| C | `874eaf5` | Agent 2 is told to flag unrecoverable rows instead of nulling them |

## Result

| | `loan_amnt` (196 bad, recoverable) | `annual_inc` (46 bad, unrecoverable) | `issue_d` (139 bad, unrecoverable) |
|---|---|---|---|
| Rules | 196 restored | 46 flagged | 139 flagged |
| LLM run A | crash | crash | 139 overwritten with wrong values |
| LLM run B | 196 restored | 46 nulled, plus 5 correct cells changed | 139 nulled |
| LLM run C | crash | 46 flagged, plus 5 correct cells flagged | crash |

The rules handled all three issues with no harm. The LLM fully succeeded in 1 of 9 outcomes, and 4 crashed on PySpark API misuse.

## Caveats

- The rules were written after the failures had been seen, and with knowledge of the three mechanisms.
- There are 3 runs, 500 rows, and 3 known mechanisms. That is enough to motivate a redesign, not to support a general claim.
- "Fabricated" in the script output counts any overwrite with a wrong value, including null.

## Reproduce

Both CSVs come from the team repository at commit `b01ef2e`:

| File | SHA-256 |
|---|---|
| `data/demo_lendingclub.csv` | `6f5bf74fd3330199ffe8dde5e78b62f57db3a08109e77ac3d4048fc29908bb85` |
| `data/LendingClub_100k.csv` | `c72a9cbf265557db58fd5e64fadb5c4a1ee5e656e87c87605b35ca1b1ca270b5` |

```bash
git -C <team-repo> show b01ef2e:data/demo_lendingclub.csv > data/demo_lendingclub.csv
git -C <team-repo> show b01ef2e:data/LendingClub_100k.csv > data/LendingClub_100k.csv
.venv/bin/python dqeval/exploratory/rules_vs_llm.py
```

No API calls are made; the LLM outputs are read from `runs/`.
