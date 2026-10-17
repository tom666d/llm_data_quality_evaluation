# Evaluation Spec

This spec fixes the method before any held-out result exists. The original version was frozen at commit `4eadf71`. This text describes the method as it is now run; every change since the freeze is listed in the changelog at the end. The goal is a working evaluation that produces results, not exhaustive rigor.

---

## 1. Questions

- **Q0.** How well does the original course system (DataGenie v1, as submitted) handle data-quality incidents?
- **Q1.** On mechanisms the rules were built for, do deterministic rules do at least as well as the LLM methods?
- **Q2.** On mechanisms the rules were not built for, does a tool-using LLM agent do better than the rules' fallback and than a fixed-prompt LLM?
- **Q3** *(optional, milestone 2).* Does a multi-agent setup beat the single agent when both have the same tools and budget?

## 2. Scenario

An on-call data engineer receives an alert about a window of loan data. Each method does the engineer's job:

1. scope the affected records,
2. decide whether the values can be recovered,
3. propose a rule candidate that a human can review.

Nothing is applied without human approval, which is out of scope here.

## 3. Data

- **Reference.** `data/LendingClub_100k.csv` is the course dataset before injection. It comes from the team repository at commit `b01ef2e`, and its SHA-256 is checked on load. It is a *pre-injection reference*, not verified business truth.
- **Synthetic fields.** These are deterministic from `id`:
  - `source_system`: feed A to D, from `id % 4`;
  - `ingested_at`: row *r* arrives 3*r* seconds after `2026-01-05 00:00`.
- **Windows.** 2,000 consecutive rows each.
- **Splits.**
  - The dev set uses rows 1 to 50,000.
  - Held-out set S1 uses rows 50,001 to 75,000.
  - Held-out set S2 uses rows 75,001 to 100,000.
- **Provenance.** The original system's code and the reference data come from the team repository at `b01ef2e`. Team 6 is credited in `dqeval/arms/v1/PROVENANCE.md`. The team repository is not modified.

## 4. Alerts

Generic monitors compare a window with the 5 nearest clean windows of its split:

| Monitor | Fires when, per column |
|---|---|
| Null rate | rises by at least 10 percentage points |
| Numeric shift | p5, median, or p95 changes by at least 3× |
| New categories | unseen values make up at least 5% of rows |
| String pattern | a character-class pattern's share changes by at least 10 pp |

An alert names the column, the monitor, and the before and after values. It never says what the mechanism is, what the scope is, or which rows are affected. Detection quality is out of scope: every incident must cause an alert that does not fire on the same window without it.

## 5. Incidents

| Case type | What happened | Correct handling |
|---|---|---|
| Recoverable | Values are wrong but can be rebuilt from visible columns | RESTORE |
| Representation-only | Same meaning, different encoding | STANDARDIZE |
| Unrecoverable | Values are wrong and cannot be rebuilt | QUARANTINE (flag the rows, keep the values) |
| Normal but unusual | Nothing was injected; the data is just rare | KEEP |

- **Format.** An incident is JSON data:
  - one window and one target column;
  - a case type and a mechanism family;
  - a corruption, written as SQL scope and value expressions;
  - proofs: a recovery expression for recoverable incidents, equivalence and canonical expressions for representation incidents;
  - counterexamples: clean look-alike rows that a rule scoped too broadly would wrongly change.
- **Checker.** `dqeval/check_incidents.py` verifies each incident: its format, its proofs, that it causes an alert, and that it has at least 10 counterexamples.
- **Dev set.** `incident_sets/dev.json` has 12 incidents and is visible. Every method may be tuned on it.
- **Held-out sets S1 and S2.** Each has 20 incidents. Codex wrote them from `docs/HELDOUT_BRIEF.md` before any rule or agent existed. They are stored outside the repository, and their hashes are in `docs/HELDOUT_HASHES.md`.
  - **Covered:** at least 10 incidents are variants of the three course-project mechanisms (null, sentinel, overwrite).
  - **Uncovered:** the rest use other mechanisms.

## 6. Methods under test

Every method except the original system returns one **rule candidate**. A rule candidate has these fields:
- a decision: RESTORE, STANDARDIZE, QUARANTINE, or KEEP;
- the alerted column;
- `scope`, a SQL boolean over one row;
- `new_value`, a SQL expression over one row.

The expressions are checked against an allowlist: no subqueries, aggregates, window functions, tables, or hidden columns. They run in DuckDB with file access disabled. This format replaced free-form PySpark because generated PySpark often crashed or edited unrelated cells.

| Method | Code | What it is |
|---|---|---|
| No change *(control)* | A0 | Always KEEP |
| Quarantine all *(control)* | A1 | Quarantine the whole alert window |
| Rules | A2 | A deterministic rule library built on the dev set. It restores from a column the target derives from, quarantines dominant placeholders, rescales a feed by a unit factor, re-encodes dates or categories, and narrows scope to feeds and time. Anything it cannot explain falls back to A1 |
| Course system (v1) | A3-v1 | The submitted code, run unchanged through an adapter that speaks its detector's format. Its option-1 PySpark runs in a subprocess with no secrets, a 120-second timeout, and a fixed preamble of common imports, so a fix is judged on what it does, not on a missing import |
| LLM, fixed prompt | A3-sql | One call with a facts summary returns a diagnosis and a rule candidate, with up to 2 retries when the format is invalid |
| LLM agent with tools | A4 | The same facts plus three tools: `run_sql` (read-only), `dry_run` (what a candidate would change in the window and in the baseline), and `submit`. Budget: 20 tool calls, 60,000 tokens, 300 seconds |
| Multi-agent *(optional)* | A5 | The same tools and budget as A4 |

- **Model.** All LLM methods use `gpt-4o-mini` with default sampling. The exact snapshot is recorded.
- **Runs.** Each LLM method runs 5 times per incident; deterministic methods run once.
- **No leakage.** No method sees ground truth, the case type, the injection, or the counterexamples.

## 7. Scoring

The unit is the incident. A run succeeds only if all three levels hold:

1. **Executes.** A valid candidate, or runnable code, produces outputs. Every failure counts.
2. **Fixes the window.** At least 95% of the injected cells are handled as the case type requires: restored to the reference value, standardized to the canonical encoding, or quarantined with their values kept. For a normal window, nothing changes.

   No clean data is harmed. Harm means a changed clean cell, an edited other column, or a dropped clean row. At most 1% of clean rows may be quarantined.
3. **Safe to reuse.** Applied to the counterexamples and to 5,000 clean rows from the windows after the incident's window, the rule harms nothing, and it quarantines at most 1% of each set.

**Reported.**
- Success rate per method: the mean over incidents of the successes out of runs.
- Success by case type, and by covered vs. uncovered.
- The first reason each failed run failed: did not execute, harmed clean data, not handled, or unsafe to reuse.
- Cost: calls, tokens, and seconds.
- Successes whose scope reads `ingested_at`, shown separately. Such a rule is a one-off fix for a time range, and level 3 says little about it.

**Not measured.** Human review time and detection quality.

## 8. Freeze and milestones

1. Methods are tuned on the dev set only.
2. Tag `eval-freeze-m1`, then run S1 once with A0 to A4. The harness refuses to run S1 or S2 unless HEAD carries the tag and the working tree is clean.
3. *(Optional.)* Build A5 on the dev set, tag `eval-freeze-m2`, then run S2 with A4 and A5.

Any later change produces a new, separately reported version.

## 9. Adoption decision (set before any results)

- **Rules vs. LLM.** If the rules succeed on at least as many held-out incidents as the best LLM method, and harm no more clean data, rules stay primary. The LLM is then used only for alerts that no rule explains, or only for explanations. If instead an LLM method wins, it is used only for the incident types it won.
- **Multi-agent vs. single agent** *(if run).* A5 is adopted only if it succeeds on more S2 incidents than A4, with no more harm.

## 10. Exploratory result (before this spec)

Three saved LLM runs and three hand-written rules were compared on the first 500 rows of the course demo data. The rules handled all three anomalies. The LLM fully succeeded in 1 of 9 outcomes, and 4 crashed. The rules were written after those failures had been seen, so this result motivated the project but is not evidence for it. The comparison is preserved in `dqeval/exploratory/`.

## 11. Limits

- **Synthetic incidents.** The held-out author is also an LLM.
- **Small scale.** There are about 20 incidents and one model, so the results describe this setup only.
- **The rules were built on the dev set,** so covered incidents favor them. The uncovered split is there to show how much.
- **The adapter may disadvantage v1** on alerts outside its three original issue types.
- **The contract cannot express** deduplication or cross-table fixes.

## Changelog

All changes below were made on 2026-10-17, after freeze point 1 and before any held-out result existed.
- **Monitors.** The numeric monitor also checks p5, because downward shifts on one feed were invisible. An incident's alert must be one the incident causes.
- **Incidents.**
  - Each incident corrupts exactly one column.
  - Covered vs. uncovered is defined by mechanism family, because the dev set did not exist yet when the held-out sets were written.
  - The regression slice comes from the windows after the incident's own window, because each held-out split has only 12 to 13 windows.
- **Scoring clarifications.**
  - The 1% quarantine allowance in level 3 applies to each set separately.
  - Representation outputs are compared using the incident's canonical and equivalence expressions under DuckDB `TRY()`.
- **Reporting.** Records carry whether the decision matches the case type and whether the scope uses time. Time-scoped successes are reported separately.
- **Scope reduction.**
  - Dropped: the format and evidence ablation arms (A3-py, A3-sql-noev), the LLM-judge meta-evaluation, threshold sensitivity, and manual failure labels. Failure reasons are derived automatically.
  - A3-sql became one structured call.
  - A4 has no separate profiler tool, because `run_sql` covers profiling.
  - Code methods run after a fixed import preamble.
  - A5 is optional.
- **This text.** Rewritten to describe only the current method. The previous text is in git history.
