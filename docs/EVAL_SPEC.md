# Evaluation Spec

**Status: DRAFT, not yet frozen.** This spec is committed before any rule library, agent, or held-out incident exists. Changes after the freeze go in the changelog at the end. Any results they affect are reported as a separate version.

---

## 1. Questions

- **Q0.** How well does the original course system (DataGenie v1, as submitted) actually handle data-quality incidents?
- **Q1.** On anomaly types the rules cover, do deterministic rules handle incidents at least as well as LLM pipelines? Two ablations come with it:
  - Does giving the LLM batch evidence help?
  - Does constraining its output to SQL expressions help?

  Q1 re-tests the exploratory finding in §11 under a frozen protocol.
- **Q2.** On incidents that no rule covers, does a tool-using agent propose better rule candidates than the rules' quarantine fallback and the fixed LLM pipelines?
- **Q3** *(milestone 2)*. Does a multi-agent setup beat a single tool-using agent that has the same tools and budget?
- **Q4.** If an LLM judge graded the candidates instead of ground truth, how often would it agree, and which failures would it miss?

## 2. Scenario

An on-call data engineer receives an alert from a generic monitor on the loan data. Each system under test does the engineer's job:

1. scope the affected records,
2. decide whether the values can be recovered,
3. propose a rule candidate that a human can review.

No system applies a fix on its own. A rule candidate becomes active only after human approval, which is out of scope here.

## 3. Provenance and data

- **Origin.** DataGenie v1 is the Team 6 course project: Sean Cabaniss, Yung Hsuan Hsieh, Ching-Fen Hung, Yonghui Kim, and Omkar Thombare. Its code is evaluated as submitted, at commit `b01ef2e` of the original team repository. That repository is not modified. Files copied from it are listed, with their SHA-256 hashes, in `dqeval/arms/v1/PROVENANCE.md`.
- **Reference data.** `data/LendingClub_100k.csv` is taken from the same commit. It is the course dataset before anomalies were injected. It is a *pre-injection reference*, not verified business truth, so scores measure recovery of the pre-injection state. The file is not committed here; its SHA-256 is recorded and checked on load.
- **Row identity.** Every record keeps its `id`.
- **Synthetic enrichment.** These fields are deterministic from `id` and documented in code:
  - `source_system`: one of four upstream feeds, `A` to `D`;
  - `ingested_at`: a timestamp that orders records into windows.

  They let incidents be scoped to a feed or a time range, which is what makes investigation necessary.
- **Windows.** Incidents are injected into windows of 2,000 consecutive rows.
- **Splits.**
  - The dev set uses rows 1 to 50,000.
  - Held-out set S1 uses rows 50,001 to 75,000.
  - Held-out set S2 uses rows 75,001 to 100,000.

## 4. Alerts

A system learns about an incident only from an alert. Alerts come from generic monitors that compare a window against a baseline built from clean reference windows:

| Monitor | Fires when, per column, window vs. baseline |
|---|---|
| Null rate | rises by at least 10 percentage points |
| Numeric shift | the p5, median, or p95 changes by at least 3× |
| New categories | values unseen in the baseline make up at least 5% of rows |
| String pattern | the share of a character-class pattern (e.g. `Aaa-9999`) changes by at least 10 pp |

An alert carries the column, the monitor, the baseline value, the window value, and the window bounds. It never says what the mechanism is, what the scope is, or which rows are affected.

Detection itself is out of scope. Every incident must trigger at least one alert that the incident causes, meaning an alert that does not fire on the same window without the incident. The authoring checker (§5.3) enforces this.

## 5. Incidents

### 5.1 Case types and correct handling

Ground truth records whether the injection changed each cell, and what its reference value is. Each incident has exactly one case type, which determines the correct handling:

| Case type | Definition | Correct handling |
|---|---|---|
| Recoverable | Reference values can be derived from data the system can see | RESTORE |
| Representation-only | Same meaning, different encoding | STANDARDIZE |
| Unrecoverable | Values are wrong and cannot be derived | QUARANTINE: hold the rows back, keep the values |
| Normal but unusual | No injection; the data is simply rare (negative control) | KEEP |

There is no separate "suspicious but undeterminable" type. When a system cannot tell, quarantining is the safe choice. It is scored as correct on unrecoverable incidents and as harm on normal ones, so caution has a measurable cost.

### 5.2 Dev set (visible)

The developer authors the dev set. It contains:
- the three course-project mechanisms: a null spike in `loan_amnt`, the sentinel value `9999999` in `annual_inc`, and overwritten `issue_d` values;
- variants of those three;
- some new mechanisms;
- at least two incidents of each case type.

Rules, prompts, tools, and agents may be tuned on the dev set freely.

### 5.3 Held-out sets S1 and S2 (sealed)

- **Author.** A separate model (Codex) writes both sets from `docs/HELDOUT_BRIEF.md`. It does so after this spec is frozen and **before** any rule library or agent exists, so the incidents cannot be tailored to them.
- **Format.** Incidents are data, not code. Each one gives:
  - the window,
  - a corruption, written as SQL scope and value expressions,
  - the case type,
  - the equivalence rule for STANDARDIZE cases,
  - counterexamples: clean rows that a rule scoped too broadly would wrongly change.
- **One column per incident.** Each incident corrupts exactly one column, and systems receive only the alerts for that column.
- **Checker.** A checker validates each incident: the format, that it triggers an alert, and that its ground truth is consistent.
- **Size.** Each set has at least 20 incidents, with at least 4 of each case type.
  - **Covered:** variants of the three course-project mechanisms, which may use other columns, scopes, or rates. Their mechanism family is `course_null`, `course_sentinel`, or `course_overwrite`.
  - **Uncovered:** at least 10 incidents use any other mechanism family.

  Every incident names its mechanism family. The dev set is written after the held-out sets, so "covered" cannot be defined by the dev set. Results are also reported split by whether the frozen rule library had a matching rule.
- **Use.** S1 is used once, for milestone 1. S2 is used once, for milestone 2. S2 is needed because S1's per-incident results become visible after milestone 1.
- **Storage.** Both sets are stored outside the repository, and their SHA-256 hashes are committed before development starts. The developer does not open a set until its milestone's systems are frozen (§9).
- **Limitation.** The hashes prove the sets did not change; they cannot prove nobody looked. Blindness rests on the process above, and is disclosed as such.

## 6. Systems under test

### 6.1 Output contract

Except for the PySpark arms (§6.2), every system returns exactly one rule candidate per alert:

```json
{
  "decision":  "RESTORE | STANDARDIZE | QUARANTINE | KEEP",
  "column":    "annual_inc",
  "scope":     "source_system = 'B' AND annual_inc < 20000",
  "new_value": "annual_inc * 12",
  "rationale": "…",
  "evidence":  ["observations the decision relies on"]
}
```

`scope` and `new_value` are each a single SQL expression over one row. Statements, subqueries, and table access are not allowed. Each expression is parsed and checked against an allowlist of operators and functions.

How a candidate is applied:
- **RESTORE / STANDARDIZE:** `CASE WHEN scope THEN new_value ELSE column END`.
- **QUARANTINE:** sets a `_quarantine` flag on the rows in scope.
- **KEEP:** changes nothing.

Rationale for the format:
- In the exploratory runs (§11), 4 of 9 generated PySpark snippets crashed on API misuse, and others edited unrelated cells.
- A constrained format separates the decision from the implementation.
- It runs safely without a sandbox.
- It reads as a reviewable rule.
- The same expression runs in DuckDB (evaluation) and Spark SQL (the stream prototype).

Its limit is that only row-level transforms and flags can be expressed. Group aggregates can be passed in as constants computed with tools. Deduplication and cross-table lookups cannot be expressed; that is listed in §12.

### 6.2 Arms

| Arm | What it does | Output | Evidence | Tools |
|---|---|---|---|---|
| A0 No change | Always returns KEEP | contract | – | – |
| A1 Quarantine all | Returns QUARANTINE scoped to every row in the alert window | contract | – | – |
| A2 Rules | Uses the rule library frozen after dev; an alert that no rule matches falls back to A1 | contract | precomputed | – |
| A3-v1 | DataGenie v1 as submitted (§6.3), option 1 | PySpark | – | – |
| A3-py | Current Agent 1 → Agent 2 pipeline | PySpark | ✓ | – |
| A3-sql | Same as A3-py | contract | ✓ | – |
| A3-sql-noev | Same as A3-sql | contract | – | – |
| A4 Single agent | One tool-using agent | contract | ✓ | ✓ |
| A5 Multi-agent *(M2)* | Orchestrator, investigator, fixer, and verifier agents | contract | ✓ | same as A4 |

How the arms pair up as ablations:
- **A3-py vs. A3-sql** isolates the output format.
- **A3-sql vs. A3-sql-noev** isolates the evidence.
- **A3-v1 vs. A3-py** shows the combined effect of the prompt redesign, the evidence, and the single-candidate output. It is not attributed to any one of them.

A4 and A5 get these tools, all read-only:
- SQL over the alert window and the clean baseline windows;
- a column profiler;
- a dry run that applies a candidate to the alert window and returns the cell-level diff.

### 6.3 DataGenie v1 as submitted

- **Code.** `src/llm/suggester_dashboard.py` and `data/historical_decisions.json` are frozen from `b01ef2e`. Prompts, sampling settings, and the historical-decision context are left unchanged.
- **Input adapter.** v1 expects its own detector's issue format, so a documented adapter turns each alert into that format. The adapter covers:
  - `column`;
  - `issue_type`: v1's own type name when the monitor matches one of v1's three issue types, otherwise the monitor name;
  - `severity`, by a fixed mapping;
  - `detail`, the alert text;
  - `sample_values`;
  - the `df_shape` file that v1 requires.
- **Primary candidate.** v1 returns three ranked options. Option 1 is the primary candidate.

### 6.4 Settings and budget

- **Model.** Every LLM arm uses the same model snapshot (`gpt-4o-mini`; the exact snapshot is recorded), with the API's default sampling settings, matching v1 as submitted. Each LLM arm runs 5 times per incident.
- **Caps.** A4 and A5 share the same caps per incident: 20 tool calls, 60,000 tokens, and 5 minutes. Actual usage is reported.
- **Retries.** A3-sql and A3-sql-noev get up to 2 retries when a candidate fails the format check. A3-py keeps the static pre-check and its retries, and v1 keeps its own critic.
- **No leakage.** No arm sees ground truth, the injection log, the case type, or the counterexamples.

## 7. Scoring

The unit of analysis is the **incident**, not the cell.

### 7.1 Three levels per candidate

1. **Executes.** API, JSON, contract, SQL, and PySpark failures all count against the arm; nothing is dropped.
   - Contract candidates must match the contract, and their expressions must parse and run.
   - PySpark candidates run in a fresh subprocess, on a copy of the window, with no API key in the environment and a 2-minute timeout.
2. **Fixes the window.** The result is compared cell by cell with the reference, joined on `id`. For PySpark candidates, only effects are scored:
   - A new boolean column ending in `_quarantine` counts as a quarantine flag. Any other new column is ignored.
   - A dropped injected row counts as not handled. A dropped clean row counts as harm.
3. **Safe to reuse.** The candidate is applied to the incident's counterexamples and to a clean regression slice. The slice is 5,000 reference rows from the incident's split, taken from the windows after the incident's window (topped up from the windows before it), excluding the incident's own rows. Incidents are evaluated independently, so another incident's window counts as clean reference data here. The candidate must change no cells and quarantine at most 1% of rows.

### 7.2 Incident success

A run succeeds only if all three levels hold. "Fixes the window" means:

| Case type | Required in the window |
|---|---|
| Recoverable | At least 95% of injected cells restored to the reference value |
| Representation-only | At least 95% of injected cells standardized to a value the incident defines as equivalent |
| Unrecoverable | At least 95% of injected rows quarantined, with their values not overwritten |
| Normal but unusual | No cell changed |

For every case type, in addition: no clean cell changed, and at most 1% of clean rows quarantined.

### 7.3 Reporting

- **Primary.** For each arm, the mean success rate over incidents, where each incident's rate is its successes out of 5 runs. Per-incident results are shown, split by case type and by covered vs. uncovered.
- **Ablations.** Format (A3-py vs. A3-sql) and evidence (A3-sql vs. A3-sql-noev), compared per incident.
- **Paired comparison for Q3.** For each incident in S2, whether A5 won, tied, or lost against A4.
- **Failure taxonomy.** Every failed run gets one primary label from this fixed list:
  - execution error,
  - invalid contract,
  - wrong decision,
  - scope too broad,
  - scope too narrow,
  - wrong value,
  - instruction violation.

  Labels are proposed by the developer's assistant, confirmed by the developer, and published with examples.
- **Cost vs. accuracy.** Success rate against tokens, dollars, and wall time per incident, for each arm.
- **Stability.** For each incident, how often its outcome changes across the 5 runs.
- **Threshold sensitivity.** Primary results recomputed with the 95% threshold at 90% and 99%, and the 1% quarantine allowance at 0.5% and 2%. The report states whether any conclusion flips.
- **Records.** All prompts, raw responses, model IDs, settings, and package versions are saved.
- **No significance claims.** With about 20 incidents per set, results are reported as counts and ranges.
- **Not measured.** Human review time; there are no real users.

### 7.4 Judge meta-evaluation (Q4)

For every candidate from the LLM arms, a judge model sees the following, and nothing that reveals ground truth:
- the alert,
- the evidence,
- the candidate,
- its dry-run diff on the window.

The judge labels the candidate *acceptable* or *not acceptable*, with a reason. Its labels are compared with ground-truth success, and the report includes:
- the agreement rate;
- the share of real failures the judge catches;
- the share of candidates it accepts that actually fail;
- which failure-taxonomy labels it misses most.

The judge is `gpt-4o-mini` unless a stronger model is added as a separately labeled run.

## 8. Milestones

| Milestone | Arms | Held-out set | Answers |
|---|---|---|---|
| M1 | A0–A4, all A3 variants | S1 | Q0, Q1, Q2, Q4 |
| M2 | A5, plus A4 again for pairing | S2 | Q3 |

## 9. Freeze protocol

1. Commit this spec. *(freeze point 1)*
2. Build the reference-data loader, the enrichment, the incident format and injector, the monitors, and the authoring checker. None of these encode a solution.
3. Write `docs/HELDOUT_BRIEF.md`. Codex writes S1 and S2 outside the repository, and both SHA-256 hashes are committed. *(freeze point 2)*
4. Build the dev set, the rule library, the contract and scorer, and the arms for M1, iterating on the dev set only.
5. Tag `eval-freeze-m1`, run S1 once, and publish the M1 results. *(freeze point 3)*
6. Build A5 on the dev set, tag `eval-freeze-m2`, run S2 once, and publish the M2 results. *(freeze point 4)*

Any later change produces a new version, and both versions are reported.

## 10. Adoption thresholds (set before any results)

- **Rules vs. LLM.** Suppose A2 succeeds on at least as many held-out incidents as the best LLM arm, and changes no more clean cells. Then rules stay primary, and the LLM is used only for alerts that no rule matches, or only to write explanations. If instead an LLM arm wins, it is used only for the incident types it won, and the results table shows which those are.
- **Multi-agent vs. single agent.** A5 is adopted over A4 only if it succeeds on more S2 incidents, with no more harm, under the same caps. Otherwise A4 is used.

## 11. Exploratory result (before this spec)

Before this spec existed, three saved LLM runs were compared with three hand-written rules. The comparison used the first 500 rows and the three known anomaly types, and scored option 1 only, giving 9 outcomes. The rules handled all three types correctly. The LLM fully succeeded in 1 of the 9 outcomes, and 4 crashed.

The rules were written **after** those failures had been seen. This result motivated the redesign, but it is not evidence for Q1; Q1 re-tests it under this protocol. The script is preserved in `dqeval/exploratory/`.

## 12. Threats to validity and limits

- The incidents are synthetic, and the held-out author is also an LLM, which may share blind spots with the systems under test.
- The pre-injection reference is not verified business truth.
- With about 20 incidents per set and one model, the results describe this setup, not data quality in general.
- The developer wrote both the dev set and the rules, so covered incidents favor A2 by design. The uncovered split exists to measure that bias.
- v1 was built around its own detector's issue format. The adapter may disadvantage it on alerts that its three issue types never covered, and this is reported alongside the v1 results.
- The contract cannot express deduplication or cross-table fixes. Incidents that need them are excluded by the brief.

## 13. Out of scope

- Streaming. The prototype lives in `prototype/`, and the rules would run there as a gate.
- Latency.
- The review dashboard and human review time.
- Detection quality.
- Other datasets.
- Any change to the original team repository.

## Changelog

**2026-10-17, after freeze point 1 and before any held-out incident existed** (no results affected):
- **§4, numeric shift.** The monitor also checks p5. With only the median and p95, downward shifts on part of a window were invisible. An example found while implementing it: one feed switching to monthly income moved p5 by 5.6×, but the median by only 1.10× and p95 by only 1.05×. Without p5, incident authors could only use upward shifts.
- **§4, alerts.** An incident's alert must be one the incident causes, meaning it does not fire on the same window without the incident. Otherwise background alerts from natural drift could satisfy the "triggers an alert" requirement.
- **§5.3, covered vs. uncovered.** These are now defined by mechanism family, relative to the three course-project mechanisms. The old definition used the dev set, which does not exist yet when the held-out sets are written.
- **§7.1, regression slice.** It is now taken from windows after the incident's own window, instead of from "outside every incident window". With at least 20 incidents and only 12 to 13 windows per held-out split, the old rule left no rows. Rows after the incident's window also match what reuse safety asks: will the rule harm later batches?
