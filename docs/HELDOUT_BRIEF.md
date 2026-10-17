# Held-out Incident Brief

**For: the author of the sealed held-out sets S1 and S2.** This brief is self-contained. The protocol behind it is `docs/EVAL_SPEC.md` (§3 to §5 and §7). If the two ever disagree, follow the spec and say so in your report.

---

## 1. Your role

You are writing the test questions for an evaluation of data-quality systems. Every system under test receives an alert about a window of loan data and must propose one rule candidate. The candidate either restores values, standardizes their encoding, quarantines rows, or leaves the data alone.

You write two sets of incidents, **S1** and **S2**. Each set is used exactly once, as a final exam. After you finish:

- **the developer will not open your files** until the systems are frozen;
- **nobody tunes anything on your incidents**;
- the only thing committed to the repository is each file's SHA-256 hash.

No rule library or agent exists yet. Do not look for them, and do not design incidents against any particular approach. Write the incidents a careful data engineer would want a system to handle correctly.

## 2. What systems see, and what they never see

For each incident, a system receives:
- the alerts the monitors raise on the incident's column;
- the injected window (2,000 rows, with the visible columns below);
- clean baseline windows from the same split.

A system **never** sees your JSON. That includes the case type, the family, the description, the scope, the counterexamples, and the proofs.

## 3. The data

**Reference rows.** `data/LendingClub_100k.csv`, loaded and enriched by `dqeval/data.py`, in file order. Explore it with pandas as much as you like.

**Visible columns.** These are all the columns a system can see, and all your SQL may use:

| Column | Type | Notes |
|---|---|---|
| `id` | integer | unique row id |
| `loan_amnt`, `funded_amnt`, `funded_amnt_inv` | float | in the reference, `funded_amnt` equals `loan_amnt` in every row |
| `term` | string | `' 36 months'` or `' 60 months'`, with a leading space |
| `int_rate` | float | a percentage, e.g. `13.99` |
| `installment` | float | monthly payment |
| `grade`, `sub_grade` | string | `A`–`G`, `A1`–`G5` |
| `emp_length` | string | e.g. `'10+ years'`; about 6% null naturally |
| `home_ownership`, `verification_status`, `loan_status`, `purpose`, `addr_state` | string | categorical |
| `annual_inc`, `dti`, `revol_util`, `tot_cur_bal` | float | |
| `issue_d` | string | `'Oct-2015'`, `'Nov-2015'`, or `'Dec-2015'` |
| `source_system` | string | synthetic upstream feed `A`–`D`, from `id % 4` |
| `ingested_at` | timestamp | synthetic; row *r* arrives at `2026-01-05 00:00:00` + 3*r* seconds |

**Windows.** Window *w* is the 2,000 consecutive rows starting at row 2,000*w*. It spans 100 minutes, starting at `2026-01-05 00:00:00` + 100*w* minutes.

| Set | Windows you may use | Rows |
|---|---|---|
| S1 | 25–36 | 50,000–73,999 |
| S2 | 37–49 | 75,000–99,999 |

Incidents are evaluated independently, each on clean data, so several incidents may use the same window.

## 4. Incident format

Each set is one JSON file containing a list of incidents. All SQL uses the DuckDB dialect.

```json
{
  "incident_id": "S1-07",
  "split": "S1",
  "window": 31,
  "column": "annual_inc",
  "case_type": "unrecoverable",
  "family": "course_sentinel",
  "corruption": {"scope": "source_system = 'D'", "value": "9999999"},
  "population": null,
  "recovery": null,
  "equivalence": null,
  "canonical": null,
  "counterexamples": "annual_inc >= 1000000",
  "description": "Feed D's form defaults missing income to a 9999999 placeholder."
}
```

| Field | Meaning |
|---|---|
| `incident_id` | Unique within the file |
| `split`, `window` | Where the incident happens (§3) |
| `column` | The one column the incident corrupts. Corruptions touching several columns are out of scope |
| `case_type` | One of `recoverable`, `representation`, `unrecoverable`, `normal` (§5) |
| `family` | The mechanism family (§6) |
| `corruption.scope` | Which rows change: a boolean over visible columns, run as `UPDATE win SET <column> = (<value>) WHERE (<scope>)` |
| `corruption.value` | The new value, an expression over the row's original values. It must be castable to the column's type |
| `population` | Optional. Fills the window with the first 2,000 rows of the split that match this predicate, re-stamped into the window's time range. Use it for normal-but-unusual windows. It must match at least 2,000 rows in the split |
| `recovery`, `equivalence`, `canonical` | The proofs (§5) |
| `counterexamples` | A predicate selecting **clean** split rows, outside the window, that a rule scoped too broadly would wrongly change. At least 10 rows must match |
| `description` | One or two sentences: the plausible upstream story behind the incident |

## 5. Case types and their proofs

| Case type | What it means | What you must provide |
|---|---|---|
| `recoverable` | The true values can be rebuilt from what a system can see | `recovery`: an expression over **visible columns** of the injected row that rebuilds **every** injected value, within 0.01 for numbers |
| `representation` | Same meaning, different encoding | `equivalence`: a boolean over `out` and `ref`, true for every injected value. `canonical`: a boolean over `out`, true for every reference value in the window and false for every injected value |
| `unrecoverable` | The values are wrong and cannot be rebuilt | Nothing to prove. The checker rejects the incident if a visible column still holds the reference values. **Do not** label an incident unrecoverable if any computation over visible columns could rebuild the values, e.g. from `funded_amnt`, or from `installment`, `term`, and `int_rate` |
| `normal` | No corruption; the data is real but rare (a negative control) | No `corruption`; use `population`. The window must still cause an alert |

## 6. Families: covered vs. uncovered

Each incident names its mechanism family.

- **Covered** incidents are variants of the course project's three mechanisms. They may use any column, scope, or rate, and must use one of these exact family names:
  - `course_null`: values set to NULL;
  - `course_sentinel`: values replaced by one repeated placeholder;
  - `course_overwrite`: values overwritten with a constant, or with a string in another format.
- **Uncovered** incidents use any other mechanism. Name the family yourself, in short snake_case.

## 7. Requirements per set

- At least **20** incidents.
- At least **4** incidents of each case type.
- At least **10** uncovered incidents.
- Every incident passes the checker. In particular, every incident must **cause** an alert on its column: the alert fires on the injected window but not on the same window without the incident. The monitors are in `dqeval/monitors.py` and described in spec §4.
- S1 and S2 have a similar mix of case types, covered vs. uncovered, and difficulty. They are different incidents: do not copy S1 into S2.

## 8. What makes a good incident

- **Scope matters.** Many incidents should hit only part of a window, such as one feed, a time range, or a value range. That way a rule scoped too broadly harms clean rows, and the counterexamples and regression slice catch it.
- **Counterexamples are look-alikes.** Choose real clean rows that resemble the corrupted ones. For example, if incomes were divided by 12, choose real low incomes.
- **Traps on both sides.** Include:
  - normal windows that look alarming;
  - unrecoverable incidents where an obvious "fix" would invent values;
  - recoverable incidents whose recovery source takes some looking to find.
- **Plausible stories.** Every incident should read like something an upstream system could really do.
- **Out of scope, do not use:**
  - corruptions of several columns;
  - deleting or duplicating rows;
  - cross-table issues;
  - anything needing data outside the split.

## 9. Process

1. Write the two files **outside the repository**: `~/dq_heldout/S1.json` and `~/dq_heldout/S2.json`.
2. From the repository root, run the checker until both files pass:
   ```bash
   .venv/bin/python -m dqeval.check_incidents ~/dq_heldout/S1.json --split S1
   .venv/bin/python -m dqeval.check_incidents ~/dq_heldout/S2.json --split S2
   ```
   Each incident takes about a second to check.
3. Report back **only**, for each set, the summary line and the `PASSED  sha256=…` line. Do not paste incident contents, families, scopes, or descriptions into the conversation. If the developer asks about the contents, decline until they say the systems are frozen.
4. After reporting the hashes, do not modify the files, and do not write anything into the repository.

The developer will commit the two hashes. That is freeze point 2.
