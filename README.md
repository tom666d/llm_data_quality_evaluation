# DataGenie: Should an LLM fix your data?

Data pipelines break in small, quiet ways. A feed starts sending blank values, a placeholder like `9999999`, or numbers in the wrong unit. In a team course project, **[DataGenie](https://github.com/tom666d/llm_data_quality_assistant)**, I built an LLM pipeline that writes code to fix problems like these.

This repository is the follow-up. **I tested whether those LLM-written fixes are actually correct**, and compared them with simpler alternatives on problems where the right answer is known.

---

## The answer in 30 seconds

**Test incidents solved, out of 20.** For the LLM methods, an incident counts as solved when at least 3 of 5 runs succeed.

```
Hand-written rules        ████████████  12
LLM agent with tools      █████          5
LLM with a fixed prompt   ████           4
Original course system    ███            3
```

- **Simple rules beat every LLM approach**, and they never changed data that was correct.
- **The original LLM pipeline was risky:** 38% of its runs changed correct data.
- **Giving the LLM tools barely helped.** The agent used 6× the tokens of a single LLM call to raise its success rate by 0.03.
- **Problems nobody had anticipated went unsolved by every method**, rules and LLMs alike.

**Decision:** rules handle the problems they know, and anything else is held back for a human to review. The LLM should not write the fixes.

---

## What was compared

Every method gets the same alert and the same data, and must propose **one fix**.

| Method | How it decides |
|---|---|
| **Hand-written rules** | About 300 lines of if-then logic, no LLM. For example: "if one value makes up most of the outliers, hold those rows back". Written using 12 practice incidents |
| **Original course system** | DataGenie as submitted. One LLM call diagnoses the problem and another writes free-form PySpark fix code. Run unchanged |
| **LLM with a fixed prompt** | One LLM call. It receives a summary of the data and must answer with a single SQL rule |
| **LLM agent with tools** | The same summary, but it can also run SQL queries and test its rule before submitting. It is allowed up to 20 tool calls |

All LLM methods use `gpt-4o-mini`.

---

## What one test looks like

**Incident S1-11.** Each incident corrupts one column in a 2,000-row batch of loan records. Here, one data feed starts sending a placeholder income of `9,999,999`. This is the same kind of problem the course demo used.

> **Alert:** `annual_inc`: the 95th percentile jumped from 160,000 to 9,999,999.

The original values are unknowable, so the correct fix is to **hold those rows back for review** and not invent replacements.

| Method | What it proposed | Result |
|---|---|---|
| Hand-written rules | Hold back rows where `annual_inc = 9999999` | ✅ All 516 bad rows held back; nothing else touched |
| Original course system | Treated the problem as *missing* data: filled blanks with the median or the mean, or dropped blank rows. Nothing was blank, so nothing changed | ❌ 0 of 5 runs |
| LLM with a fixed prompt | Right in 3 runs. The other two held back every row, or only 5 hand-picked rows | ⚠️ 3 of 5 |
| LLM agent with tools | It queried the data every time and saw 516 identical values. Then in 3 runs it **replaced them with invented numbers**: 1,000,000, `loan_amnt × 10`, and 160,000 | ⚠️ 2 of 5 |

The agent had the evidence and still made values up. Across all six incidents where the true value was unknowable, it chose to overwrite values in **18 of 30 runs**.

**The four kinds of incident, and what counts as the right fix:**

| The data is… | Right fix |
|---|---|
| wrong, but the true value can be rebuilt (from another column, or by undoing a unit change) | **Restore** it |
| right in meaning, but written in a different format | **Standardize** it |
| wrong, and the true value is unknowable | **Hold back** the rows for a human, values untouched |
| real, just unusual | **Leave it alone** |

---

## Results in detail

These are **20 test incidents**, written by a separate model and locked away before any method was built. Each LLM method ran 5 times per incident.

| Method | Incidents solved | Success rate (all runs) | Runs that changed correct data | Tokens per run |
|---|---|---|---|---|
| **Hand-written rules** | **12** | **0.60** | **0%** | 0 |
| LLM agent with tools | 5 | 0.27 | 7% | 10,213 |
| LLM with a fixed prompt | 4 | 0.24 | 1% | 1,651 |
| Original course system | 3 | 0.19 | 38% | 1,589 |

For reference, *change nothing* solves 4 incidents (the "leave it alone" ones), and *hold back the whole batch* solves 0.

**A run counts as a success only if:**
1. the fix runs;
2. it handles at least 95% of the corrupted values correctly;
3. it changes no correct value and holds back at most 1% of correct rows;
4. applied to later, clean data, it still changes nothing.

**Known vs. new problems.** Half of the incidents are variations of the three problems in the course demo; the other half are new.

| Success rate | Known problems | New problems |
|---|---|---|
| Hand-written rules | **0.90** | 0.30 |
| LLM agent with tools | 0.38 | 0.16 |
| LLM with a fixed prompt | 0.42 | 0.06 |
| Original course system | 0.32 | 0.06 |

The rules' 0.30 on new problems comes only from the "leave it alone" incidents. Six new kinds of corruption were solved by no method:
- a unit change;
- a decimal shift;
- a sign flip;
- an added offset;
- bucketing;
- truncation.

The one exception: the agent fixed the sign flip in 1 of 5 runs.

Every table: [`results/S1/report.md`](results/S1/report.md).

---

## Why the LLM methods fell short

These are patterns observed in this setup, with `gpt-4o-mini` and one prompt design. They are not claims about LLMs in general.

1. **They misread the problem.** The course system treated a placeholder as missing data, and an overwritten date as a formatting issue. When the true value could be rebuilt, the LLM methods managed it at most 17% of the time; the rules managed 33%.
2. **They invented values.** When the true value was unknowable, they filled in something plausible: a median, a cap, a guess.
3. **They acted when nothing was wrong.** On "leave it alone" incidents, the fixed-prompt LLM succeeded 15% of the time and the agent 35%. The rules succeeded 75% of the time.
4. **They were inconsistent.** The same input gave different answers across runs. The fixed-prompt LLM got all 5 runs right on only 1 incident out of 20; the agent never did.
5. **Tools did not fix judgment.** The agent used about 5 of its 20 allowed tool calls and gained only 0.03 in success rate.

---

## Decision

The rule for adopting a method was written down **before** any results existed:

> If the rules solve at least as many test incidents as the best LLM method, and harm no more correct data, rules stay primary.

The rules solved 12 incidents against the agent's 5, and changed no correct value. So:
- **Known problems:** hand-written rules.
- **Anything else:** hold the rows back, and a human decides.
- **The LLM:** not used to write fixes. Whether it could help explain incidents to the reviewer was not tested.

## How the test was kept fair

- **The answer key is the original data,** kept from before any corruption.
- **The 20 test incidents were written by a different model (Codex)** before any rule or agent existed. They were locked away, and their file hashes were committed.
- **The methods were frozen with a git tag before the test incidents were opened.** The test runner refuses to run otherwise.
- **The rules and the LLM prompts were tuned on the same 12 practice incidents.**
- **Every method gets the same input and is scored by the same code.**

Full method: [`docs/EVAL_SPEC.md`](docs/EVAL_SPEC.md).

## Limits

- **Small scale.** There is one dataset, one model (`gpt-4o-mini`), and 20 test incidents. The incidents are synthetic, and an LLM wrote them.
- **The rules were built on the practice incidents,** so known problems favor them. On new problems they did no better than the LLMs.
- **One test incident (S1-12) is unsolvable under the scoring rules.** Even the ideal answer holds back 49 correct rows against a limit of 15. Without it, the rules solve 12/19 and the agent 5/19: the same conclusion.
- **Feed-wide corruptions.** The test assumes a problem ends with its batch. A fix aimed at a whole feed therefore also rewrites later clean rows, and only fixes limited to that batch's time range pass the reuse check.
- **Open question:** would a stronger model change the picture? A second locked set of 20 incidents is reserved for that.

---

## Background

- **The course project.** *DataGenie* was built by Team 6 for the Big Data Analytics course in the MSBA program at the Carlson School of Management, University of Minnesota. The team was Sean Cabaniss, Yung Hsuan Hsieh, Ching-Fen Hung, Yonghui Kim, and Omkar Thombare.
  - I was the team's LLM engineer and built the LLM pipeline with Ching-Fen Hung.
  - The submitted version: [llm_data_quality_assistant](https://github.com/tom666d/llm_data_quality_assistant). How it is used here: [provenance](dqeval/arms/v1/PROVENANCE.md).
- **My extension.** I added a streaming prototype on Kafka and Spark Structured Streaming ([`prototype/`](prototype/)), and then built this evaluation. The [first exploratory comparison](dqeval/exploratory/) that prompted it is preserved.

## Repository

```
dqeval/          the evaluation: data, incidents, monitors, rule format, scoring, methods, runner, report
incident_sets/   the 12 practice incidents
results/S1/      the test run: scored records, run metadata, report
docs/            the full method, the brief for the incident author, the locked-set hashes, a history note
prototype/       the streaming prototype (not developed further)
tests/           pytest suite (no API calls)
```

## Reproduce

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Reference data: the course dataset at the submitted commit, from a clone of llm_data_quality_assistant
git -C <path-to-llm_data_quality_assistant> show b01ef2e:data/LendingClub_100k.csv > data/LendingClub_100k.csv

python -m pytest                                   # tests, no API calls
python -m dqeval.report results/S1                 # rebuild the result tables from the saved records
python -m dqeval.harness incident_sets/dev.json --split dev --arms A0 A1 A2 --out results/dev

# The LLM methods call the OpenAI API. They need OPENAI_API_KEY in .env and cost money.
python -m dqeval.harness incident_sets/dev.json --split dev --arms A3-v1 A3-sql A4 --out results/dev-llm
```
