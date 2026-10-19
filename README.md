# DataGenie: Should an LLM fix your data?

For a team course project, I built an LLM pipeline that writes fixes for data-quality incidents. Afterwards I measured whether those fixes were right.

**This repository is that measurement.** It compares hand-written rules with three LLM designs on incidents whose correct answer is known.

---

## TL;DR

- The original LLM pipeline **changed valid data in 38% of its runs**.
- Constraining the LLM to a single reviewable SQL rule, and giving it facts about the data, cut that harm to **1–7%**. It did not make the fixes more accurate.
- On 20 sealed, held-out incidents, a **hand-written rule library solved 12**. The best LLM method, an agent that can query the data and dry-run its fix, solved **5**. It used **6× the tokens** of a single LLM call for a success rate only 0.03 higher.
- On **new kinds of corruption**, such as a unit change, a decimal shift, or a sign flip, **nothing worked**: not the rules, and not the LLMs.
- **Decision:** rules handle the issues they know. Anything else is quarantined for a human. The LLM does not write fixes.

---

## Results

**Held-out set S1:** 20 incidents. Each LLM method was run 5 times per incident.

| Method | Success rate | Runs that changed valid data | Tokens per run | How it usually failed |
|---|---|---|---|---|
| **Hand-written rules** | **0.60** | **0%** | 0 | Did not recognize a new kind of problem |
| LLM agent with tools | 0.27 | 7% | 10,213 | Chose the wrong action, often inventing values |
| LLM, fixed prompt | 0.24 | 1% | 1,651 | Chose the wrong action, or scoped it too broadly |
| Course system (v1, as submitted) | 0.19 | 38% | 1,589 | Changed valid data |

The two controls bound the range:
- **Change nothing** scored 0.20. It only got the four "leave it alone" incidents right.
- **Quarantine the whole window** scored 0.00. It held back every valid row along with the bad ones.

A run **succeeds** only if all of these hold:
- the fix runs;
- it handles at least 95% of the corrupted cells correctly;
- it changes no valid cell and holds back at most 1% of valid rows;
- applied to later, clean data, it still harms nothing.

**Known vs. new problems.** Half of the incidents are variants of the three anomalies from the course demo; the other half are mechanisms nobody had seen.

| | Known mechanisms | New mechanisms |
|---|---|---|
| Hand-written rules | **0.90** | 0.30 |
| LLM agent with tools | 0.38 | 0.16 |
| LLM, fixed prompt | 0.42 | 0.06 |
| Course system (v1) | 0.32 | 0.06 |

The rules' 0.30 on new mechanisms comes entirely from the "leave it alone" incidents. Six new corruptions were scored at 0 for every method:
- a unit change;
- a decimal shift;
- a sign flip;
- an added offset;
- bucketing;
- truncation.

The one exception: the agent fixed the sign flip once in 5 runs.

Full tables: [`results/S1/report.md`](results/S1/report.md).

---

## One incident, four methods

**Incident S1-11.** Feed B starts sending a placeholder income of `9,999,999`. This is the same kind of anomaly the course demo used.

> **Alert:** `annual_inc`: p95 moved from 160,000 (baseline) to 9,999,999.
> **Facts given to the LLM methods:** 516 of the unusual values are exactly 9,999,999, and all of them come from feed B.

The correct action is to **quarantine** those rows. The real incomes cannot be recovered, so any replacement value would be made up.

| Method | What it proposed | Result |
|---|---|---|
| Hand-written rules | Quarantine `annual_inc = 9999999` | ✅ Held back all 516 rows; no valid row touched |
| Course system (v1) | In all 5 runs, it treated the placeholder as *missing* data: it filled nulls with the median or the mean, or dropped null rows | ❌ 0/5. Nothing was missing, so nothing was fixed |
| LLM, fixed prompt | Quarantine `annual_inc = 9999999` in 3 runs. The other two quarantined `annual_inc = annual_inc` (every row) and a list of 5 ids | ⚠️ 3/5 |
| LLM agent with tools | It always queried the rows first and saw 516 identical values. Then, in 3 of 5 runs, it **replaced them with invented numbers**: 1,000,000, `loan_amnt × 10`, and 160,000 (the baseline's p95) | ⚠️ 2/5 |

**The agent had the evidence in front of it and still invented values.** Across all six unrecoverable incidents, it chose to rewrite values in **18 of 30 runs**.

---

## Where the LLM methods failed

These are failure modes observed in this setup, with `gpt-4o-mini` and one prompt design. They are not claims about LLMs in general.

1. **Misreading the problem.**
   - v1 treated a placeholder as missing data, and an overwritten date as a formatting issue.
   - On incidents where the true value *could* be rebuilt from another column, the fixed-prompt LLM and the agent each succeeded 0.17 of the time, and v1 never did. The rules succeeded 0.33 of the time.
2. **Inventing values.** When the true value was unknowable, the models filled in a plausible one: a median, a cap, or a guess.
3. **Acting when nothing is wrong.** On "leave it alone" incidents, the fixed-prompt LLM scored 0.15 and the agent 0.35. The rules scored 0.75.
4. **Inconsistency.** Given the same input, the fixed-prompt LLM solved an incident in all 5 runs only once out of 20 incidents. The agent never did.
5. **Tools did not fix judgment.** The agent used 4.8 of its 20 allowed tool calls on average, and gained 0.03 in success rate.

---

## How the evaluation works

```
LendingClub, 100k rows, before injection   ─── the answer key
  → replayed in time order as 2,000-row windows
  → an incident corrupts one column in one window
      (for example: one feed, from 14:10 onward, a value range)
  → a monitor raises an alert: which column, and what moved — never the cause
  → each method proposes ONE fix
  → the fix runs, and every cell is compared with the answer key
```

| Kind of incident | Correct action |
|---|---|
| The true value can be rebuilt from other columns | **Restore** it |
| Same meaning, different encoding | **Standardize** it |
| Wrong, and the true value is unknowable | **Quarantine**: hold the rows back for a human, keep the values |
| Real but unusual data | **Keep** it |

Every method except v1 must answer with one **rule**: a decision, a SQL condition for which rows, and a SQL expression for the new value. This makes fixes reviewable and safe to run. v1 is scored on its own PySpark output, run unchanged.

**Keeping the test fair:**
- The held-out incidents were written by a different model (Codex) before any rule or agent existed. They were sealed, and their hashes were committed.
- The methods were frozen with a git tag before the held-out set was opened. The harness refuses to run otherwise.
- The rules and the LLM prompts were tuned on the same 12 dev incidents.
- Every method gets the same input and is scored by the same scorer.

Details: [`docs/EVAL_SPEC.md`](docs/EVAL_SPEC.md).

---

## Decision

The adoption threshold was set before any results existed:

> If the rules solve at least as many held-out incidents as the best LLM method, and harm no more valid data, rules stay primary.

The rules solved 12 incidents against the agent's 5, and changed no valid value. So:
- **Known issues:** hand-written rules.
- **Anything the rules cannot explain:** targeted quarantine, and a human decides.
- **The LLM:** not used to write fixes. Whether it helps *explain* incidents to the reviewer was not measured.

## Limits

- **Scale.** There is one dataset, one model (`gpt-4o-mini`), and 20 held-out incidents. The incidents are synthetic, and they were written by an LLM.
- **The rules were developed on the dev set,** so known mechanisms favor them. On new mechanisms they did no better than the LLMs.
- **One held-out incident (S1-12) is unsolvable under the scoring rules.** Even the ideal answer holds back 49 valid rows against a limit of 15. Excluding it, the rules solve 12/19 and the agent 5/19. The conclusion is the same.
- **Feed-wide corruptions.** This setup assumes an incident ends with its window. So a fix scoped to a whole feed also rewrites later clean rows, and only window-bounded fixes pass the reuse check.
- **Open question:** does a stronger model change the picture? The second sealed set, S2, is reserved for that.

---

## Where this came from

- **The course project.** *DataGenie* was built by Team 6 for the Big Data Analytics course in the MSBA program at the Carlson School of Management, University of Minnesota. The team was Sean Cabaniss, Yung Hsuan Hsieh, Ching-Fen Hung, Yonghui Kim, and Omkar Thombare. I was the team's LLM engineer and built the LLM pipeline with Ching-Fen Hung. It is evaluated here as submitted ([provenance](dqeval/arms/v1/PROVENANCE.md)).
- **My extension.** I added a streaming prototype on Kafka and Spark Structured Streaming ([`prototype/`](prototype/)), and then this evaluation. The [exploratory comparison](dqeval/exploratory/) that prompted it is preserved.
- **Tools.** Developed with Claude Code as a pair programmer. Codex acted as a mock reviewer and wrote the sealed incident sets.

## Repository

```
dqeval/          the evaluation: data, incidents, monitors, rule contract, scoring, methods, harness, report
incident_sets/   the 12 dev incidents
results/S1/      the held-out run: scored records, run metadata, report
docs/            the method spec, the brief for the incident author, the sealed-set hashes
prototype/       the streaming prototype (not developed further)
tests/           pytest suite (no API calls)
```

## Reproduce

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Reference data: the course dataset at the commit the project was submitted with
git -C <path-to-team-repo> show b01ef2e:data/LendingClub_100k.csv > data/LendingClub_100k.csv

python -m pytest                                   # tests, no API calls
python -m dqeval.report results/S1                 # rebuild the S1 tables from the saved records
python -m dqeval.harness incident_sets/dev.json --split dev --arms A0 A1 A2 --out results/dev

# The LLM methods call the OpenAI API. They need OPENAI_API_KEY in .env and cost money.
python -m dqeval.harness incident_sets/dev.json --split dev --arms A3-v1 A3-sql A4 --out results/dev-llm
```

The team repository is [tom666d/big_data_team_6](https://github.com/tom666d/big_data_team_6).
