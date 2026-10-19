## Headline

| method | success rate | main failure | tokens / run | seconds / run |
|---|---|---|---|---|
| No change (control) [A0] | 0.20 | not handled (16/20 runs) | 0 | 0.0 |
| Quarantine all (control) [A1] | 0.00 | harmed clean data (20/20 runs) | 0 | 0.0 |
| Rules [A2] | 0.60 | not handled (6/20 runs) | 0 | 0.0 |
| LLM, fixed prompt [A3-sql] | 0.24 | not handled (37/100 runs) | 1,651 | 2.7 |
| Course system (v1) [A3-v1] | 0.19 | harmed clean data (38/100 runs) | 1,589 | 19.2 |
| LLM agent with tools [A4] | 0.27 | not handled (44/100 runs) | 10,213 | 25.4 |

## Success by arm

| arm | success | executes | fixes window | safe to reuse | decision matches | time-scoped successes | LLM calls / run | tokens / run | seconds / run |
|---|---|---|---|---|---|---|---|---|---|
| A0 | 0.20 (4/20 always) | 1.00 | 0.20 | 1.00 | 0.20 | 0 | 0.0 | 0 | 0.00 |
| A1 | 0.00 (0/20 always) | 1.00 | 0.00 | 0.90 | 0.30 | 0 | 0.0 | 0 | 0.00 |
| A2 | 0.60 (12/20 always) | 1.00 | 0.60 | 0.85 | 0.75 | 0 | 0.0 | 0 | 0.03 |
| A3-sql | 0.24 (1/20 always) | 0.92 | 0.24 | 0.56 | 0.52 | 0 | 1.2 | 1651 | 2.71 |
| A3-v1 | 0.19 (3/20 always) | 0.93 | 0.25 | 0.45 | n/a | 0 | 2.0 | 1589 | 19.24 |
| A4 | 0.27 (0/20 always) | 0.97 | 0.27 | 0.63 | 0.45 | 0 | 4.6 | 10213 | 25.40 |

## Success rate by case type

| arm | normal | recoverable | representation | unrecoverable |
|---|---|---|---|---|
| A0 | 1.0 | 0.0 | 0.0 | 0.0 |
| A1 | 0.0 | 0.0 | 0.0 | 0.0 |
| A2 | 0.75 | 0.33 | 1.0 | 0.5 |
| A3-sql | 0.15 | 0.17 | 0.4 | 0.27 |
| A3-v1 | 0.15 | 0.0 | 0.8 | 0.0 |
| A4 | 0.35 | 0.17 | 0.5 | 0.17 |

## Covered vs. uncovered

| arm | covered | uncovered |
|---|---|---|
| A0 | 0.0 | 0.4 |
| A1 | 0.0 | 0.0 |
| A2 | 0.9 | 0.3 |
| A3-sql | 0.42 | 0.06 |
| A3-v1 | 0.32 | 0.06 |
| A4 | 0.38 | 0.16 |

## Why runs failed (first reason, number of runs)

| arm | did not execute | harmed clean data | not handled | unsafe to reuse |
|---|---|---|---|---|
| A0 | 0 | 0 | 16 | 0 |
| A1 | 0 | 20 | 0 | 0 |
| A2 | 0 | 2 | 6 | 0 |
| A3-sql | 8 | 31 | 37 | 0 |
| A3-v1 | 7 | 38 | 30 | 6 |
| A4 | 3 | 26 | 44 | 0 |

## Per incident (successes / runs)

| incident_id | case_type | A0 | A1 | A2 | A3-sql | A3-v1 | A4 |
|---|---|---|---|---|---|---|---|
| S1-01 | recoverable | 0/1 | 0/1 | 1/1 | 5/5 | 0/5 | 4/5 |
| S1-02 | recoverable | 0/1 | 0/1 | 1/1 | 0/5 | 0/5 | 0/5 |
| S1-03 | recoverable | 0/1 | 0/1 | 0/1 | 0/5 | 0/5 | 0/5 |
| S1-04 | recoverable | 0/1 | 0/1 | 0/1 | 0/5 | 0/5 | 0/5 |
| S1-05 | recoverable | 0/1 | 0/1 | 0/1 | 0/5 | 0/5 | 1/5 |
| S1-06 | recoverable | 0/1 | 0/1 | 0/1 | 0/5 | 0/5 | 0/5 |
| S1-07 | representation | 0/1 | 0/1 | 1/1 | 2/5 | 5/5 | 4/5 |
| S1-08 | representation | 0/1 | 0/1 | 1/1 | 3/5 | 1/5 | 3/5 |
| S1-09 | representation | 0/1 | 0/1 | 1/1 | 1/5 | 5/5 | 2/5 |
| S1-10 | representation | 0/1 | 0/1 | 1/1 | 2/5 | 5/5 | 1/5 |
| S1-11 | unrecoverable | 0/1 | 0/1 | 1/1 | 3/5 | 0/5 | 2/5 |
| S1-12 | unrecoverable | 0/1 | 0/1 | 0/1 | 0/5 | 0/5 | 0/5 |
| S1-13 | unrecoverable | 0/1 | 0/1 | 1/1 | 2/5 | 0/5 | 0/5 |
| S1-14 | unrecoverable | 0/1 | 0/1 | 1/1 | 3/5 | 0/5 | 3/5 |
| S1-15 | unrecoverable | 0/1 | 0/1 | 0/1 | 0/5 | 0/5 | 0/5 |
| S1-16 | unrecoverable | 0/1 | 0/1 | 0/1 | 0/5 | 0/5 | 0/5 |
| S1-17 | normal | 1/1 | 0/1 | 1/1 | 1/5 | 0/5 | 2/5 |
| S1-18 | normal | 1/1 | 0/1 | 1/1 | 2/5 | 2/5 | 3/5 |
| S1-19 | normal | 1/1 | 0/1 | 1/1 | 0/5 | 1/5 | 2/5 |
| S1-20 | normal | 1/1 | 0/1 | 0/1 | 0/5 | 0/5 | 0/5 |
