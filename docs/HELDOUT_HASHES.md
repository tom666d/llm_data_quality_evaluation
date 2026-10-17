# Held-out Set Hashes (freeze point 2)

These hashes were committed on 2026-10-17. At that time, no dev incident, rule library, or agent existed.

| Set | SHA-256 | Checker summary |
|---|---|---|
| S1 | `09481ddee13ee03291c74fb06bf7b64b64fe7d1dfd3b2e99a3041d49aa8ef9bc` | 20 incidents: recoverable 6, representation 4, unrecoverable 6, normal 4. Covered 10, uncovered 10. 12 distinct windows |
| S2 | `8c1f5869ef7186a47a39dade4b12a42f9d6987c118fd3c42d40791af7cd105d5` | 20 incidents: recoverable 6, representation 4, unrecoverable 6, normal 4. Covered 10, uncovered 10. 12 distinct windows |

- **Author.** Codex wrote both sets, following `docs/HELDOUT_BRIEF.md` at commit `e97c94a`.
- **Storage.** The files are stored outside the repository, in `~/dq_heldout/`. The developer has not opened them.
- **Verification.** The developer verified them with the checker's quiet mode, which prints only the summary and the hash:

  ```bash
  .venv/bin/python -m dqeval.check_incidents ~/dq_heldout/S1.json --split S1 --quiet
  .venv/bin/python -m dqeval.check_incidents ~/dq_heldout/S2.json --split S2 --quiet
  ```
- **Use.** S1 is opened once, after tag `eval-freeze-m1`. S2 is opened once, after tag `eval-freeze-m2`.
- **Limitation.** A hash proves a file did not change; it cannot prove nobody looked. See spec §5.3.
