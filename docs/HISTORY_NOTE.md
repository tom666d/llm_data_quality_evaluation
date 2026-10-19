# History note

On 2026-10-18 this project moved from the repository `data-quality-agent` to this one. While moving it, commit messages were rewritten to drop co-author trailers.

- **Nothing else changed.** Every commit keeps its file contents (the same git tree), its author date, and its commit date.
- **The tag `eval-freeze-m1`** points to the rewritten commit and keeps its original tag date: 2026-10-17 16:15:23 -0500.
- **Rewriting changes commit hashes.** Current docs cite the new hashes. [`results/S1/run.json`](../results/S1/run.json) is the raw record of the held-out run, so it is left as written. It cites `7d3d379`, which is `6e1c2a4` here.

## Hash mapping (old → new, oldest first)

| Old | New | Commit |
|---|---|---|
| `f53a7e1` | `d4d18d3` | Initial commit |
| `cdcf45a` | `f952fba` | initial commit: detector, suggester, producer, README |
| `3ad9c99` | `483baca` | Remove generated artifacts and empty placeholders |
| `3fbd0f9` | `762326c` | Trim pipeline to its working core and ground agents in batch evidence |
| `3cc97d2` | `1a4cc7a` | Fix retry and scoring bugs, keep evidence to observations |
| `874eaf5` | `ca8e103` | Wire critic failures into retries; flag instead of nulling |
| `4eadf71` | `ecf2e52` | Freeze evaluation spec (freeze point 1) |
| `0e3f591` | `8908e34` | Move streaming code to prototype/ and set up the eval package |
| `38dde70` | `cb73d9b` | Add reference data loader and preserve the exploratory comparison |
| `08a3507` | `3126344` | Add incident format, injector, and author-side checks |
| `b92499d` | `be96dcc` | Add generic monitors that turn windows into alerts |
| `98e653b` | `1dcca35` | Add the incident authoring checker |
| `e97c94a` | `bb428eb` | Add the brief for the held-out incident author |
| `263d588` | `a3da739` | Commit held-out set hashes (freeze point 2) |
| `7692df1` | `7c770d2` | Add the rule-candidate contract |
| `d526e91` | `e2fbb3d` | Add the scorer |
| `d9f1e59` | `ddd7f55` | Add the dev incident set with reference answers |
| `d57c66e` | `4ab0d13` | Add the non-LLM arms, the harness, and the report |
| `17bb353` | `a6fb1b2` | Add arm A3-v1: the course system as submitted, plus code scoring |
| `0163d46` | `42f0d2b` | Reduce scope and add arm A3-sql |
| `d46566a` | `5daa603` | Tune A3-sql's prompt on the dev set |
| `ad2fed7` | `be69fab` | Add arm A4: a single tool-using agent |
| `70a2484` | `7eb1ef5` | Report why runs failed, derived from the scores |
| `5972edb` | `0c872e9` | Rewrite the spec to the current method and add a headline table |
| `7d3d379` | `6e1c2a4` | Record run metadata before any output is written **(tag `eval-freeze-m1`)** |
| `e3ed712` | `b06d31a` | Fix the failure table being overwritten in the report |
| `ae72bff` | `b8d3898` | Add milestone 1 results on held-out set S1 |
| `fffb808` | `e2ad77e` | Rewrite the README as a one-page evaluation report |
