"""
arms
The systems under test (spec §6.2). Every arm receives the same Case for an alert
and returns a Proposal holding one rule candidate.
"""

from dataclasses import dataclass, field

import pandas as pd

from ..contract import RuleCandidate


@dataclass
class Case:
    """What a system under test sees for one incident: no ground truth, no case type, no incident id."""
    column:   str
    alerts:   list[dict]       # the monitors' alerts on this column
    window:   pd.DataFrame     # the injected window, visible columns only
    baseline: pd.DataFrame     # nearby clean windows of the same split, visible columns only

    @property
    def window_start(self) -> str:
        return str(self.window["ingested_at"].min())

    @property
    def window_end(self) -> str:
        return str(self.window["ingested_at"].max())


@dataclass
class Proposal:
    candidate:     RuleCandidate | None
    error:         str | None = None
    code:          str | None = None      # PySpark arms return code instead of a contract candidate
    llm_calls:     int = 0
    input_tokens:  int = 0
    output_tokens: int = 0
    trace:         list = field(default_factory=list)   # rule notes or raw model messages, saved with the results


class Arm:
    name     = "base"
    uses_llm = False

    def propose(self, case: Case) -> Proposal:
        raise NotImplementedError


class NoChange(Arm):
    """A0: always KEEP."""
    name = "A0"

    def propose(self, case: Case) -> Proposal:
        return Proposal(RuleCandidate("KEEP", case.column))


class QuarantineAll(Arm):
    """A1: quarantine every row in the alert window."""
    name = "A1"

    def propose(self, case: Case) -> Proposal:
        scope = f"ingested_at BETWEEN TIMESTAMP '{case.window_start}' AND TIMESTAMP '{case.window_end}'"
        return Proposal(RuleCandidate("QUARANTINE", case.column, scope))
