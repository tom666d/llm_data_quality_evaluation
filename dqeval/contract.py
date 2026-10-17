"""
contract.py
The rule-candidate contract (spec §6.1): its fields, its validation, and how a
candidate is applied to a table.

A candidate is a decision plus at most two SQL expressions over one row:
  RESTORE / STANDARDIZE  column := CASE WHEN scope THEN new_value ELSE column END
  QUARANTINE             _quarantine := scope (values are kept)
  KEEP                   nothing changes
Expressions are parsed with sqlglot and must use only allowlisted node types
and visible columns, so a candidate is row-level, has no side effects, and
cannot read other rows, tables, or files. They run in DuckDB with file and
network access disabled as a second line of defense.
"""

import json
from dataclasses import dataclass, field

import pandas as pd
import sqlglot
from sqlglot import exp

from . import data
from .incidents import connect

DECISIONS          = ("RESTORE", "STANDARDIZE", "QUARANTINE", "KEEP")
QUARANTINE_COLUMN  = "_quarantine"

# Node types an expression may contain: literals, columns, operators, and pure row-level functions
ALLOWED_NODES = {
    exp.Column, exp.Identifier, exp.Literal, exp.Null, exp.Boolean, exp.Paren, exp.DataType, exp.Interval, exp.Var,
    exp.Neg, exp.Not, exp.And, exp.Or,
    exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.NullSafeEQ, exp.NullSafeNEQ,
    exp.Is, exp.In, exp.Between, exp.Like, exp.ILike,
    exp.Add, exp.Sub, exp.Mul, exp.Div, exp.IntDiv, exp.Mod, exp.Pow, exp.DPipe,
    exp.Case, exp.If, exp.Coalesce, exp.Nullif, exp.Greatest, exp.Least, exp.Cast, exp.TryCast,
    exp.Abs, exp.Round, exp.Floor, exp.Ceil, exp.Sqrt, exp.Ln,
    exp.Lower, exp.Upper, exp.Trim, exp.Substring, exp.Length, exp.Replace, exp.Concat,
    exp.Contains, exp.StartsWith, exp.RegexpLike, exp.RegexpReplace, exp.RegexpExtract,
    exp.StrToTime, exp.TimeToStr, exp.TimestampTrunc,
}


@dataclass(frozen=True)
class RuleCandidate:
    decision:  str
    column:    str
    scope:     str | None = None
    new_value: str | None = None
    rationale: str = ""
    evidence:  tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_dict(cls, d: dict) -> "RuleCandidate":
        return cls(
            decision=str(d.get("decision", "")).upper(), column=d.get("column", ""),
            scope=d.get("scope") or None, new_value=d.get("new_value") or None,
            rationale=d.get("rationale", ""), evidence=tuple(d.get("evidence") or ()),
        )

    def to_dict(self) -> dict:
        return {"decision": self.decision, "column": self.column, "scope": self.scope,
                "new_value": self.new_value, "rationale": self.rationale, "evidence": list(self.evidence)}


def parse(raw: str | dict) -> RuleCandidate:
    """Builds a candidate from a model's JSON text or a dict. Raises ValueError on malformed JSON."""
    return RuleCandidate.from_dict(json.loads(raw) if isinstance(raw, str) else raw)


def expression_problems(expression: str) -> list[str]:
    """Why an SQL expression is not a single, allowlisted, row-level expression over visible columns."""
    try:
        statements = sqlglot.parse(f"SELECT {expression}", read="duckdb")
    except sqlglot.errors.ParseError as e:
        return [f"does not parse: {str(e).splitlines()[0]}"]
    if len(statements) != 1:
        return ["must be a single expression, not several statements"]
    root = statements[0]
    if not isinstance(root, exp.Select) or len(root.expressions) != 1 or \
            {k for k, v in root.args.items() if v} != {"expressions"}:
        return ["must be a single expression"]

    problems = set()
    for node in root.expressions[0].walk():
        if type(node) not in ALLOWED_NODES:
            name = node.name if isinstance(node, exp.Anonymous) else type(node).__name__
            problems.add(f"{name} is not allowed")
        elif isinstance(node, exp.Column):
            if node.table:
                problems.add(f"column {node.sql()} must not be qualified")
            elif node.name not in data.VISIBLE_COLUMNS:
                problems.add(f"column {node.name} is not visible")
    return sorted(problems)


def validate(c: RuleCandidate, expected_column: str) -> list[str]:
    """Contract problems; an empty list means the candidate may be applied."""
    if c.decision not in DECISIONS:
        return [f"decision must be one of {', '.join(DECISIONS)}"]
    problems = []
    if c.column != expected_column:
        problems.append(f"column must be the alerted column {expected_column!r}")
    needs_scope = c.decision != "KEEP"
    needs_value = c.decision in ("RESTORE", "STANDARDIZE")
    if needs_scope and not c.scope:
        problems.append(f"{c.decision} needs a scope")
    if needs_value and not c.new_value:
        problems.append(f"{c.decision} needs a new_value")
    if not needs_value and c.new_value:
        problems.append(f"{c.decision} must not set new_value")
    if c.decision == "KEEP" and c.scope:
        problems.append("KEEP must not set a scope")
    for name in ("scope", "new_value"):
        expression = getattr(c, name)
        if expression:
            problems += [f"{name}: {p}" for p in expression_problems(expression)]
    return problems


def apply(table: pd.DataFrame, c: RuleCandidate) -> pd.DataFrame:
    """
    Returns a copy of `table` with the candidate applied and a boolean _quarantine column.
    The rewritten column keeps its type; a value that cannot be cast raises, which the
    scorer counts as an execution failure. Call validate() first.
    """
    con = connect()
    con.register("t", table)
    col = f'"{c.column}"'
    if c.decision in ("RESTORE", "STANDARDIZE"):
        col_type = {row[0]: row[1] for row in con.execute("DESCRIBE t").fetchall()}[c.column]
        query = (f"SELECT * REPLACE (CAST(CASE WHEN ({c.scope}) THEN ({c.new_value}) ELSE {col} END "
                 f"AS {col_type}) AS {col}), false AS {QUARANTINE_COLUMN} FROM t")
    elif c.decision == "QUARANTINE":
        query = f"SELECT *, coalesce(({c.scope}), false) AS {QUARANTINE_COLUMN} FROM t"
    else:
        query = f"SELECT *, false AS {QUARANTINE_COLUMN} FROM t"
    return con.execute(query).df()
