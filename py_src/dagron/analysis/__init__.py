"""Analysis subpackage — read-only DAG inspection tools."""

from dagron.analysis.explain import NodeExplanation, WhatIfResult, explain, what_if
from dagron.analysis.linting import DAGSchema, LintReport, LintSeverity, LintWarning, lint
from dagron.analysis.query import query

__all__ = [
    "DAGSchema",
    "LintReport",
    "LintSeverity",
    "LintWarning",
    "NodeExplanation",
    "WhatIfResult",
    "explain",
    "lint",
    "query",
    "what_if",
]
