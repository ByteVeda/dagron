"""Centralized monkey-patches for DAG convenience methods."""


def apply_patches() -> None:
    from dagron._internal import DAG
    from dagron.analysis import explain, lint, query, track_lineage, what_if
    from dagron.display import _repr_svg_, pretty_print
    from dagron.integration import from_records

    DAG.from_records = staticmethod(from_records)  # type: ignore[method-assign]
    DAG.pretty_print = lambda self, **kw: pretty_print(self, **kw)  # type: ignore[method-assign]
    DAG._repr_svg_ = lambda self: _repr_svg_(self)  # type: ignore[method-assign]
    DAG.explain = lambda self, node, costs=None: explain(self, node, costs)  # type: ignore[method-assign]
    DAG.what_if = lambda self, **kw: what_if(self, **kw)  # type: ignore[method-assign]
    DAG.lint = lambda self, **kw: lint(self, **kw)  # type: ignore[method-assign]
    DAG.query = lambda self, expr: query(self, expr)  # type: ignore[method-assign]
    DAG.track_lineage = lambda self, result: track_lineage(self, result)  # type: ignore[attr-defined]
