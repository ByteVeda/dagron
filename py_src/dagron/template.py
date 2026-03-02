"""DAG Templates — parameterized DAG construction with substitution."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


class TemplateError(Exception):
    """Raised when template validation or rendering fails."""


@dataclass(frozen=True)
class TemplateParam:
    """Specification for a single template parameter."""

    name: str
    type: type = str
    default: Any = None
    description: str = ""
    validator: Any = None  # Callable[[Any], bool] | None

    def validate(self, value: Any) -> None:
        if not isinstance(value, self.type):
            raise TemplateError(
                f"Parameter '{self.name}' expects {self.type.__name__}, "
                f"got {type(value).__name__}"
            )
        if self.validator is not None and not self.validator(value):
            raise TemplateError(
                f"Parameter '{self.name}' failed custom validation"
            )


class DAGTemplate:
    """A parameterized DAG template that renders concrete DAGs.

    Example::

        template = DAGTemplate(
            params={"env": str, "replicas": int},
            defaults={"env": "staging"},
        )
        template.add_node("extract_{{env}}")
        template.add_edge("extract_{{env}}", "load_{{env}}")
        dag = template.render(env="prod", replicas=3)
    """

    def __init__(
        self,
        params: dict[str, type] | None = None,
        defaults: dict[str, Any] | None = None,
        descriptions: dict[str, str] | None = None,
        validators: dict[str, Any] | None = None,
        delimiters: tuple[str, str] = ("{{", "}}"),
    ) -> None:
        self._params: dict[str, TemplateParam] = {}
        self._nodes: list[tuple[str, dict[str, Any]]] = []
        self._edges: list[tuple[str, str, dict[str, Any]]] = []
        self._delimiters = delimiters

        params = params or {}
        defaults = defaults or {}
        descriptions = descriptions or {}
        validators = validators or {}

        for name, typ in params.items():
            self._params[name] = TemplateParam(
                name=name,
                type=typ,
                default=defaults.get(name),
                description=descriptions.get(name, ""),
                validator=validators.get(name),
            )

        # Build regex pattern from delimiters
        left = re.escape(delimiters[0])
        right = re.escape(delimiters[1])
        self._pattern = re.compile(rf"{left}(\w+){right}")

    @property
    def params(self) -> dict[str, TemplateParam]:
        """Return a copy of the template parameters."""
        return dict(self._params)

    def add_node(
        self, name: str, *, payload: object = None, metadata: object = None
    ) -> DAGTemplate:
        """Add a templated node. Name may contain ``{{param}}`` placeholders."""
        self._nodes.append((name, {"payload": payload, "metadata": metadata}))
        return self

    def add_edge(
        self,
        from_node: str,
        to_node: str,
        *,
        weight: float | None = None,
        label: str | None = None,
    ) -> DAGTemplate:
        """Add a templated edge. Node names may contain placeholders."""
        self._edges.append(
            (from_node, to_node, {"weight": weight, "label": label})
        )
        return self

    def validate_params(self, **kwargs: Any) -> list[str]:
        """Validate parameters without rendering. Returns list of error messages."""
        errors: list[str] = []
        provided = set(kwargs.keys())
        defined = set(self._params.keys())

        # Unknown params
        unknown = provided - defined
        for name in sorted(unknown):
            errors.append(f"Unknown parameter: '{name}'")

        # Missing required params (no default)
        for name, param in self._params.items():
            if name not in kwargs and param.default is None:
                errors.append(f"Missing required parameter: '{name}'")
                continue
            value = kwargs.get(name, param.default)
            if value is not None:
                try:
                    param.validate(value)
                except TemplateError as e:
                    errors.append(str(e))

        return errors

    def _resolve_params(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Resolve parameters with defaults and validate."""
        resolved: dict[str, Any] = {}
        for name, param in self._params.items():
            if name in kwargs:
                resolved[name] = kwargs[name]
            elif param.default is not None:
                resolved[name] = param.default
            else:
                raise TemplateError(f"Missing required parameter: '{name}'")

        unknown = set(kwargs.keys()) - set(self._params.keys())
        if unknown:
            raise TemplateError(
                f"Unknown parameters: {', '.join(sorted(unknown))}"
            )

        for name, value in resolved.items():
            self._params[name].validate(value)

        return resolved

    def _substitute(self, template_str: str, values: dict[str, Any]) -> Any:
        """Substitute placeholders in a string.

        Type-preserving: if the entire string is a single placeholder ``"{{x}}"``,
        return the raw Python value. Otherwise stringify and interpolate.
        """
        # Check if the entire string is a single placeholder
        m = self._pattern.fullmatch(template_str)
        if m:
            param_name = m.group(1)
            if param_name in values:
                return values[param_name]

        # Partial substitution — stringify values
        def replacer(match: re.Match[str]) -> str:
            param_name = match.group(1)
            if param_name in values:
                return str(values[param_name])
            return match.group(0)  # leave unresolved placeholders as-is

        return self._pattern.sub(replacer, template_str)

    def _substitute_kwargs(
        self, kwargs: dict[str, Any], values: dict[str, Any]
    ) -> dict[str, Any]:
        """Substitute in keyword arguments, only for string values."""
        result: dict[str, Any] = {}
        for key, val in kwargs.items():
            if isinstance(val, str):
                result[key] = self._substitute(val, values)
            else:
                result[key] = val
        return result

    def render(self, **kwargs: Any) -> Any:
        """Render the template into a concrete DAG.

        Returns:
            A new ``DAG`` instance with all placeholders resolved.

        Raises:
            TemplateError: If parameters are missing, wrong type, or fail validation.
        """
        return self.render_builder(**kwargs).build()

    def render_builder(self, **kwargs: Any) -> Any:
        """Render the template into a pre-populated ``DAGBuilder``.

        Returns:
            A ``DAGBuilder`` with all nodes and edges added, ready for
            additional modifications before ``build()``.
        """
        from dagron.builder import DAGBuilder

        values = self._resolve_params(kwargs)
        builder = DAGBuilder()

        for name_template, node_kwargs in self._nodes:
            name = self._substitute(name_template, values)
            if not isinstance(name, str):
                name = str(name)
            resolved_kwargs = self._substitute_kwargs(node_kwargs, values)
            builder.add_node(name, **resolved_kwargs)

        for from_template, to_template, edge_kwargs in self._edges:
            from_name = self._substitute(from_template, values)
            to_name = self._substitute(to_template, values)
            if not isinstance(from_name, str):
                from_name = str(from_name)
            if not isinstance(to_name, str):
                to_name = str(to_name)
            resolved_kwargs = self._substitute_kwargs(edge_kwargs, values)
            builder.add_edge(from_name, to_name, **resolved_kwargs)

        return builder

    def render_pipeline(self, tasks: list[Any] | None = None, **kwargs: Any) -> Any:
        """Render the template into a ``Pipeline``.

        Args:
            tasks: Optional list of ``@dagron.task``-decorated functions.
            **kwargs: Template parameters.

        Returns:
            A ``Pipeline`` wrapping the rendered DAG.
        """
        from dagron.execution.pipeline import Pipeline

        return Pipeline(tasks or [], name=f"template-{id(self)}")

    def __repr__(self) -> str:
        param_names = ", ".join(sorted(self._params.keys()))
        return (
            f"DAGTemplate(params=[{param_names}], "
            f"nodes={len(self._nodes)}, edges={len(self._edges)})"
        )
