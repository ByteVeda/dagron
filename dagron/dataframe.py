"""Optional DataFrame/Polars integration with schema validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dagron._internal import DAG
    from dagron.execution._types import ExecutionResult


@dataclass(frozen=True)
class ColumnSchema:
    """Schema definition for a single column."""

    name: str
    dtype: str | None = None
    nullable: bool = True
    required: bool = True


@dataclass(frozen=True)
class DataFrameSchema:
    """Schema definition for a DataFrame at an edge boundary."""

    columns: list[ColumnSchema] = field(default_factory=list)
    min_rows: int | None = None
    max_rows: int | None = None


@dataclass(frozen=True)
class SchemaViolation:
    """A single schema violation."""

    node_name: str
    message: str


def _detect_framework(df: Any) -> str | None:
    """Detect whether an object is a pandas or polars DataFrame."""
    type_name = type(df).__module__ + "." + type(df).__qualname__
    if "pandas" in type_name:
        return "pandas"
    if "polars" in type_name:
        return "polars"
    return None


def _get_columns(df: Any, framework: str) -> list[str]:
    """Get column names from a DataFrame."""
    if framework == "pandas":
        return list(df.columns)
    if framework == "polars":
        return list(df.columns)
    return []


def _get_dtype_str(df: Any, col: str, framework: str) -> str:
    """Get string representation of a column's dtype."""
    if framework == "pandas":
        return str(df[col].dtype)
    if framework == "polars":
        return str(df[col].dtype)
    return ""


def _get_row_count(df: Any, framework: str) -> int:
    """Get number of rows in a DataFrame."""
    if framework in ("pandas", "polars"):
        return len(df)
    return 0


def _has_nulls(df: Any, col: str, framework: str) -> bool:
    """Check if a column has null values."""
    if framework == "pandas":
        return bool(df[col].isnull().any())
    if framework == "polars":
        return bool(df[col].is_null().any())
    return False


def validate_schema(
    df: Any,
    schema: DataFrameSchema,
    node_name: str = "",
) -> list[SchemaViolation]:
    """Validate a DataFrame against a schema.

    Works with both pandas and polars DataFrames.

    Args:
        df: A pandas or polars DataFrame.
        schema: The expected schema.
        node_name: Name of the node (for error messages).

    Returns:
        List of schema violations. Empty means valid.
    """
    violations: list[SchemaViolation] = []

    framework = _detect_framework(df)
    if framework is None:
        violations.append(
            SchemaViolation(node_name, f"Expected DataFrame, got {type(df).__name__}")
        )
        return violations

    actual_cols = set(_get_columns(df, framework))

    for col_schema in schema.columns:
        if col_schema.required and col_schema.name not in actual_cols:
            violations.append(
                SchemaViolation(
                    node_name,
                    f"Missing required column '{col_schema.name}'",
                )
            )
            continue

        if col_schema.name not in actual_cols:
            continue

        if col_schema.dtype is not None:
            actual_dtype = _get_dtype_str(df, col_schema.name, framework)
            if col_schema.dtype.lower() not in actual_dtype.lower():
                violations.append(
                    SchemaViolation(
                        node_name,
                        f"Column '{col_schema.name}' expected dtype containing "
                        f"'{col_schema.dtype}', got '{actual_dtype}'",
                    )
                )

        if not col_schema.nullable and _has_nulls(df, col_schema.name, framework):
            violations.append(
                SchemaViolation(
                    node_name,
                    f"Column '{col_schema.name}' has null values but nullable=False",
                )
            )

    row_count = _get_row_count(df, framework)
    if schema.min_rows is not None and row_count < schema.min_rows:
        violations.append(
            SchemaViolation(
                node_name,
                f"Expected at least {schema.min_rows} rows, got {row_count}",
            )
        )

    if schema.max_rows is not None and row_count > schema.max_rows:
        violations.append(
            SchemaViolation(
                node_name,
                f"Expected at most {schema.max_rows} rows, got {row_count}",
            )
        )

    return violations


class DataFramePipeline:
    """Execute a DAG pipeline with schema validation at edge boundaries.

    Validates that each node's output DataFrame matches the expected
    schema before passing it to downstream nodes.

    Args:
        dag: The DAG defining the pipeline.
        schemas: Dict mapping node names to their output DataFrameSchema.

    Example::

        schemas = {
            "extract": DataFrameSchema(
                columns=[ColumnSchema("id", dtype="int"), ColumnSchema("name")],
                min_rows=1,
            ),
        }
        pipeline = DataFramePipeline(dag, schemas)
        violations = pipeline.validate_result(result)
    """

    def __init__(
        self,
        dag: DAG,
        schemas: dict[str, DataFrameSchema],
    ) -> None:
        self._dag = dag
        self._schemas = schemas

    def validate_result(self, result: ExecutionResult) -> list[SchemaViolation]:
        """Validate execution results against schemas.

        Args:
            result: The execution result containing node outputs.

        Returns:
            List of schema violations across all nodes.
        """
        from dagron.execution._types import NodeStatus

        violations: list[SchemaViolation] = []

        for node_name, schema in self._schemas.items():
            nr = result.node_results.get(node_name)
            if nr is None or nr.status != NodeStatus.COMPLETED:
                continue
            violations.extend(validate_schema(nr.result, schema, node_name))

        return violations

    def validate_value(self, node_name: str, value: Any) -> list[SchemaViolation]:
        """Validate a single value against its node's schema.

        Args:
            node_name: Name of the node.
            value: The value to validate.

        Returns:
            List of schema violations.
        """
        schema = self._schemas.get(node_name)
        if schema is None:
            return []
        return validate_schema(value, schema, node_name)
