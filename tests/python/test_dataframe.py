"""Tests for DataFrame/Polars Integration."""

import pytest

from dagron.dataframe import (
    ColumnSchema,
    DataFrameSchema,
    validate_schema,
)


def _has_pandas():
    try:
        import pandas  # noqa: F401
        return True
    except ImportError:
        return False


def _has_polars():
    try:
        import polars  # noqa: F401
        return True
    except ImportError:
        return False


class TestSchemaValidation:
    def test_non_dataframe(self):
        schema = DataFrameSchema(columns=[ColumnSchema("a")])
        violations = validate_schema({"a": 1}, schema, "test")
        assert len(violations) == 1
        assert "Expected DataFrame" in violations[0].message

    @pytest.mark.skipif(
        not _has_pandas(), reason="pandas not installed"
    )
    def test_pandas_valid(self):
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        schema = DataFrameSchema(
            columns=[ColumnSchema("id"), ColumnSchema("name")],
        )
        violations = validate_schema(df, schema, "test")
        assert violations == []

    @pytest.mark.skipif(
        not _has_pandas(), reason="pandas not installed"
    )
    def test_pandas_missing_column(self):
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2]})
        schema = DataFrameSchema(
            columns=[ColumnSchema("id"), ColumnSchema("name", required=True)],
        )
        violations = validate_schema(df, schema, "test")
        assert any("Missing required column 'name'" in v.message for v in violations)

    @pytest.mark.skipif(
        not _has_pandas(), reason="pandas not installed"
    )
    def test_pandas_optional_column(self):
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2]})
        schema = DataFrameSchema(
            columns=[ColumnSchema("id"), ColumnSchema("name", required=False)],
        )
        violations = validate_schema(df, schema, "test")
        assert violations == []

    @pytest.mark.skipif(
        not _has_pandas(), reason="pandas not installed"
    )
    def test_pandas_row_count(self):
        import pandas as pd

        df = pd.DataFrame({"id": [1, 2]})
        schema = DataFrameSchema(min_rows=5)
        violations = validate_schema(df, schema, "test")
        assert any("at least 5 rows" in v.message for v in violations)

    @pytest.mark.skipif(
        not _has_pandas(), reason="pandas not installed"
    )
    def test_pandas_max_rows(self):
        import pandas as pd

        df = pd.DataFrame({"id": list(range(100))})
        schema = DataFrameSchema(max_rows=10)
        violations = validate_schema(df, schema, "test")
        assert any("at most 10 rows" in v.message for v in violations)

    @pytest.mark.skipif(
        not _has_pandas(), reason="pandas not installed"
    )
    def test_pandas_dtype_check(self):
        import pandas as pd

        df = pd.DataFrame({"id": ["a", "b"]})
        schema = DataFrameSchema(
            columns=[ColumnSchema("id", dtype="int")],
        )
        violations = validate_schema(df, schema, "test")
        assert any("dtype" in v.message for v in violations)

    @pytest.mark.skipif(
        not _has_pandas(), reason="pandas not installed"
    )
    def test_pandas_nullable_check(self):
        import numpy as np
        import pandas as pd

        df = pd.DataFrame({"id": [1, np.nan]})
        schema = DataFrameSchema(
            columns=[ColumnSchema("id", nullable=False)],
        )
        violations = validate_schema(df, schema, "test")
        assert any("null values" in v.message for v in violations)
