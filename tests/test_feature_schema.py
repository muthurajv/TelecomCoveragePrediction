from src.features.feature_schema import (
    ALL_FEATURE_COLS,
    BOOL_FEATURE_COLS,
    NUMERIC_FEATURE_COLS,
    SYNTHETIC_FEATURE_COLS,
    FEATURE_SPECS,
    validate_feature_row,
)


def test_all_feature_cols_non_empty():
    assert len(ALL_FEATURE_COLS) > 0


def test_no_duplicate_feature_names():
    assert len(ALL_FEATURE_COLS) == len(set(ALL_FEATURE_COLS))


def test_numeric_and_bool_cols_are_disjoint():
    assert not set(NUMERIC_FEATURE_COLS) & set(BOOL_FEATURE_COLS)


def test_synthetic_cols_are_subset_of_all():
    assert set(SYNTHETIC_FEATURE_COLS).issubset(set(ALL_FEATURE_COLS))


def test_validate_feature_row_passes_for_empty_nullable_row():
    row = {col: None for col in ALL_FEATURE_COLS}
    errors = validate_feature_row(row)
    assert errors == [], f"Unexpected errors: {errors}"


def test_validate_feature_row_catches_missing_required():
    required = [s.name for s in FEATURE_SPECS if not s.nullable]
    if not required:
        return
    row = {}
    errors = validate_feature_row(row)
    for name in required:
        assert any(name in e for e in errors)
