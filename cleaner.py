import re

import pandas as pd

_NULL_STRINGS: frozenset = frozenset({
    "NULL", "null", "None", "N/A", "na", "n/a",
    "#N/A", "nan", "NaN", "", " ", "-",
})

_DATETIME_THRESHOLD: float = 0.80

_SPECIAL_CHAR_PATTERN = re.compile(r"[^\w\s.,!?@#$%]")


def _standardise_column_names(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    log: list[str] = []
    seen: dict[str, int] = {}
    rename_map: dict[str, str] = {}

    for i, original in enumerate(df.columns):
        cleaned = re.sub(r"[^a-z0-9]+", "_", str(original).lower()).strip("_")
        if not cleaned:
            cleaned = f"col_{i}"

        # Resolve collisions
        candidate = cleaned
        while candidate in seen.values() or (candidate in df.columns and candidate != original):
            n = seen.get(cleaned, 1) + 1
            seen[cleaned] = n
            candidate = f"{cleaned}_{n}"
        seen[cleaned] = seen.get(cleaned, 1)

        if candidate != original:
            rename_map[original] = candidate
            log.append(f"Renamed column '{original}' → '{candidate}'")

    if rename_map:
        df = df.rename(columns=rename_map)
    return df, log


def _strip_whitespace(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    log: list[str] = []
    for col in df.select_dtypes(include="object").columns:
        stripped = df[col].str.strip()
        changed = (stripped != df[col]).fillna(False).sum()
        if changed > 0:
            df[col] = stripped
            log.append(f"'{col}': stripped whitespace from {changed} value(s)")
    return df, log


def _standardise_nulls(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    log: list[str] = []
    for col in df.select_dtypes(include="object").columns:
        # Strip first so "  NULL  " matches "NULL"
        mask = df[col].str.strip().isin(_NULL_STRINGS)
        n = mask.sum()
        if n > 0:
            df[col] = df[col].where(~mask, other=pd.NA)
            log.append(f"'{col}': replaced {n} null-like string(s) with NaN")
    return df, log


def _remove_special_characters(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    log: list[str] = []
    for col in df.select_dtypes(include="object").columns:
        cleaned = df[col].str.replace(_SPECIAL_CHAR_PATTERN, "", regex=True)
        changed = (cleaned != df[col]).fillna(False).sum()
        if changed > 0:
            # Replace empty strings produced by removal with NaN
            empty_mask = cleaned.str.len() == 0
            cleaned = cleaned.where(~empty_mask, other=pd.NA)
            empty_count = empty_mask.fillna(False).sum()
            df[col] = cleaned
            msg = f"'{col}': removed special characters from {changed} value(s)"
            if empty_count > 0:
                msg += f"; {empty_count} resulting empty string(s) converted to NaN"
            log.append(msg)
    return df, log


def _parse_datetimes(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    log: list[str] = []
    for col in df.select_dtypes(include="object").columns:
        non_null = df[col].dropna()
        if len(non_null) == 0:
            continue
        # Guard: require at least one date-separator character
        has_separator = non_null.str.contains(r"[-/.\s]", regex=True, na=False).any()
        if not has_separator:
            continue
        try:
            converted = pd.to_datetime(non_null, format="mixed", dayfirst=False, errors="coerce")
        except TypeError:
            converted = pd.to_datetime(non_null, errors="coerce")
        parse_ratio = converted.notna().sum() / len(non_null)
        if parse_ratio >= _DATETIME_THRESHOLD:
            try:
                df[col] = pd.to_datetime(df[col], format="mixed", dayfirst=False, errors="coerce")
            except TypeError:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            log.append(
                f"'{col}': detected as datetime ({parse_ratio:.0%} values parsed); "
                f"converted from object to datetime64"
            )
    return df, log


def _drop_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    n_before = len(df)
    df = df.drop_duplicates()
    n_dropped = n_before - len(df)
    if n_dropped > 0:
        log = [f"Dropped {n_dropped} duplicate row(s); {len(df)} rows remain"]
    else:
        log = ["No duplicate rows found — nothing to drop"]
    return df, log


def clean_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Apply all cleaning steps and return (cleaned_df, cleaning_log).

    The original df is not mutated. cleaning_log is a list of human-readable
    strings describing each change made.
    """
    df = df.copy()
    log: list[str] = []

    for fn in [
        _standardise_column_names,
        _strip_whitespace,
        _standardise_nulls,
        _parse_datetimes,
        _remove_special_characters,
        _drop_duplicates,
    ]:
        df, entries = fn(df)
        log.extend(entries)

    if not log:
        log.append("No issues found — data appears clean.")

    return df, log
