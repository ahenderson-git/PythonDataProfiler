import datetime
import re

import pandas as pd

_NULL_STRINGS: frozenset = frozenset({
    "NULL", "null", "None", "N/A", "na", "n/a",
    "#N/A", "nan", "NaN", "", " ", "-",
})

_DATETIME_THRESHOLD: float = 0.80

_SPECIAL_CHAR_PATTERN = re.compile(r"[^\w\s.,!?@#$%]")


def _standardise_column_names(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict], list[dict]]:
    log: list[dict] = []
    detail: list[dict] = []
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
            log.append({
                "operation": "standardise_column_names",
                "column_name": original,
                "affected_count": 1,
                "description": f"Renamed column '{original}' \u2192 '{candidate}'",
            })
            detail.append({
                "original_row_number": "header",
                "column_name": original,
                "action": "rename_column",
                "original_value": original,
                "new_value": candidate,
            })

    if rename_map:
        df = df.rename(columns=rename_map)
    return df, log, detail


def _strip_whitespace(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict], list[dict]]:
    log: list[dict] = []
    detail: list[dict] = []
    for col in df.select_dtypes(include="object").columns:
        stripped = df[col].str.strip()
        mask = (stripped != df[col]).fillna(False)
        changed = mask.sum()
        if changed > 0:
            orig_vals = df.loc[mask, col]
            new_vals = stripped[mask]
            for rn, ov, nv in zip(orig_vals.index + 1, orig_vals, new_vals):
                detail.append({
                    "original_row_number": rn,
                    "column_name": col,
                    "action": "strip_whitespace",
                    "original_value": ov,
                    "new_value": nv,
                })
            df[col] = stripped
            log.append({
                "operation": "strip_whitespace",
                "column_name": col,
                "affected_count": int(changed),
                "description": f"'{col}': stripped whitespace from {changed} value(s)",
            })
    return df, log, detail


def _standardise_nulls(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict], list[dict]]:
    log: list[dict] = []
    detail: list[dict] = []
    for col in df.select_dtypes(include="object").columns:
        # Strip first so "  NULL  " matches "NULL"
        mask = df[col].str.strip().isin(_NULL_STRINGS)
        n = mask.sum()
        if n > 0:
            orig_vals = df.loc[mask, col]
            for rn, ov in zip(orig_vals.index + 1, orig_vals):
                detail.append({
                    "original_row_number": rn,
                    "column_name": col,
                    "action": "standardise_null",
                    "original_value": ov,
                    "new_value": "",
                })
            df[col] = df[col].where(~mask, other=pd.NA)
            log.append({
                "operation": "standardise_nulls",
                "column_name": col,
                "affected_count": int(n),
                "description": f"'{col}': replaced {n} null-like string(s) with NaN",
            })
    return df, log, detail


def _remove_special_characters(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict], list[dict]]:
    log: list[dict] = []
    detail: list[dict] = []
    for col in df.select_dtypes(include="object").columns:
        cleaned = df[col].str.replace(_SPECIAL_CHAR_PATTERN, "", regex=True)
        changed_mask = (cleaned != df[col]).fillna(False)
        changed = changed_mask.sum()
        if changed > 0:
            # Replace empty strings produced by removal with NaN
            empty_mask = cleaned.str.len() == 0
            cleaned = cleaned.where(~empty_mask, other=pd.NA)
            empty_count = empty_mask.fillna(False).sum()
            orig_vals = df.loc[changed_mask, col]
            new_vals = cleaned[changed_mask]
            for rn, ov, nv in zip(orig_vals.index + 1, orig_vals, new_vals):
                detail.append({
                    "original_row_number": rn,
                    "column_name": col,
                    "action": "remove_special_characters",
                    "original_value": ov,
                    "new_value": "" if pd.isna(nv) else nv,
                })
            df[col] = cleaned
            msg = f"'{col}': removed special characters from {changed} value(s)"
            if empty_count > 0:
                msg += f"; {empty_count} resulting empty string(s) converted to NaN"
            log.append({
                "operation": "remove_special_characters",
                "column_name": col,
                "affected_count": int(changed),
                "description": msg,
            })
    return df, log, detail


def _parse_datetimes(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict], list[dict]]:
    log: list[dict] = []
    detail: list[dict] = []
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
            before_series = df[col].copy()
            try:
                df[col] = pd.to_datetime(df[col], format="mixed", dayfirst=False, errors="coerce")
            except TypeError:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            changed_mask = df[col].notna() & before_series.notna()
            orig_vals = before_series[changed_mask]
            new_vals = df[col][changed_mask].dt.strftime("%Y-%m-%dT%H:%M:%S")
            for rn, ov, nv in zip(orig_vals.index + 1, orig_vals, new_vals):
                detail.append({
                    "original_row_number": rn,
                    "column_name": col,
                    "action": "parse_datetime",
                    "original_value": str(ov),
                    "new_value": nv,
                })
            log.append({
                "operation": "parse_datetimes",
                "column_name": col,
                "affected_count": int(converted.notna().sum()),
                "description": (
                    f"'{col}': detected as datetime ({parse_ratio:.0%} values parsed); "
                    f"converted from object to datetime64"
                ),
            })
    return df, log, detail


def _drop_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict], list[dict]]:
    detail: list[dict] = []
    dup_mask = df.duplicated(keep="first")
    dropped_rows = df[dup_mask]
    df = df.drop_duplicates()
    n_dropped = dup_mask.sum()
    for idx in dropped_rows.index:
        detail.append({
            "original_row_number": idx + 1,
            "column_name": "",
            "action": "drop_duplicate_row",
            "original_value": "",
            "new_value": "",
        })
    if n_dropped > 0:
        log = [{
            "operation": "drop_duplicates",
            "column_name": "",
            "affected_count": n_dropped,
            "description": f"Dropped {n_dropped} duplicate row(s); {len(df)} rows remain",
        }]
    else:
        log = [{
            "operation": "drop_duplicates",
            "column_name": "",
            "affected_count": 0,
            "description": "No duplicate rows found \u2014 nothing to drop",
        }]
    return df, log, detail


def clean_dataframe(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict], list[dict], str]:
    """Apply all cleaning steps and return (cleaned_df, summary_log, detail_log, run_timestamp).

    The original df is not mutated.
    summary_log: one dict per cleaning operation (operation, column_name, affected_count, description).
    detail_log:  one dict per changed cell or dropped row (original_row_number, column_name,
                 action, original_value, new_value, run_timestamp).
    run_timestamp: ISO 8601 string stamped at the start of this call.
    """
    run_ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    df = df.copy()
    log: list[dict] = []
    detail_log: list[dict] = []

    for fn in [
        _standardise_column_names,
        _strip_whitespace,
        _standardise_nulls,
        _parse_datetimes,
        _remove_special_characters,
        _drop_duplicates,
    ]:
        df, entries, detail_entries = fn(df)
        log.extend(entries)
        detail_log.extend(detail_entries)

    if not log:
        log.append({
            "operation": "no_changes",
            "column_name": "",
            "affected_count": 0,
            "description": "No issues found \u2014 data appears clean.",
        })

    for entry in detail_log:
        entry["run_timestamp"] = run_ts

    return df, log, detail_log, run_ts
