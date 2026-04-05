import datetime
import re
from functools import partial

import polars as pl

from constants import DATETIME_PARSE_THRESHOLD

_NULL_STRINGS: frozenset = frozenset({
    "NULL", "null", "None", "N/A", "na", "n/a",
    "#N/A", "nan", "NaN", "", " ", "-",
})

# Default allowlist for the special-character removal step.  Used as the
# default value for the pattern_str parameter.
_DEFAULT_EXTRA_CHARS = r".,!?@#$%"
_DEFAULT_SPECIAL_CHAR_PATTERN = rf"[^\w\s{re.escape(_DEFAULT_EXTRA_CHARS)}]"

# Internal column name injected for audit-log row tracking.  Must not collide
# with any real column name in the user's data.
_ROW_NR_COL = "__row_nr__"


def _string_cols(df: pl.DataFrame) -> list[str]:
    """Return names of Utf8/String columns, excluding the internal row-index column."""
    return [
        col for col in df.columns
        if df[col].dtype in (pl.Utf8, pl.String) and col != _ROW_NR_COL
    ]


def _standardise_column_names(df: pl.DataFrame) -> tuple[pl.DataFrame, list[dict], list[dict]]:
    log: list[dict] = []
    detail: list[dict] = []
    seen: dict[str, int] = {}
    rename_map: dict[str, str] = {}

    for i, original in enumerate(df.columns):
        # Never rename the internal row-index column
        if original == _ROW_NR_COL:
            continue

        cleaned = re.sub(r"[^a-z0-9]+", "_", str(original).lower()).strip("_")
        if not cleaned:
            cleaned = f"col_{i}"

        # Ensure we never accidentally produce a name that collides with _ROW_NR_COL
        if cleaned == _ROW_NR_COL:
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
        df = df.rename(rename_map)
    return df, log, detail


def _strip_whitespace(df: pl.DataFrame) -> tuple[pl.DataFrame, list[dict], list[dict]]:
    log: list[dict] = []
    detail: list[dict] = []

    for col in _string_cols(df):
        original = df[col]
        stripped = original.str.strip_chars()
        # Changed = both sides not-null and values differ
        changed_mask = original.is_not_null() & stripped.is_not_null() & (original != stripped)
        changed_count = int(changed_mask.sum())

        if changed_count > 0:
            changed_rows = df.filter(changed_mask).select([_ROW_NR_COL, col])
            new_vals = stripped.filter(changed_mask).to_list()

            for (row_nr, orig_val), new_val in zip(changed_rows.iter_rows(), new_vals):
                detail.append({
                    "original_row_number": int(row_nr) + 1,
                    "column_name": col,
                    "action": "strip_whitespace",
                    "original_value": orig_val,
                    "new_value": new_val,
                })

            df = df.with_columns(stripped.alias(col))
            log.append({
                "operation": "strip_whitespace",
                "column_name": col,
                "affected_count": changed_count,
                "description": f"'{col}': stripped whitespace from {changed_count} value(s)",
            })

    return df, log, detail


def _standardise_nulls(df: pl.DataFrame) -> tuple[pl.DataFrame, list[dict], list[dict]]:
    log: list[dict] = []
    detail: list[dict] = []

    for col in _string_cols(df):
        series = df[col]
        # Strip first so "  NULL  " matches "NULL"
        mask = series.str.strip_chars().is_in(list(_NULL_STRINGS))
        n = int(mask.sum())

        if n > 0:
            changed_rows = df.filter(mask).select([_ROW_NR_COL, col])
            for row_nr, orig_val in changed_rows.iter_rows():
                detail.append({
                    "original_row_number": int(row_nr) + 1,
                    "column_name": col,
                    "action": "standardise_null",
                    "original_value": orig_val,
                    "new_value": "",
                })

            df = df.with_columns(
                pl.when(mask).then(None).otherwise(series).alias(col)
            )
            log.append({
                "operation": "standardise_nulls",
                "column_name": col,
                "affected_count": n,
                "description": f"'{col}': replaced {n} null-like string(s) with null",
            })

    return df, log, detail


def _remove_special_characters(
    df: pl.DataFrame,
    pattern_str: str = _DEFAULT_SPECIAL_CHAR_PATTERN,
) -> tuple[pl.DataFrame, list[dict], list[dict]]:
    log: list[dict] = []
    detail: list[dict] = []

    for col in _string_cols(df):
        series = df[col]
        cleaned = series.str.replace_all(pattern_str, "")
        # Changed = not null and value differs after stripping
        changed_mask = series.is_not_null() & (series != cleaned)
        changed_count = int(changed_mask.sum())

        if changed_count > 0:
            # Replace empty strings produced by removal with null
            empty_mask = cleaned.str.len_chars() == 0
            cleaned_with_nulls = pl.when(empty_mask & cleaned.is_not_null()).then(None).otherwise(cleaned)
            empty_count = int((empty_mask & cleaned.is_not_null()).sum())

            changed_rows = df.filter(changed_mask).select([_ROW_NR_COL, col])
            # cleaned_with_nulls is an Expr (used later in with_columns); extract new
            # values from the Series directly, mirroring the empty-string → None rule.
            new_vals = [None if v == "" else v for v in cleaned.filter(changed_mask).to_list()]

            for (row_nr, orig_val), new_val in zip(changed_rows.iter_rows(), new_vals):
                detail.append({
                    "original_row_number": int(row_nr) + 1,
                    "column_name": col,
                    "action": "remove_special_characters",
                    "original_value": orig_val,
                    "new_value": "" if new_val is None else new_val,
                })

            df = df.with_columns(cleaned_with_nulls.alias(col))
            msg = f"'{col}': removed special characters from {changed_count} value(s)"
            if empty_count > 0:
                msg += f"; {empty_count} resulting empty string(s) converted to null"
            log.append({
                "operation": "remove_special_characters",
                "column_name": col,
                "affected_count": changed_count,
                "description": msg,
            })

    return df, log, detail


def _has_ambiguous_dates(series: pl.Series) -> bool:
    """Return True if any value could be ambiguously day-first or month-first.

    Detects patterns like '01/05/2023' where both the first and second numeric
    components are ≤ 12, making DD/MM vs MM/DD interpretation unclear.

    Note: Polars datetime parsing has no dayfirst parameter, so we flag
    ambiguity heuristically by inspecting the raw string values.
    """
    try:
        # Match "XX/XX/YYYY" or "XX-XX-YYYY" or "XX.XX.YYYY" where both parts ≤ 12
        ambig = series.drop_nulls().str.contains(
            r"^(0?[1-9]|1[0-2])[/\-\.](0?[1-9]|1[0-2])[/\-\.]\d{2,4}$"
        )
        return bool(ambig.any())
    except Exception:
        return False


def _parse_datetimes(
    df: pl.DataFrame,
    dayfirst: bool = False,
) -> tuple[pl.DataFrame, list[dict], list[dict]]:
    """Attempt to convert object columns whose values look like datetimes.

    Polars datetime parsing does not support per-row mixed formats or a
    ``dayfirst`` parameter.  We use ``format=None, strict=False`` which infers
    a single format from the first non-null value and applies it to the whole
    column; rows that do not match are silently left as null.
    """
    log: list[dict] = []
    detail: list[dict] = []

    for col in _string_cols(df):
        series = df[col]
        non_null = series.drop_nulls()

        if len(non_null) == 0:
            continue

        # Guard: require at least one date-separator character
        has_separator = non_null.str.contains(r"[-/.\s]").any()
        if not has_separator:
            continue

        # Probe on non-null values to measure parse success rate
        try:
            converted = non_null.str.to_datetime(format=None, strict=False)
        except Exception:
            continue

        parse_ratio = converted.is_not_null().sum() / len(non_null)
        if parse_ratio < DATETIME_PARSE_THRESHOLD:
            continue

        # Apply the conversion to the full column (nulls remain null)
        before_series = series
        try:
            new_series = series.str.to_datetime(format=None, strict=False)
        except Exception:
            continue

        # Audit rows where a string was successfully parsed
        changed_mask = new_series.is_not_null() & before_series.is_not_null()
        changed_rows = df.filter(changed_mask).select([_ROW_NR_COL, col])
        new_dt_vals = new_series.filter(changed_mask).to_list()  # Python datetimes

        for (row_nr, orig_val), new_val in zip(changed_rows.iter_rows(), new_dt_vals):
            detail.append({
                "original_row_number": int(row_nr) + 1,
                "column_name": col,
                "action": "parse_datetime",
                "original_value": str(orig_val),
                "new_value": new_val.strftime("%Y-%m-%dT%H:%M:%S") if new_val else "",
            })

        order_label = "day-first" if dayfirst else "month-first"
        ambiguity_note = ""
        if not dayfirst and _has_ambiguous_dates(non_null):
            ambiguity_note = (
                " WARNING: ambiguous date values detected — month-first assumed "
                "(e.g. 01/02 → January 2nd). Pass dayfirst=True if dates are day-first."
            )

        df = df.with_columns(new_series.alias(col))
        log.append({
            "operation": "parse_datetimes",
            "column_name": col,
            "affected_count": int(converted.is_not_null().sum()),
            "description": (
                f"'{col}': detected as datetime ({parse_ratio:.0%} values parsed, {order_label}); "
                f"converted from Utf8 to Datetime.{ambiguity_note}"
            ),
        })

    return df, log, detail


def _drop_duplicates(df: pl.DataFrame) -> tuple[pl.DataFrame, list[dict], list[dict]]:
    detail: list[dict] = []

    n_before = len(df)
    # Deduplicate on all columns except the internal row-index, keeping first occurrence
    data_cols = [col for col in df.columns if col != _ROW_NR_COL]
    df_deduped = df.unique(subset=data_cols, keep="first", maintain_order=True)
    n_dropped = n_before - len(df_deduped)

    if n_dropped > 0:
        kept = set(df_deduped[_ROW_NR_COL].to_list())
        dropped_row_nrs = sorted(set(df[_ROW_NR_COL].to_list()) - kept)
        for row_nr in dropped_row_nrs:
            detail.append({
                "original_row_number": int(row_nr) + 1,
                "column_name": "",
                "action": "drop_duplicate_row",
                "original_value": "",
                "new_value": "",
            })
        log = [{
            "operation": "drop_duplicates",
            "column_name": "",
            "affected_count": n_dropped,
            "description": f"Dropped {n_dropped} duplicate row(s); {len(df_deduped)} rows remain",
        }]
    else:
        log = [{
            "operation": "drop_duplicates",
            "column_name": "",
            "affected_count": 0,
            "description": "No duplicate rows found \u2014 nothing to drop",
        }]

    return df_deduped, log, detail


def clean_dataframe(
    df: pl.DataFrame,
    extra_chars: str = r".,!?@#$%",
    dayfirst: bool = False,
) -> tuple[pl.DataFrame, list[dict], list[dict], str]:
    """Apply all cleaning steps and return (cleaned_df, summary_log, detail_log, run_timestamp).

    The original df is not mutated.
    summary_log: one dict per cleaning operation (operation, column_name, affected_count, description).
    detail_log:  one dict per changed cell or dropped row (original_row_number, column_name,
                 action, original_value, new_value, run_timestamp).
    run_timestamp: ISO 8601 string stamped at the start of this call.

    extra_chars: characters (beyond word chars and whitespace) that are *kept* during special-
                 character removal.  Default is ``.,!?@#$%``.  Pass e.g. ``r".,!?@#$%/-()"``
                 to also preserve slashes, hyphens and parentheses.
    dayfirst: hint for datetime ambiguity warnings.  Polars datetime parsing has no dayfirst
              parameter; this flag only affects the warning message in the audit log.
    """
    run_ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # Inject a 0-based row index so every cleaning step can record the original
    # row number in the audit log.  The column is stripped before returning.
    df = df.with_row_index(_ROW_NR_COL)

    log: list[dict] = []
    detail_log: list[dict] = []

    special_char_pattern_str = rf"[^\w\s{re.escape(extra_chars)}]"

    for fn in [
        _standardise_column_names,
        _strip_whitespace,
        _standardise_nulls,
        partial(_parse_datetimes, dayfirst=dayfirst),
        partial(_remove_special_characters, pattern_str=special_char_pattern_str),
        _drop_duplicates,
    ]:
        df, entries, detail_entries = fn(df)
        log.extend(entries)
        detail_log.extend(detail_entries)

    # Remove the internal tracking column before returning
    df = df.drop(_ROW_NR_COL)

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
