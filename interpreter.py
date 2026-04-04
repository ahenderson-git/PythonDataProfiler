from itertools import combinations


def interpret_profile(profile: dict, df=None) -> dict:
    """
    Analyse a profile dict (from profile_dataframe()) and return plain-English findings.

    Returns:
        {
            "table": [str, ...],              # table-level findings
            "columns": {col: [str, ...], ...} # per-column findings
        }
    """
    summary = profile["summary"]
    columns = profile["columns"]

    table_findings = _table_findings(summary, columns, df)
    column_findings = {col: _column_findings(col, stats, summary["rows"]) for col, stats in columns.items()}

    return {"table": table_findings, "columns": column_findings}


# ---------------------------------------------------------------------------
# Table-level rules
# ---------------------------------------------------------------------------

def _table_findings(summary: dict, columns: dict, df) -> list:
    findings = []

    # --- Primary key candidates ---
    pk_candidates = [
        col for col, stats in columns.items()
        if stats["unique_pct"] == 100.0 and stats["null_count"] == 0
    ]

    if pk_candidates:
        for col in pk_candidates:
            findings.append(f"PRIMARY KEY CANDIDATE: '{col}' — 100% unique, no nulls")
    else:
        # Near-PK candidates (unique but not 100%)
        near_pk = [
            (col, stats["unique_pct"]) for col, stats in columns.items()
            if stats["unique_pct"] >= 95.0 and stats["null_count"] == 0
        ]
        if near_pk:
            for col, pct in sorted(near_pk, key=lambda x: -x[1]):
                findings.append(f"NEAR PRIMARY KEY: '{col}' — {pct}% unique, no nulls (some duplicates present)")

        # Composite key detection — requires the original DataFrame
        if df is not None:
            composite = _find_composite_keys(columns, df)
            if composite:
                for combo in composite:
                    cols_str = ", ".join(f"'{c}'" for c in combo)
                    findings.append(f"COMPOSITE KEY CANDIDATE: ({cols_str}) — unique together, no nulls in either column")
            else:
                findings.append("No single-column or composite primary key candidate found")

    # --- Duplicate rows ---
    dupes = summary["duplicate_rows"]
    if dupes > 0:
        findings.append(f"Duplicate rows detected ({dupes:,}) — deduplicate before analysis")
    else:
        findings.append("No duplicate rows found")

    # --- Overall null rate ---
    null_pct = summary["total_null_pct"]
    if null_pct >= 50:
        findings.append(f"Critical overall null rate ({null_pct}%) — dataset may be largely unusable")
    elif null_pct >= 20:
        findings.append(f"High overall null rate ({null_pct}%) — data may be incomplete")
    elif null_pct > 0:
        findings.append(f"Overall null rate is low ({null_pct}%)")
    else:
        findings.append("No null values across the entire dataset")

    return findings


def _find_composite_keys(columns: dict, df) -> list:
    """
    Test 2-column then 3-column combinations of candidate columns for joint uniqueness.
    Candidates: unique_pct >= 50% and null_count == 0.
    Returns a list of tuples, each a qualifying key combination.
    """
    candidates = [
        col for col, stats in columns.items()
        if stats["unique_pct"] >= 50.0 and stats["null_count"] == 0
    ]

    qualifying = []

    # Test pairs first
    for combo in combinations(candidates, 2):
        if df.duplicated(subset=list(combo)).sum() == 0:
            qualifying.append(combo)

    # Only try triples if no pairs found (keeps output manageable)
    if not qualifying and len(candidates) >= 3:
        for combo in combinations(candidates, 3):
            if df.duplicated(subset=list(combo)).sum() == 0:
                qualifying.append(combo)

    return qualifying


# ---------------------------------------------------------------------------
# Column-level rules
# ---------------------------------------------------------------------------

def _column_findings(col: str, stats: dict, total_rows: int) -> list:
    findings = []

    # --- Null quality ---
    null_pct = stats["null_pct"]
    if stats["null_count"] == 0:
        findings.append("Complete — no missing values")
    elif null_pct < 10:
        findings.append(f"Low null rate ({null_pct}%) — minor gaps")
    elif null_pct < 50:
        findings.append(f"Moderate nulls ({null_pct}%) — investigate cause before use")
    else:
        findings.append(f"Majority missing ({null_pct}%) — consider dropping this column")

    # --- Cardinality ---
    unique_pct = stats["unique_pct"]
    unique_count = stats["unique_count"]

    if unique_count == 1:
        findings.append("Constant column — single value throughout, no analytical value")
    elif stats["null_count"] == 0 and unique_pct == 100.0:
        findings.append("Primary key candidate — all values unique")
    elif unique_pct >= 95:
        findings.append(f"High cardinality ({unique_pct}%) — likely an identifier or natural key")
    elif unique_pct <= 1:
        findings.append(f"Very low cardinality ({unique_count} distinct values) — suitable for grouping or filtering")

    is_numeric = "mean" in stats

    if is_numeric:
        _numeric_findings(stats, findings)
    else:
        _categorical_findings(stats, total_rows, findings)

    return findings


def _numeric_findings(stats: dict, findings: list) -> None:
    skewness = stats.get("skewness")
    if skewness is not None:
        if skewness > 2:
            findings.append(f"Strongly right-skewed ({skewness}) — median is more representative than mean; consider log transform")
        elif skewness < -2:
            findings.append(f"Strongly left-skewed ({skewness}) — median is more representative than mean")

    if stats.get("std") == 0:
        findings.append("Zero variance — all non-null values are identical")

    negatives = stats.get("negatives", 0)
    if negatives > 0:
        findings.append(f"Contains {negatives:,} negative value(s) — verify this is expected for this field")

    zeros = stats.get("zeros", 0)
    if zeros > 0:
        findings.append(f"Contains {zeros:,} zero(s) — verify whether zero is a valid value or a substitute for null")


def _categorical_findings(stats: dict, total_rows: int, findings: list) -> None:
    top_values = stats.get("top_5_values", {})
    if top_values and total_rows > 0:
        top_val = next(iter(top_values))
        top_count = top_values[top_val]
        non_null = total_rows - stats["null_count"]
        if non_null > 0:
            top_pct = round(top_count / non_null * 100, 1)
            if top_pct >= 90:
                findings.append(f"Heavily dominated by '{top_val}' ({top_pct}% of non-null values) — low informational value")

    avg_length = stats.get("avg_length")
    if avg_length is not None and avg_length > 50:
        findings.append(f"Long average value length ({avg_length} chars) — may be free-text; consider text analysis")


# ---------------------------------------------------------------------------
# Plain-text formatter for GUI display
# ---------------------------------------------------------------------------

def format_findings(findings: dict, file_name: str = "") -> str:
    divider = "═" * 60
    thin = "─" * 60
    lines = [divider]
    header = "FINDINGS & RECOMMENDATIONS"
    if file_name:
        header += f"  ({file_name})"
    lines.append(header)
    lines.append(divider)

    lines.append("TABLE LEVEL")
    for finding in findings["table"]:
        lines.append(f"  • {finding}")

    for col, col_findings in findings["columns"].items():
        if col_findings:
            lines.append("")
            lines.append(f"COLUMN: {col}")
            for finding in col_findings:
                lines.append(f"  • {finding}")

    lines.append(divider)
    lines.append("")
    return "\n".join(lines)
