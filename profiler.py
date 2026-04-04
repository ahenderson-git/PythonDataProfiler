# Standard library imports for file path handling
import pathlib

# pandas is the core data manipulation library used to load and inspect the dataframe
import pandas as pd

# rich imports for styled terminal output
from rich.console import Console   # handles printing with markup/color support
from rich.table import Table       # renders bordered key/value tables per column
from rich.panel import Panel       # renders the summary block in a named box
from rich.text import Text         # (available for future use with styled text objects)
from rich.columns import Columns   # lays out multiple tables side-by-side
import rich.box                    # provides box style constants (e.g. SIMPLE_HEAVY)


def load_file(file_path: str) -> pd.DataFrame:
    # Resolve the path and verify the file exists before attempting to read
    path = pathlib.Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Dispatch to the correct pandas reader based on file extension
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    elif suffix == ".parquet":
        return pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file type '{suffix}'. Expected .csv or .parquet")


def profile_column(series: pd.Series, total_rows: int) -> dict:
    # Count missing values and express as a percentage of total rows
    null_count = series.isna().sum()
    null_pct = null_count / total_rows * 100

    # Count distinct non-null values (dropna=True excludes NaN from the unique count)
    unique_count = series.nunique(dropna=True)

    # Base stats present for every column regardless of type
    profile = {
        "dtype": str(series.dtype),
        "null_count": int(null_count),
        "null_pct": round(null_pct, 2),
        "unique_count": int(unique_count),
        "unique_pct": round(unique_count / total_rows * 100, 2),
    }

    # Work only on non-null values for all further calculations
    non_null = series.dropna()

    # Numeric branch: booleans are technically numeric in pandas but are excluded
    # here so they fall through to the categorical branch instead
    if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series):
        mode_vals = non_null.mode()  # mode() can return multiple values; we take the first
        profile.update({
            "min": round(float(non_null.min()), 4) if len(non_null) else None,
            "max": round(float(non_null.max()), 4) if len(non_null) else None,
            "mean": round(float(non_null.mean()), 4) if len(non_null) else None,
            "median": round(float(non_null.median()), 4) if len(non_null) else None,
            "mode": round(float(mode_vals.iloc[0]), 4) if len(mode_vals) else None,
            # std requires at least 2 values (ddof=1 by default)
            "std": round(float(non_null.std()), 4) if len(non_null) > 1 else None,
            "q1": round(float(non_null.quantile(0.25)), 4) if len(non_null) else None,
            "q3": round(float(non_null.quantile(0.75)), 4) if len(non_null) else None,
            # skew requires at least 3 values to be meaningful
            "skewness": round(float(non_null.skew()), 4) if len(non_null) > 2 else None,
            # counts of special values useful for data quality checks
            "zeros": int((non_null == 0).sum()),
            "negatives": int((non_null < 0).sum()),
        })
    else:
        # Categorical / boolean / string branch
        mode_vals = non_null.mode()
        # value_counts returns frequencies sorted descending; take top 5
        top_values = non_null.value_counts().head(5).to_dict()
        profile.update({
            "mode": str(mode_vals.iloc[0]) if len(mode_vals) else None,
            # Cast keys/values to str/int for safe serialisation later
            "top_5_values": {str(k): int(v) for k, v in top_values.items()},
            # Average character length gives a sense of value width (useful for strings)
            "avg_length": round(non_null.astype(str).str.len().mean(), 2) if len(non_null) else None,
        })

    return profile


def profile_dataframe(df: pd.DataFrame) -> dict:
    total_rows = len(df)
    total_cells = total_rows * len(df.columns)  # used to compute overall null percentage

    # sum().sum() flattens the per-column null counts into a single integer
    total_nulls = int(df.isna().sum().sum())

    summary = {
        "rows": total_rows,
        "columns": len(df.columns),
        "total_cells": total_cells,
        "total_nulls": total_nulls,
        # Guard against empty dataframe (total_cells == 0) to avoid division by zero
        "total_null_pct": round(total_nulls / total_cells * 100, 2) if total_cells else 0,
        # duplicated() marks every duplicate occurrence (not just the first); sum counts them
        "duplicate_rows": int(df.duplicated().sum()),
        # deep=True includes object column memory (e.g. string values), not just the index
        "memory_mb": round(df.memory_usage(deep=True).sum() / 1024 ** 2, 3),
    }

    # Profile each column independently and store results keyed by column name
    columns = {col: profile_column(df[col], total_rows) for col in df.columns}

    return {"summary": summary, "columns": columns}


def _null_color(null_pct: float) -> str:
    # Returns a rich markup color string based on null severity thresholds:
    # 0% nulls  -> green (clean)
    # < 10%     -> yellow (minor concern)
    # >= 10%    -> red (significant data quality issue)
    if null_pct == 0:
        return "bold green"
    elif null_pct < 10:
        return "yellow"
    else:
        return "bold red"


def _col_table(col: str, stats: dict) -> Table:
    # Determine null color once and reuse for both the null count and null % rows
    null_color = _null_color(stats["null_pct"])

    # Presence of "mean" in stats distinguishes numeric columns from categorical ones
    is_numeric = "mean" in stats

    # Columns with >= 10% nulls get a red header to flag them immediately
    header_style = "bold red" if stats["null_pct"] >= 10 else "bold cyan"

    # Build the rich Table with the column name and dtype as its title
    table = Table(
        title=f"[{header_style}]{col}[/]  [dim]{stats['dtype']}[/dim]",
        box=rich.box.SIMPLE_HEAVY,  # single-line borders with a heavier top/bottom
        show_header=False,          # column headers ("Metric"/"Value") are not displayed
        title_justify="left",
        min_width=36,
    )
    # Two columns: metric label (left) and its value (right-aligned)
    table.add_column("Metric", style="dim", min_width=14)
    table.add_column("Value", justify="right", min_width=12)

    # Null stats — colored by severity
    table.add_row("Null count", f"[{null_color}]{stats['null_count']:,}[/]")
    table.add_row("Null %", f"[{null_color}]{stats['null_pct']}%[/]")

    # Unique stats — cyan when unique% >= 95, which typically indicates an ID-like column
    unique_style = "cyan" if stats["unique_pct"] >= 95 else ""
    table.add_row("Unique", f"[{unique_style}]{stats['unique_count']:,}[/]" if unique_style else f"{stats['unique_count']:,}")
    table.add_row("Unique %", f"[{unique_style}]{stats['unique_pct']}%[/]" if unique_style else f"{stats['unique_pct']}%")

    # Visual separator between quality metrics and distribution stats
    table.add_section()

    if is_numeric:
        # Highlight skewness in yellow when the distribution is significantly asymmetric
        skew = stats["skewness"]
        skew_style = "yellow" if skew is not None and abs(skew) > 1 else ""
        skew_val = f"[{skew_style}]{skew}[/]" if skew_style else str(skew)

        # Full distribution stats for numeric columns
        table.add_row("Min", str(stats["min"]))
        table.add_row("Max", str(stats["max"]))
        table.add_row("Mean", str(stats["mean"]))
        table.add_row("Median", str(stats["median"]))
        table.add_row("Mode", str(stats["mode"]))
        table.add_row("Std Dev", str(stats["std"]))
        table.add_row("Q1", str(stats["q1"]))   # 25th percentile
        table.add_row("Q3", str(stats["q3"]))   # 75th percentile
        table.add_row("Skewness", skew_val)
        table.add_row("Zeros", f"{stats['zeros']:,}")
        table.add_row("Negatives", f"{stats['negatives']:,}")
    else:
        # Categorical / boolean columns show mode and top-5 value frequency
        table.add_row("Mode", str(stats["mode"]))
        table.add_row("Avg length", str(stats["avg_length"]))
        # Second section separator before the frequency table
        table.add_section()
        # Each top value is indented slightly to visually distinguish it from the metric rows
        for val, count in stats["top_5_values"].items():
            table.add_row(f"  {val}", f"{count:,}")

    return table


def print_profile(profile: dict) -> None:
    # force_terminal=True: treat stream as a terminal even when not a real TTY
    # color_system="truecolor": force ANSI colour output (auto-detection returns None
    # for non-TTY streams, suppressing all colour even with force_terminal=True)
    console = Console(force_terminal=True, color_system="truecolor")
    s = profile["summary"]

    # Choose colors for the two summary metrics that signal data quality issues
    null_color = _null_color(s["total_null_pct"])
    dup_color = "bold red" if s["duplicate_rows"] > 0 else "bold green"

    # Build the summary panel content as a single rich markup string
    summary_text = (
        f"[bold]Rows[/bold]           {s['rows']:,}\n"
        f"[bold]Columns[/bold]        {s['columns']}\n"
        f"[bold]Total cells[/bold]    {s['total_cells']:,}\n"
        f"[bold]Total nulls[/bold]    [{null_color}]{s['total_nulls']:,} ({s['total_null_pct']}%)[/]\n"
        f"[bold]Duplicates[/bold]     [{dup_color}]{s['duplicate_rows']:,}[/]\n"
        f"[bold]Memory[/bold]         {s['memory_mb']} MB"
    )
    # Panel wraps the summary text in a named bordered box
    console.print(Panel(summary_text, title="[bold white]Dataset Summary[/bold white]", expand=False))

    # Build one Table per column, then render them side-by-side using Columns
    tables = [_col_table(col, stats) for col, stats in profile["columns"].items()]
    console.print(Columns(tables, equal=False, expand=False))
