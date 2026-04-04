# Python Data Profiler

A lightweight command-line tool for quickly profiling CSV and Parquet datasets. It computes statistical summaries for every column and renders them as colour-coded tables in the terminal.

---

## Features

- Loads **CSV** and **Parquet** files into a pandas DataFrame
- Computes per-column statistics including:
  - Null count and percentage
  - Unique count and percentage
  - Min, max, mean, median, mode, standard deviation
  - Q1 (25th percentile) and Q3 (75th percentile)
  - Skewness, zero count, negative count *(numeric columns)*
  - Top 5 most frequent values, average string length *(categorical/boolean columns)*
- Dataset-level summary: total rows, columns, nulls, duplicate rows, memory usage
- Colour-coded output via [`rich`](https://github.com/Textualize/rich):
  - **Green** — 0% nulls (clean)
  - **Yellow** — < 10% nulls (minor concern)
  - **Red** — ≥ 10% nulls (significant data quality issue)
  - **Cyan** — column unique% ≥ 95 (likely an ID column)
  - **Yellow skewness** — |skew| > 1 (significantly asymmetric distribution)
  - **Red duplicates** — any duplicate rows detected

---

## Project Structure

```
PythonDataProfiler/
├── main.py           # Entry point — run this file
├── profiler.py       # Core logic: load, profile, and display
├── sample_data.csv   # Sample dataset for testing
└── README.md
```

---

## Requirements

- Python 3.9+
- Dependencies (install via pip):

```
pandas
pyarrow
rich
```

Install all dependencies into your virtual environment:

```bash
pip install pandas pyarrow rich
```

> `pyarrow` is only required for reading Parquet files. If you only use CSV, it can be omitted.

---

## Usage

### Command Line (CLI)

Run the profiler by passing a file path as an argument:

```bash
python main.py path/to/your/file.csv
```

```bash
python main.py path/to/your/file.parquet
```

**Example using the included sample file:**

```bash
python main.py sample_data.csv
```

**Expected output:**

```
Loaded 27 rows x 8 columns from 'sample_data.csv'

+---- Dataset Summary -----+
| Rows           27        |
| Columns        8         |
| Total cells    216       |
| Total nulls    6 (2.78%) |
| Duplicates     2         |
| Memory         0.002 MB  |
+--------------------------+

customer_id  [int64]              name  [str]
+----------------------------+   +----------------------------+
| Null count  |            0 |   | Null count  |            1 |
| Null %      |         0.0% |   | Null %      |         3.7% |
| Unique      |           25 |   | ...                        |
| ...                        |   +----------------------------+
+----------------------------+
```

Column tables are rendered **side-by-side** in the terminal for compact output. Colours are visible in any ANSI-compatible terminal (Windows Terminal, macOS Terminal, Linux terminals).

---

### IDE (PyCharm / VS Code)

When running from an IDE without command-line arguments, the script uses a **default file path** defined at the top of `main.py`:

```python
DEFAULT_FILE = "sample_data.csv"
```

**To profile a different file from your IDE:**

1. Open `main.py`
2. Change the `DEFAULT_FILE` variable to your file path:

```python
DEFAULT_FILE = "path/to/your/data.csv"
```

3. Run `main.py` directly (no arguments needed)

**To pass arguments via PyCharm run configuration:**

1. Go to **Run → Edit Configurations**
2. Select (or create) the configuration for `main.py`
3. In the **Parameters** field, enter your file path:
   ```
   sample_data.csv
   ```
   or a full path:
   ```
   C:\Users\yourname\data\sales_data.parquet
   ```
4. Click **OK** and run normally

> **Colour output in PyCharm:** The included run configuration (`.idea/runConfigurations/main.xml`) has **"Emulate terminal in output console"** pre-enabled. Open the project in PyCharm and run `main.py` directly — colours will appear automatically. If you create a new run configuration manually, tick **Emulate terminal in output console** in **Run → Edit Configurations** to enable colour.

---

## Supported File Formats

| Format   | Extension    | Notes                                      |
|----------|--------------|--------------------------------------------|
| CSV      | `.csv`       | Loaded with `pandas.read_csv`              |
| Parquet  | `.parquet`   | Loaded with `pandas.read_parquet` (requires `pyarrow`) |

Files with any other extension will raise a `ValueError` with a clear message.

---

## Column Statistics Reference

### All Columns

| Stat         | Description                                      |
|--------------|--------------------------------------------------|
| dtype        | pandas data type (e.g. `int64`, `float64`, `str`) |
| Null count   | Number of missing values                         |
| Null %       | Percentage of rows that are null                 |
| Unique       | Number of distinct non-null values               |
| Unique %     | Unique values as a percentage of total rows      |

### Numeric Columns (int, float — excluding bool)

| Stat       | Description                                           |
|------------|-------------------------------------------------------|
| Min        | Smallest value                                        |
| Max        | Largest value                                         |
| Mean       | Arithmetic average                                    |
| Median     | Middle value (50th percentile)                        |
| Mode       | Most frequently occurring value                       |
| Std Dev    | Standard deviation (population sample, ddof=1)        |
| Q1         | 25th percentile                                       |
| Q3         | 75th percentile                                       |
| Skewness   | Asymmetry of distribution (highlighted if \|skew\| > 1) |
| Zeros      | Count of values equal to 0                            |
| Negatives  | Count of values less than 0                           |

### Categorical / String / Boolean Columns

| Stat        | Description                                           |
|-------------|-------------------------------------------------------|
| Mode        | Most frequently occurring value                       |
| Avg length  | Mean character length of values (cast to string)      |
| Top 5 values | Most frequent values with their counts              |

---

## Customisation

### Changing the Default File

Edit the `DEFAULT_FILE` constant in `main.py`:

```python
DEFAULT_FILE = "my_dataset.csv"
```

### Adjusting Null Severity Thresholds

Edit the `_null_color` function in `profiler.py`:

```python
def _null_color(null_pct: float) -> str:
    if null_pct == 0:
        return "bold green"
    elif null_pct < 10:   # <-- change this threshold
        return "yellow"
    else:
        return "bold red"
```

### Adjusting the Number of Top Values Shown

In `profile_column` inside `profiler.py`, change `.head(5)` to any number:

```python
top_values = non_null.value_counts().head(5).to_dict()  # change 5 to any n
```

---

## Sample Data

The included `sample_data.csv` contains 27 rows and 8 columns, intentionally designed to exercise all profiler paths:

| Column       | Type        | Notes                                |
|--------------|-------------|--------------------------------------|
| customer_id  | int64       | High cardinality — highlights as cyan |
| name         | str         | 1 null, 2 duplicate values           |
| age          | float64     | 1 null                               |
| salary       | float64     | 2 nulls                              |
| department   | str         | Low cardinality (4 categories)       |
| score        | float64     | 2 nulls, slight negative skew        |
| join_date    | str         | Date strings (not parsed as datetime)|
| is_active    | bool        | 2-value boolean column               |

The last two rows are intentional duplicates of rows 2 and 10, so `Duplicates: 2` appears in the summary.
