# Null severity thresholds — percentage of values that are null
NULL_WARN_PCT: float = 10.0           # below → low concern; at/above → significant (red in profiler)
NULL_MODERATE_COL_PCT: float = 50.0   # column: at/above → "majority missing"
NULL_HIGH_TABLE_PCT: float = 20.0     # dataset: at/above → "high overall null rate"
NULL_CRITICAL_TABLE_PCT: float = 50.0 # dataset: at/above → "critical overall null rate"

# Cardinality thresholds — percentage of rows that are distinct
HIGH_CARDINALITY_PCT: float = 95.0    # at/above → likely an identifier or natural key
COMPOSITE_CANDIDATE_PCT: float = 50.0 # at/above → eligible for composite-key test

# Numeric distribution thresholds
SKEWNESS_STRONG: float = 2.0          # |skewness| above this → "strongly skewed" finding

# Categorical analysis thresholds
DOMINANT_VALUE_PCT: float = 90.0      # top value covers this % of non-nulls → "dominated" finding
LONG_TEXT_THRESHOLD: float = 50.0     # average character length above this → "free-text" finding
TOP_VALUES_COUNT: int = 5             # number of top values kept per categorical column
TOP_VALUES_KEY: str = f"top_{TOP_VALUES_COUNT}_values"  # profile dict key: "top_5_values"

# Encoding detection
ENCODING_CONFIDENCE_LOW: float = 0.85 # below → low-confidence encoding warning shown

# Data cleaning
DATETIME_PARSE_THRESHOLD: float = 0.80 # fraction of values that must parse for column to be converted

# Analysis worker progress markers (percentage, 0–100 scale)
PROGRESS_LOAD_START: int = 2      # data loading / query has begun
PROGRESS_PROFILE_START: int = 5   # column profiling has begun (loading complete)
PROGRESS_PROFILE_END: int = 80    # column profiling complete (fills PROFILE_START → PROFILE_END)
PROGRESS_INTERPRET: int = 82      # interpretation step (profiling complete)
PROGRESS_RENDER: int = 95         # rendering step (interpretation complete)
