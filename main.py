import sys

# Console is used here solely to print the loading confirmation with rich markup
from rich.console import Console

# Import the three public functions from the profiler module
from profiler import load_file, profile_dataframe, print_profile


# Default file to load when no command-line argument is provided (e.g. when run from an IDE)
DEFAULT_FILE = "sample_data.csv"


def main() -> None:
    # Use the first CLI argument as the file path, or fall back to the default
    file_path = sys.argv[1] if len(sys.argv) >= 2 else DEFAULT_FILE

    # Load the CSV or Parquet file into a pandas DataFrame
    df = load_file(file_path)

    # Print a styled confirmation showing the dimensions of the loaded dataset
    Console(force_terminal=True, color_system="truecolor").print(f"[dim]Loaded [bold]{len(df):,}[/bold] rows x [bold]{len(df.columns)}[/bold] columns from '[italic]{file_path}[/italic]'[/dim]\n")

    # Compute all profiling statistics for the dataframe
    profile = profile_dataframe(df)

    # Render the profile to the terminal using rich tables and panels
    print_profile(profile)


if __name__ == "__main__":
    main()
