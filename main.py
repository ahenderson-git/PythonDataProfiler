# Standard library — used for package discovery, subprocess pip calls, logging, and file paths
import importlib
import subprocess
import sys
import logging
import pathlib


def _ensure_dependencies() -> None:
    """Install any missing runtime dependencies before the app starts."""
    # Map Python import name → pip package name.
    # These can differ (e.g. charset_normalizer vs charset-normalizer), so both are tracked.
    required = {
        "polars": "polars",
        "rich": "rich",
        "charset_normalizer": "charset-normalizer",
    }
    # Build a list of pip names whose import name cannot be found in the current environment
    missing = [
        pip_name
        for import_name, pip_name in required.items()
        if importlib.util.find_spec(import_name) is None
    ]
    if missing:
        print(f"Installing missing packages: {', '.join(missing)}")
        # Use sys.executable so pip targets the same interpreter/venv as this script.
        # stdout is suppressed to keep the terminal clean on successful installs;
        # non-zero exit codes still raise CalledProcessError via check_call.
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", *missing],
            stdout=subprocess.DEVNULL,
        )


# Run the dependency check before any project imports that rely on those packages
_ensure_dependencies()

from gui import launch  # noqa: E402 — must come after dependency check


def _configure_logging() -> None:
    """Write ERROR-level (and above) log entries to a persistent file.

    Log location: ~/.python_data_profiler/app.log
    The file is created on first run; subsequent runs append to it.
    """
    # Create the log directory inside the user's home folder if it doesn't exist yet
    log_dir = pathlib.Path.home() / ".python_data_profiler"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Configure the root logger to write ERROR and above to the persistent log file.
    # INFO/DEBUG messages are intentionally discarded — only real errors are kept.
    logging.basicConfig(
        filename=str(log_dir / "app.log"),
        level=logging.ERROR,
        format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        encoding="utf-8",
    )


# Guard: only start the app when this file is run directly, not when imported as a module
if __name__ == "__main__":
    _configure_logging()
    launch()
