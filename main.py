import logging
import pathlib

from gui import launch


def _configure_logging() -> None:
    """Write ERROR-level (and above) log entries to a persistent file.

    Log location: ~/.python_data_profiler/app.log
    The file is created on first run; subsequent runs append to it.
    """
    log_dir = pathlib.Path.home() / ".python_data_profiler"
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=str(log_dir / "app.log"),
        level=logging.ERROR,
        format="%(asctime)s  %(levelname)-8s  %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        encoding="utf-8",
    )


if __name__ == "__main__":
    _configure_logging()
    launch()
