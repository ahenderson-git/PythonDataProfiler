# Standard library imports used across the GUI
import csv          # writing profile and audit log CSV exports
import io           # StringIO buffer for capturing Rich console output as a string
import json         # serialising the profile dict to JSON for export
import logging      # module-level logger for recording errors to the log file
import os           # os.path.basename for extracting the filename from a full path
import pathlib      # path manipulation for building audit log filenames alongside cleaned files
import queue        # thread-safe queue for passing results from worker threads to the GUI
import threading    # background threads for analysis, cleaning, and save operations
import time         # perf_counter for elapsed timing displayed in the results panel
import tkinter as tk
import tkinter.font as tkfont                           # measuring character width for Rich output
from tkinter import filedialog, messagebox, ttk         # file dialogs, message boxes, themed widgets
from typing import Callable                             # type hint for the on_connect_start callback

# Module-level logger — errors are written to the persistent log file configured in main.py
_log = logging.getLogger(__name__)

from rich.console import Console  # used to render profile output into a StringIO buffer

# Application modules
import db_connector
from profiler import load_file, profile_dataframe, print_profile
from interpreter import interpret_profile, format_findings
from cleaner import clean_dataframe
from constants import (
    PROGRESS_LOAD_START,
    PROGRESS_PROFILE_START,
    PROGRESS_PROFILE_END,
    PROGRESS_INTERPRET,
    PROGRESS_RENDER,
    TOP_VALUES_KEY,
)


# ---------------------------------------------------------------------------
# SQL connection panel — self-contained widget
# ---------------------------------------------------------------------------

class SqlPanel(tk.Frame):
    """Self-contained SQL Server connection panel.

    Owns all connection-related widgets and state.  The parent app supplies a
    shared queue so the connection worker can post results, and an
    ``on_connect_start`` callback so the parent knows to begin queue-polling.
    """

    def __init__(
        self,
        parent: tk.Widget,
        shared_queue: queue.Queue,
        on_connect_start: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(parent)
        # Shared queue lets the background connect worker post results to the GUI thread
        self._queue = shared_queue
        # Optional callback fired when a connection attempt begins, so the parent starts polling
        self._on_connect_start = on_connect_start
        # Populated on successful connection; empty string means "not connected"
        self.connection_string = ""

        # Tkinter variables — each is bound to a widget and reflects its current value
        self._auth_var     = tk.StringVar(value="sql")    # "sql" or "aad"
        self._server_var   = tk.StringVar()               # short server name (no .database.windows.net)
        self._database_var = tk.StringVar()               # database name
        self._username_var = tk.StringVar()               # SQL auth username
        self._password_var = tk.StringVar()               # SQL auth password (shown as asterisks)
        self._table_var    = tk.StringVar()               # currently selected table/view name

        self._build()   # create and lay out all widgets

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def table_name(self) -> str:
        """Currently selected table/view name, stripped of leading/trailing whitespace."""
        return self._table_var.get().strip()

    @property
    def custom_sql(self) -> str:
        """Contents of the Custom SQL text box, stripped of whitespace."""
        return self._sql_text.get("1.0", tk.END).strip()

    @property
    def has_tables(self) -> bool:
        """True if the table dropdown has been populated after a successful connection."""
        return bool(self._table_combo["values"])

    def set_enabled(self, enabled: bool) -> None:
        """Enable or disable interactive controls (called by parent during analysis)."""
        self._btn_connect.config(state=tk.NORMAL if enabled else tk.DISABLED)
        # Only re-enable the dropdown if there are tables loaded — otherwise leave it disabled
        if enabled and self.has_tables:
            self._table_combo.config(state="readonly")
        else:
            self._table_combo.config(state="disabled")

    def on_connect_complete(self, tables: list, conn_str: str) -> None:
        """Called by the parent queue-poller on successful connection."""
        # Store the connection string so the analyse worker can use it later
        self.connection_string = conn_str
        self._btn_connect.config(state=tk.NORMAL)

        if not tables:
            messagebox.showwarning(
                "No tables found",
                "No tables or views are accessible with this connection.",
            )
            self._sql_status_label.config(
                text="Connected — no accessible tables found.", fg="orange"
            )
            return

        # Populate the dropdown with the returned table/view names
        names = [t["full_name"] for t in tables]
        self._table_combo["values"] = names
        self._table_combo.config(state="readonly")
        self._table_combo.current(0)    # pre-select the first entry
        self._sql_status_label.config(
            text=f"Connected — {len(tables)} table(s)/view(s) found.", fg="green"
        )

    def on_connect_error(self, message: str) -> None:
        """Called by the parent queue-poller on connection failure."""
        self._btn_connect.config(state=tk.NORMAL)
        self._sql_status_label.config(text=f"Connection failed: {message}", fg="red")

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build(self) -> None:
        # Allow column 1 to expand so Entry widgets stretch with the panel width
        self.columnconfigure(1, weight=1)

        # Row 0: Server name entry
        tk.Label(self, text="Server:").grid(row=0, column=0, sticky="e", pady=2, padx=(0, 4))
        tk.Entry(self, textvariable=self._server_var, width=40).grid(
            row=0, column=1, columnspan=3, sticky="ew"
        )

        # Row 1: Database name entry
        tk.Label(self, text="Database:").grid(row=1, column=0, sticky="e", pady=2, padx=(0, 4))
        tk.Entry(self, textvariable=self._database_var, width=40).grid(
            row=1, column=1, columnspan=3, sticky="ew"
        )

        # Row 2: Auth type radio buttons — switching auth hides/shows username/password rows
        tk.Label(self, text="Auth:").grid(row=2, column=0, sticky="e", pady=2, padx=(0, 4))
        tk.Radiobutton(
            self, text="SQL auth", variable=self._auth_var, value="sql",
            command=self._on_auth_change,
        ).grid(row=2, column=1, sticky="w")
        tk.Radiobutton(
            self, text="AAD Interactive", variable=self._auth_var, value="aad",
            command=self._on_auth_change,
        ).grid(row=2, column=2, sticky="w")

        # Row 3: Username (only visible when SQL auth is selected)
        self._lbl_username = tk.Label(self, text="Username:")
        self._lbl_username.grid(row=3, column=0, sticky="e", pady=2, padx=(0, 4))
        self._ent_username = tk.Entry(self, textvariable=self._username_var, width=30)
        self._ent_username.grid(row=3, column=1, columnspan=2, sticky="ew")

        # Row 4: Password (show="*" masks the input; only visible when SQL auth is selected)
        self._lbl_password = tk.Label(self, text="Password:")
        self._lbl_password.grid(row=4, column=0, sticky="e", pady=2, padx=(0, 4))
        self._ent_password = tk.Entry(self, textvariable=self._password_var, show="*", width=30)
        self._ent_password.grid(row=4, column=1, columnspan=2, sticky="ew")

        # Row 5: Connect button + table/view dropdown (disabled until connected)
        self._btn_connect = ttk.Button(self, text="Connect", command=self._connect, width=10)
        self._btn_connect.grid(row=5, column=0, sticky="w", pady=(8, 2))

        self._table_combo = ttk.Combobox(
            self, textvariable=self._table_var, state="disabled", width=38
        )
        self._table_combo.grid(row=5, column=1, columnspan=3, sticky="ew", pady=(8, 2))

        # Row 6: Custom SQL text area — overrides the table dropdown when non-empty
        tk.Label(self, text="Custom SQL:").grid(
            row=6, column=0, sticky="ne", pady=(6, 0), padx=(0, 4)
        )
        self._sql_text = tk.Text(self, height=3, width=50, wrap=tk.WORD)
        self._sql_text.grid(row=6, column=1, columnspan=3, sticky="ew", pady=(6, 0))

        # Row 7: Helper hint explaining custom SQL precedence
        tk.Label(
            self, text="If provided, custom SQL overrides the table selection above.", fg="gray"
        ).grid(row=7, column=1, columnspan=3, sticky="w")

        # Row 8: Connection status label — updated by on_connect_complete / on_connect_error
        self._sql_status_label = tk.Label(self, text="", anchor="w")
        self._sql_status_label.grid(row=8, column=0, columnspan=4, sticky="ew", pady=(4, 0))

    # ------------------------------------------------------------------
    # Auth toggle
    # ------------------------------------------------------------------

    def _on_auth_change(self) -> None:
        """Show or hide username/password widgets based on the selected auth type."""
        if self._auth_var.get() == "aad":
            # AAD Interactive doesn't need a username or password in the UI
            self._lbl_username.grid_remove()
            self._ent_username.grid_remove()
            self._lbl_password.grid_remove()
            self._ent_password.grid_remove()
        else:
            # SQL auth requires both username and password
            self._lbl_username.grid()
            self._ent_username.grid()
            self._lbl_password.grid()
            self._ent_password.grid()

    # ------------------------------------------------------------------
    # Connect
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        """Validate inputs, build the connection string, then start the connect worker thread."""
        server   = self._server_var.get().strip()
        database = self._database_var.get().strip()
        auth     = self._auth_var.get()
        username = self._username_var.get().strip()
        password = self._password_var.get()

        # Validate and build the connection string — raises ValueError on missing fields
        try:
            conn_str = db_connector.build_connection_string(
                server, database, auth, username, password
            )
        except ValueError as exc:
            messagebox.showwarning("Missing fields", str(exc))
            return

        # Disable the Connect button and dropdown while the attempt is in progress
        self._btn_connect.config(state=tk.DISABLED)
        self._table_combo.config(state="disabled")
        self._sql_status_label.config(text="Connecting...", fg="black")

        # Notify the parent so it starts polling the shared queue for the result
        if self._on_connect_start:
            self._on_connect_start()

        # Run the blocking pyodbc call on a daemon thread so the GUI stays responsive
        threading.Thread(
            target=self._connect_worker, args=(conn_str,), daemon=True
        ).start()

    def _connect_worker(self, connection_string: str) -> None:
        """Background thread: call list_tables() and post the result to the shared queue."""
        try:
            tables = db_connector.list_tables(connection_string)
            # Post (tag, tables, connection_string) so the parent can call on_connect_complete
            self._queue.put(("tables", tables, connection_string))
        except Exception as exc:
            _log.error("SQL Server connection failed", exc_info=True)
            self._queue.put(("connect_error", str(exc)))


# ---------------------------------------------------------------------------
# Main application
# ---------------------------------------------------------------------------

class DataProfilerApp(tk.Frame):
    def __init__(self, master: tk.Tk) -> None:
        super().__init__(master, padx=12, pady=12)
        self.pack(fill=tk.BOTH, expand=True)

        # Apply the "clam" theme for a cleaner, more modern look than the default
        _style = ttk.Style()
        _style.theme_use("clam")

        # --- Shared application state ---
        self._profile = None        # profile dict from profile_dataframe(); None until first analysis
        self._findings = None       # findings dict from interpret_profile(); None until first analysis
        self._source_label = ""     # display name used in export filenames and the findings header
        self._queue = queue.Queue() # thread-safe channel for worker threads to post results
        self._poll_id = None        # ID of the pending after() call; used to cancel polling if needed
        self._start_time = 0.0      # perf_counter timestamp when the current operation started
        self._polling_for = "analysis"  # tracks which operation the poller is serving
        self._df = None             # loaded DataFrame; retained so clean export can operate on it
        self._cancel_event = threading.Event()  # set to True to signal workers to stop early

        # Tracks whether the File or SQL Server source panel is active
        self._source_var = tk.StringVar(value="file")

        self._build_ui()
        # Intercept the window close button so we can signal any running worker to stop first
        master.protocol("WM_DELETE_WINDOW", self._on_close)

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        # Blue header banner with the application name
        hdr = tk.Frame(self, bg="#2b5797")
        hdr.pack(fill=tk.X, pady=(0, 8))
        tk.Label(hdr, text="Python Data Profiler", font=("", 15, "bold"),
                 fg="white", bg="#2b5797", pady=8).pack()

        # Source toggle: File vs SQL Server — controls which input panel is visible
        source_row = tk.Frame(self)
        source_row.pack(anchor="w", pady=(0, 6))
        tk.Label(source_row, text="Source:").pack(side=tk.LEFT)
        tk.Radiobutton(
            source_row, text="File", variable=self._source_var, value="file",
            command=self._on_source_change,
        ).pack(side=tk.LEFT, padx=(6, 0))
        tk.Radiobutton(
            source_row, text="SQL Server", variable=self._source_var, value="sql",
            command=self._on_source_change,
        ).pack(side=tk.LEFT, padx=(4, 0))

        # File input row: read-only path entry + Browse button
        self._file_frame = tk.Frame(self)
        self._file_frame.pack(fill=tk.X, pady=(0, 6))

        tk.Label(self._file_frame, text="File:").pack(side=tk.LEFT)
        self._file_var = tk.StringVar()
        tk.Entry(
            self._file_frame, textvariable=self._file_var, state="readonly", width=60
        ).pack(side=tk.LEFT, padx=(6, 6))
        self._btn_browse = ttk.Button(self._file_frame, text="Browse", command=self._browse)
        self._btn_browse.pack(side=tk.LEFT)

        # SQL panel is hidden initially; shown when the user selects "SQL Server"
        self._sql_panel = SqlPanel(
            self, self._queue, on_connect_start=self._on_sql_connect_started
        )

        # Separator between the input section and the action buttons
        self._input_sep = ttk.Separator(self, orient="horizontal")
        self._input_sep.pack(fill=tk.X, pady=(4, 6))

        # Analyse button — starts the analysis worker thread
        analyse_row = tk.Frame(self)
        analyse_row.pack(anchor="w", pady=(0, 4))
        self._btn_analyse = ttk.Button(analyse_row, text="Analyse", command=self._analyse, width=12)
        self._btn_analyse.pack(side=tk.LEFT)

        # Export Analysis buttons (disabled until a successful analysis has been run)
        export_analysis_frame = ttk.LabelFrame(self, text="Export Analysis", padding=(6, 4))
        export_analysis_frame.pack(fill=tk.X, pady=(4, 2))
        self._btn_json = ttk.Button(
            export_analysis_frame, text="Export JSON", command=self._export_json,
            state=tk.DISABLED, width=12,
        )
        self._btn_json.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_csv = ttk.Button(
            export_analysis_frame, text="Export CSV", command=self._export_csv,
            state=tk.DISABLED, width=12,
        )
        self._btn_csv.pack(side=tk.LEFT)

        # Export Clean Data buttons (disabled until a successful analysis has been run)
        export_clean_frame = ttk.LabelFrame(self, text="Export Clean Data", padding=(6, 4))
        export_clean_frame.pack(fill=tk.X, pady=(2, 4))
        self._btn_clean_csv = ttk.Button(
            export_clean_frame, text="Export Clean CSV", command=self._export_clean_csv,
            state=tk.DISABLED, width=16,
        )
        self._btn_clean_csv.pack(side=tk.LEFT, padx=(0, 4))
        self._btn_clean_parquet = ttk.Button(
            export_clean_frame, text="Export Clean Parquet", command=self._export_clean_parquet,
            state=tk.DISABLED, width=20,
        )
        self._btn_clean_parquet.pack(side=tk.LEFT)

        # Progress frame: shown only while an operation is running; hidden otherwise
        self._progress_frame = tk.Frame(self)
        self._progress_bar = ttk.Progressbar(
            self._progress_frame, mode="determinate", maximum=100, length=400
        )
        self._progress_bar.pack(side=tk.LEFT, padx=(0, 8))
        self._status_label = tk.Label(self._progress_frame, text="", anchor="w")
        self._status_label.pack(side=tk.LEFT, expand=True, fill=tk.X)
        self._btn_cancel = ttk.Button(
            self._progress_frame, text="Cancel", command=self._cancel_operation, width=8
        )
        self._btn_cancel.pack(side=tk.RIGHT, padx=(8, 0))
        # Elapsed time shown to the right of the Cancel button
        self._timer_label = tk.Label(self._progress_frame, text="", width=8, anchor="e")
        self._timer_label.pack(side=tk.RIGHT)

        # Separator between action area and results panel
        ttk.Separator(self, orient="horizontal").pack(fill=tk.X, pady=(2, 4))

        # Results text area: monospace font, scrollable in both directions, read-only
        text_frame = tk.Frame(self)
        text_frame.pack(fill=tk.BOTH, expand=True)

        v_scrollbar = tk.Scrollbar(text_frame, orient=tk.VERTICAL)
        v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        h_scrollbar = tk.Scrollbar(text_frame, orient=tk.HORIZONTAL)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)

        self._results = tk.Text(
            text_frame, wrap=tk.NONE, font=("Courier", 10), state=tk.DISABLED,
            yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set,
        )
        self._results.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        # Link scrollbars to the text widget so they move together
        v_scrollbar.config(command=self._results.yview)
        h_scrollbar.config(command=self._results.xview)

    # ------------------------------------------------------------------
    # Source toggle handler
    # ------------------------------------------------------------------

    def _on_source_change(self) -> None:
        """Swap between the file input row and the SQL panel when the source radio changes."""
        if self._source_var.get() == "file":
            # Hide the SQL panel and restore the file input row
            self._sql_panel.pack_forget()
            self._file_frame.pack(
                fill=tk.X, pady=(0, 6), before=self._input_sep
            )
        else:
            # Hide the file input row and show the SQL connection panel
            self._file_frame.pack_forget()
            self._sql_panel.pack(
                fill=tk.X, pady=(0, 6), before=self._input_sep
            )

    # ------------------------------------------------------------------
    # File browse
    # ------------------------------------------------------------------

    def _browse(self) -> None:
        """Open a file dialog restricted to .csv and .parquet files."""
        path = filedialog.askopenfilename(
            title="Select a file to analyse",
            filetypes=[
                ("Supported files", "*.csv *.parquet"),
                ("CSV files", "*.csv"),
                ("Parquet files", "*.parquet"),
            ],
        )
        if path:
            self._file_var.set(path)

    # ------------------------------------------------------------------
    # SQL connect polling trigger
    # ------------------------------------------------------------------

    def _on_sql_connect_started(self) -> None:
        """Start queue-polling when SqlPanel initiates a connection."""
        self._polling_for = "connect"
        self.after(100, self._poll_queue)

    # ------------------------------------------------------------------
    # Analyse
    # ------------------------------------------------------------------

    def _set_controls_enabled(self, enabled: bool) -> None:
        """Enable or disable all input controls together (called before/after operations)."""
        state = tk.NORMAL if enabled else tk.DISABLED
        self._btn_browse.config(state=state)
        self._btn_analyse.config(state=state)
        self._sql_panel.set_enabled(enabled)

    def _show_saving_indicator(self, status: str = "Saving...") -> None:
        """Switch the progress bar to indeterminate (spinner) mode and show the progress frame."""
        self._progress_bar.config(mode="indeterminate")
        self._progress_bar.start(10)    # interval in ms between animation steps
        self._status_label.config(text=status)
        self._timer_label.config(text="")
        self._progress_frame.pack(fill=tk.X, pady=(0, 6), before=self._results.master)
        self.update_idletasks()         # force a repaint so the user sees the indicator immediately

    def _hide_saving_indicator(self) -> None:
        """Stop the spinner and hide the progress frame."""
        self._progress_bar.stop()
        self._progress_bar.config(mode="determinate", value=0)
        self._progress_frame.pack_forget()

    def _analyse(self) -> None:
        """Validate inputs, set up the progress UI, and start the analysis worker thread."""
        # Measure the character width of the results panel so Rich renders at the right column count
        font = tkfont.Font(font=self._results.cget("font"))
        char_width = max(40, self._results.winfo_width() // font.measure("0"))

        if self._source_var.get() == "file":
            path = self._file_var.get()
            if not path:
                messagebox.showwarning(
                    "No file selected", "Please select a file before clicking Analyse."
                )
                return
            thread_args = ("file", path, char_width)
        else:
            # SQL source — verify that a connection has been established
            if not self._sql_panel.connection_string:
                messagebox.showwarning(
                    "Not connected", "Please click Connect before clicking Analyse."
                )
                return
            custom_sql = self._sql_panel.custom_sql
            table_name = self._sql_panel.table_name
            if not custom_sql and not table_name:
                messagebox.showwarning(
                    "No table selected",
                    "Please select a table from the list or enter a custom SQL query.",
                )
                return
            thread_args = (
                "sql", self._sql_panel.connection_string, char_width, table_name, custom_sql
            )

        # Reset the progress bar and show it before disabling controls
        self._progress_bar["value"] = 0
        self._status_label.config(text="Starting...")
        self._timer_label.config(text="0.0s")
        self._progress_frame.pack(fill=tk.X, pady=(0, 6), before=self._results.master)
        self._set_controls_enabled(False)
        # Disable export buttons so stale results can't be saved during a new analysis
        self._btn_json.config(state=tk.DISABLED)
        self._btn_csv.config(state=tk.DISABLED)
        self._btn_clean_csv.config(state=tk.DISABLED)
        self._btn_clean_parquet.config(state=tk.DISABLED)

        self._polling_for = "analysis"
        self._start_time = time.perf_counter()
        self._cancel_event.clear()  # reset the cancel flag from any previous operation
        threading.Thread(
            target=self._worker, args=thread_args,
            kwargs={"cancel_event": self._cancel_event}, daemon=True,
        ).start()
        # Start the 100 ms queue-polling loop
        self._poll_id = self.after(100, self._poll_queue)

    # ------------------------------------------------------------------
    # Worker thread
    # ------------------------------------------------------------------

    def _worker(
        self,
        source: str,
        source_arg: str,
        char_width: int,
        table_name: str = "",
        custom_sql: str = "",
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Background thread: load data, profile, interpret, and render, posting progress updates."""
        def cancelled() -> bool:
            """True if the user has clicked Cancel since this worker started."""
            return cancel_event is not None and cancel_event.is_set()

        try:
            t0 = time.perf_counter()

            # --- Step 1: Load data ---
            if source == "sql":
                if custom_sql:
                    self._queue.put(("progress", PROGRESS_LOAD_START, "Running query..."))
                    df = db_connector.fetch_query(source_arg, custom_sql)
                    source_label = "Custom SQL"
                else:
                    self._queue.put(("progress", PROGRESS_LOAD_START, f"Fetching {table_name}..."))
                    df = db_connector.fetch_table(source_arg, table_name)
                    source_label = table_name
                # SQL results have no file encoding; report as driver-level
                encoding_info = {"encoding": "SQL (driver)", "confidence": 1.0, "detected": False}
            else:
                self._queue.put(("progress", PROGRESS_LOAD_START, "Loading file..."))
                df, encoding_info = load_file(source_arg)
                source_label = os.path.basename(source_arg)

            t1 = time.perf_counter()
            if cancelled():
                self._queue.put(("cancelled",))
                return

            # --- Step 2: Profile each column ---
            # col_cb converts (columns_done, total) into a progress percentage and posts it
            def col_cb(n, total):
                span = PROGRESS_PROFILE_END - PROGRESS_PROFILE_START
                pct = PROGRESS_PROFILE_START + (n / total * span)
                self._queue.put(("progress", pct, f"Profiling column {n}/{total}..."))

            self._queue.put(("progress", PROGRESS_PROFILE_START, "Profiling columns..."))
            profile = profile_dataframe(df, progress_callback=col_cb)
            t2 = time.perf_counter()
            if cancelled():
                self._queue.put(("cancelled",))
                return

            # Inject encoding info into the profile summary so the interpreter can use it
            profile["summary"]["encoding"] = encoding_info["encoding"]
            profile["summary"]["encoding_confidence"] = encoding_info["confidence"]

            # --- Step 3: Interpret findings ---
            self._queue.put(("progress", PROGRESS_INTERPRET, "Interpreting findings..."))
            findings = interpret_profile(profile, df)
            t3 = time.perf_counter()
            if cancelled():
                self._queue.put(("cancelled",))
                return

            # --- Step 4: Render output into a string buffer ---
            self._queue.put(("progress", PROGRESS_RENDER, "Rendering..."))
            findings_text = format_findings(findings, file_name=source_label)
            buf = io.StringIO()
            # Render into the StringIO buffer rather than stdout so the GUI can display it
            console = Console(file=buf, highlight=False, width=char_width)
            print_profile(profile, console=console, encoding_info=encoding_info)
            t4 = time.perf_counter()

            # Collect per-stage timings for the header shown above the results
            timings = {
                "Data loading":     t1 - t0,
                "Column profiling": t2 - t1,
                "Interpretation":   t3 - t2,
                "Rendering":        t4 - t3,
            }
            # Post all results as a single queue message; the poller unpacks them together
            self._queue.put((
                "done", df, profile, findings, findings_text, buf.getvalue(),
                timings, source_label,
            ))
        except Exception as exc:
            _log.error("Analysis worker failed", exc_info=True)
            self._queue.put(("error", str(exc)))

    # ------------------------------------------------------------------
    # Queue polling
    # ------------------------------------------------------------------

    def _poll_queue(self) -> None:
        """Drain the queue and dispatch each message to the appropriate handler.

        Runs every 100 ms via after().  Stops rescheduling once a terminal message
        (done, error, cancelled, etc.) is received.
        """
        try:
            while True:
                # get_nowait raises queue.Empty immediately if nothing is waiting
                msg = self._queue.get_nowait()

                if msg[0] == "progress":
                    # Update the progress bar percentage and status text
                    _, pct, status = msg
                    self._progress_bar["value"] = pct
                    self._status_label.config(text=status)

                elif msg[0] == "done":
                    # Analysis complete — unpack and hand off to the completion handler
                    _, df, profile, findings, findings_text, rich_output, timings, source_label = msg
                    elapsed = time.perf_counter() - self._start_time
                    self._on_analysis_complete(
                        df, profile, findings, findings_text,
                        rich_output, elapsed, timings, source_label,
                    )
                    return  # stop rescheduling

                elif msg[0] == "clean_done":
                    # Cleaning complete — hand the results to the clean completion handler
                    _, cleaned_df, cleaning_log, detail_log, run_ts, fmt = msg
                    self._on_clean_complete(cleaned_df, cleaning_log, detail_log, run_ts, fmt)
                    return

                elif msg[0] == "clean_error":
                    self._on_clean_error(msg[1])
                    return

                elif msg[0] == "save_done":
                    _, fmt, path, audit_path, detail_path = msg
                    self._on_save_complete(fmt, path, audit_path, detail_path)
                    return

                elif msg[0] == "save_error":
                    # Re-enable all export buttons so the user can try again
                    self._hide_saving_indicator()
                    self._btn_json.config(state=tk.NORMAL)
                    self._btn_csv.config(state=tk.NORMAL)
                    self._btn_clean_csv.config(state=tk.NORMAL)
                    self._btn_clean_parquet.config(state=tk.NORMAL)
                    messagebox.showerror("Save failed", msg[1])
                    return

                elif msg[0] == "cancelled":
                    self._on_cancelled()
                    return

                elif msg[0] == "error":
                    self._on_analysis_error(msg[1])
                    return

                elif msg[0] == "tables":
                    # SQL connection result — pass to SqlPanel's completion handler
                    _, tables, conn_str = msg
                    self._sql_panel.on_connect_complete(tables, conn_str)
                    return

                elif msg[0] == "connect_error":
                    self._sql_panel.on_connect_error(msg[1])
                    return

        except queue.Empty:
            pass    # no messages this tick — continue polling

        # Update the elapsed timer while an operation is still running
        if self._polling_for in ("analysis", "cleaning", "saving"):
            self._timer_label.config(text=f"{time.perf_counter() - self._start_time:.1f}s")

        # Reschedule for the next tick
        self._poll_id = self.after(100, self._poll_queue)

    # ------------------------------------------------------------------
    # Analysis completion handlers
    # ------------------------------------------------------------------

    def _on_analysis_complete(
        self, df, profile, findings, findings_text,
        rich_output, elapsed, timings, source_label,
    ) -> None:
        """Store results, populate the results panel, and re-enable all controls."""
        # Persist results so export handlers can access them without re-running the analysis
        self._df = df
        self._profile = profile
        self._findings = findings
        self._source_label = source_label

        # Build the timing header that appears at the top of the results panel
        timing_lines = "\n".join(
            f"  {label:<20}{secs:.2f}s" for label, secs in timings.items()
        )
        header = f"Analysed in {elapsed:.2f}s\n{timing_lines}\n"
        output = header + "\n" + findings_text + rich_output

        # Temporarily re-enable the read-only text widget to update its content
        self._results.config(state=tk.NORMAL)
        self._results.delete("1.0", tk.END)
        self._results.insert(tk.END, output)
        self._results.config(state=tk.DISABLED)

        # Restore the UI to its ready state
        self._progress_frame.pack_forget()
        self._set_controls_enabled(True)
        self._btn_json.config(state=tk.NORMAL)
        self._btn_csv.config(state=tk.NORMAL)
        self._btn_clean_csv.config(state=tk.NORMAL)
        self._btn_clean_parquet.config(state=tk.NORMAL)

    def _on_analysis_error(self, message: str) -> None:
        """Clear any stale results, display the error, and restore controls."""
        # Discard any partial results so export buttons remain disabled
        self._df = None
        self._profile = None
        self._findings = None

        self._results.config(state=tk.NORMAL)
        self._results.delete("1.0", tk.END)
        self._results.insert(tk.END, f"Error: {message}")
        self._results.config(state=tk.DISABLED)

        self._progress_frame.pack_forget()
        self._set_controls_enabled(True)
        # Clean export is not available without a successfully loaded DataFrame
        self._btn_clean_csv.config(state=tk.DISABLED)
        self._btn_clean_parquet.config(state=tk.DISABLED)

    # ------------------------------------------------------------------
    # Cancellation
    # ------------------------------------------------------------------

    def _cancel_operation(self) -> None:
        """Signal the active worker to stop at its next checkpoint."""
        self._cancel_event.set()
        self._status_label.config(text="Cancelling…")
        # Disable Cancel to prevent double-clicks while the worker winds down
        self._btn_cancel.config(state=tk.DISABLED)

    def _on_cancelled(self) -> None:
        """Restore UI after a worker honoured the cancel signal."""
        self._progress_frame.pack_forget()
        self._btn_cancel.config(state=tk.NORMAL)
        self._set_controls_enabled(True)
        # Re-enable whichever export buttons still have backing data from a previous run
        json_csv_state = tk.NORMAL if self._profile is not None else tk.DISABLED
        clean_state    = tk.NORMAL if self._df is not None       else tk.DISABLED
        self._btn_json.config(state=json_csv_state)
        self._btn_csv.config(state=json_csv_state)
        self._btn_clean_csv.config(state=clean_state)
        self._btn_clean_parquet.config(state=clean_state)

    def _on_close(self) -> None:
        """Signal running workers to stop, then destroy the window."""
        self._cancel_event.set()
        self.master.destroy()

    # ------------------------------------------------------------------
    # Export helpers
    # ------------------------------------------------------------------

    def _export_stem(self) -> str:
        """Return a filesystem-safe stem for export filenames derived from the source label."""
        return self._source_label.replace(".", "_").replace(" ", "_").replace("/", "_")

    # Spec dict for profile export formats; avoids duplicating dialog/file parameters
    _PROFILE_EXPORT_SPECS = {
        "json": dict(
            title="Save profile as JSON",
            ext=".json",
            filetypes=[("JSON files", "*.json")],
            suffix="_profile.json",
            label="JSON",
            status="Saving JSON...",
        ),
        "csv": dict(
            title="Save profile as CSV",
            ext=".csv",
            filetypes=[("CSV files", "*.csv")],
            suffix="_profile.csv",
            label="CSV",
            status="Saving CSV...",
        ),
    }

    def _export_json(self) -> None:
        self._run_profile_export("json")

    def _export_csv(self) -> None:
        self._run_profile_export("csv")

    def _run_profile_export(self, fmt: str) -> None:
        """Open a save dialog for the given format, then write the profile export file."""
        spec = self._PROFILE_EXPORT_SPECS[fmt]
        path = filedialog.asksaveasfilename(
            title=spec["title"],
            defaultextension=spec["ext"],
            filetypes=spec["filetypes"],
            initialfile=f"{self._export_stem()}{spec['suffix']}",
        )
        if not path:
            return  # user cancelled the dialog
        self._show_saving_indicator(spec["status"])
        try:
            if fmt == "json":
                self._write_profile_json(path)
            else:
                self._write_profile_csv(path)
            messagebox.showinfo("Export complete", f"{spec['label']} saved to:\n{path}")
        except Exception as exc:
            _log.error("Profile export failed", exc_info=True)
            messagebox.showerror("Export failed", str(exc))
        finally:
            self._hide_saving_indicator()

    def _write_profile_json(self, path: str) -> None:
        """Serialise the full profile (summary + columns + findings) to a JSON file."""
        export = {
            "source": self._source_label,
            "summary": self._profile["summary"],
            "columns": self._profile["columns"],
            "findings": self._findings,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2)

    def _write_profile_csv(self, path: str) -> None:
        """Write a flat CSV with one row per column plus a leading __summary__ row."""
        fieldnames = [
            "column", "dtype",
            "null_count", "null_pct", "unique_count", "unique_pct",
            "min", "max", "mean", "median", "mode", "std", "q1", "q3",
            "skewness", "zeros", "negatives",
            "top_values", "avg_length",
            "findings",
        ]
        s = self._profile["summary"]
        # The __summary__ row packs dataset-level stats into the same column schema
        summary_row = {
            "column": "__summary__", "dtype": "",
            "null_count": s["total_nulls"], "null_pct": s["total_null_pct"],
            "unique_count": "", "unique_pct": "",
            "min": "", "max": "", "mean": "", "median": "", "mode": "",
            "std": "", "q1": "", "q3": "", "skewness": "",
            "zeros": "", "negatives": "",
            # Pack row/column/duplicate/memory info into the top_values cell for readability
            "top_values": (
                f"rows:{s['rows']}; columns:{s['columns']}; "
                f"duplicates:{s['duplicate_rows']}; memory_mb:{s['memory_mb']}"
            ),
            "avg_length": "",
            "findings": " | ".join(self._findings["table"]),
        }
        col_rows = []
        for col, stats in self._profile["columns"].items():
            is_numeric = "mean" in stats
            top_values_str = ""
            if not is_numeric:
                # Serialise the top-values dict as "val1:n1; val2:n2; ..." for Excel readability
                top_values_str = "; ".join(
                    f"{k}:{v}" for k, v in stats.get(TOP_VALUES_KEY, {}).items()
                )
            col_rows.append({
                "column": col, "dtype": stats["dtype"],
                "null_count": stats["null_count"], "null_pct": stats["null_pct"],
                "unique_count": stats["unique_count"], "unique_pct": stats["unique_pct"],
                # Numeric-only fields are left blank for categorical columns
                "min": stats.get("min", ""), "max": stats.get("max", ""),
                "mean": stats.get("mean", ""), "median": stats.get("median", ""),
                "mode": stats.get("mode", ""), "std": stats.get("std", ""),
                "q1": stats.get("q1", ""), "q3": stats.get("q3", ""),
                "skewness": stats.get("skewness", ""),
                "zeros": stats.get("zeros", ""), "negatives": stats.get("negatives", ""),
                "top_values": top_values_str, "avg_length": stats.get("avg_length", ""),
                # Join multiple column-level findings into one pipe-delimited string
                "findings": " | ".join(self._findings["columns"].get(col, [])),
            })
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(summary_row)
            writer.writerows(col_rows)

    # ------------------------------------------------------------------
    # Clean export
    # ------------------------------------------------------------------

    def _save_audit_log(self, clean_path: str, cleaning_log: list, run_ts: str) -> str:
        """Write an audit log CSV alongside clean_path. Returns the audit log path."""
        p = pathlib.Path(clean_path)
        # Place the audit log next to the cleaned file, with the same stem + _audit_log
        audit_path = p.with_name(p.stem + "_audit_log.csv")
        fieldnames = [
            "run_timestamp", "source", "operation",
            "column_name", "affected_count", "description",
        ]
        with open(audit_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for entry in cleaning_log:
                if not isinstance(entry, dict):
                    # Defensive: non-dict entries can appear if a cleaning step returned malformed data
                    _log.warning("Skipped non-dict cleaning log entry: %r", entry)
                    continue
                writer.writerow({
                    "run_timestamp":  run_ts,
                    "source":         self._source_label,
                    "operation":      entry["operation"],
                    "column_name":    entry.get("column_name", ""),
                    "affected_count": entry.get("affected_count", -1),
                    "description":    entry["description"],
                })
        return str(audit_path)

    def _save_detail_log(self, clean_path: str, detail_log: list, run_ts: str) -> str:
        """Write a per-cell/per-row detail audit log alongside clean_path. Returns the detail log path."""
        p = pathlib.Path(clean_path)
        # Place the detail log next to the cleaned file, with the same stem + _audit_detail
        detail_path = p.with_name(p.stem + "_audit_detail.csv")
        fieldnames = [
            "run_timestamp", "source", "original_row_number",
            "column_name", "action", "original_value", "new_value",
        ]
        with open(detail_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for entry in detail_log:
                if not isinstance(entry, dict):
                    _log.warning("Skipped non-dict detail log entry: %r", entry)
                    continue
                writer.writerow({
                    # Use per-entry timestamp if present, otherwise fall back to the run timestamp
                    "run_timestamp":       entry.get("run_timestamp", run_ts),
                    "source":              self._source_label,
                    "original_row_number": entry.get("original_row_number", ""),
                    "column_name":         entry.get("column_name", ""),
                    "action":              entry.get("action", ""),
                    "original_value":      entry.get("original_value", ""),
                    "new_value":           entry.get("new_value", ""),
                })
        return str(detail_path)

    def _export_clean_csv(self) -> None:
        self._start_clean("csv")

    def _export_clean_parquet(self) -> None:
        self._start_clean("parquet")

    def _start_clean(self, fmt: str) -> None:
        """Start the cleaning worker thread and show the progress frame."""
        # Reset progress UI before starting
        self._progress_bar["value"] = 0
        self._status_label.config(text="Cleaning data...")
        self._timer_label.config(text="0.0s")
        self._progress_frame.pack(fill=tk.X, pady=(0, 6), before=self._results.master)
        # Disable all controls while cleaning runs
        self._set_controls_enabled(False)
        self._btn_json.config(state=tk.DISABLED)
        self._btn_csv.config(state=tk.DISABLED)
        self._btn_clean_csv.config(state=tk.DISABLED)
        self._btn_clean_parquet.config(state=tk.DISABLED)

        self._polling_for = "cleaning"
        self._start_time = time.perf_counter()
        self._cancel_event.clear()
        threading.Thread(
            target=self._clean_worker, args=(fmt,),
            kwargs={"cancel_event": self._cancel_event}, daemon=True,
        ).start()
        self._poll_id = self.after(100, self._poll_queue)

    def _clean_worker(
        self, fmt: str, cancel_event: threading.Event | None = None
    ) -> None:
        """Background thread: run clean_dataframe() and post the result to the queue."""
        try:
            self._queue.put(("progress", 10, "Cleaning data..."))
            cleaned_df, cleaning_log, detail_log, run_ts = clean_dataframe(self._df)
            if cancel_event is not None and cancel_event.is_set():
                self._queue.put(("cancelled",))
                return
            self._queue.put(("progress", 100, "Done."))
            # Bundle the cleaned data and audit logs into a single queue message
            self._queue.put(("clean_done", cleaned_df, cleaning_log, detail_log, run_ts, fmt))
        except Exception as exc:
            _log.error("Cleaning worker failed", exc_info=True)
            self._queue.put(("clean_error", str(exc)))

    def _save_worker(
        self,
        fmt: str,
        path: str,
        cleaned_df,
        cleaning_log: list,
        detail_log: list,
        run_ts: str,
        cancel_event: threading.Event | None = None,
    ) -> None:
        """Background thread: write the cleaned file and both audit logs to disk."""
        # Check for cancellation one final time before touching the filesystem
        if cancel_event is not None and cancel_event.is_set():
            self._queue.put(("cancelled",))
            return
        try:
            # Write the cleaned dataset in the requested format
            if fmt == "csv":
                cleaned_df.write_csv(path)
            else:
                cleaned_df.write_parquet(path)
            # Write both audit logs alongside the cleaned file
            audit_path = self._save_audit_log(path, cleaning_log, run_ts)
            detail_path = self._save_detail_log(path, detail_log, run_ts)
            self._queue.put(("save_done", fmt, path, audit_path, detail_path))
        except Exception as exc:
            _log.error("Save worker failed", exc_info=True)
            self._queue.put(("save_error", str(exc)))

    def _on_clean_complete(
        self, cleaned_df, cleaning_log: list, detail_log: list, run_ts: str, fmt: str
    ) -> None:
        """Show a cleaning summary dialog, then open a save dialog for the cleaned file."""
        # Restore controls before showing the dialog so the app stays responsive
        self._progress_frame.pack_forget()
        self._set_controls_enabled(True)
        self._btn_json.config(state=tk.NORMAL)
        self._btn_csv.config(state=tk.NORMAL)
        self._btn_clean_csv.config(state=tk.NORMAL)
        self._btn_clean_parquet.config(state=tk.NORMAL)

        # Truncate the summary at 15 entries so the dialog doesn't grow too tall
        max_shown = 15
        if len(cleaning_log) > max_shown:
            shown = cleaning_log[:max_shown]
            shown.append(f"... and {len(cleaning_log) - max_shown} more")
        else:
            shown = cleaning_log
        summary = "\n".join(
            f"  \u2022 {entry['description'] if isinstance(entry, dict) else entry}"
            for entry in shown
        )
        messagebox.showinfo(
            "Cleaning Summary",
            f"Cleaning complete. {len(cleaning_log)} action(s):\n\n{summary}",
        )

        # Prompt for where to save the cleaned file
        stem = self._export_stem()
        if fmt == "csv":
            path = filedialog.asksaveasfilename(
                title="Save cleaned data as CSV",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv")],
                initialfile=f"{stem}_clean.csv",
            )
        else:
            path = filedialog.asksaveasfilename(
                title="Save cleaned data as Parquet",
                defaultextension=".parquet",
                filetypes=[("Parquet files", "*.parquet")],
                initialfile=f"{stem}_clean.parquet",
            )
        if not path:
            return  # user cancelled the save dialog

        # Hand off to the save worker so disk I/O doesn't block the GUI thread
        self._show_saving_indicator("Saving files...")
        self._btn_json.config(state=tk.DISABLED)
        self._btn_csv.config(state=tk.DISABLED)
        self._btn_clean_csv.config(state=tk.DISABLED)
        self._btn_clean_parquet.config(state=tk.DISABLED)
        self._polling_for = "saving"
        self._start_time = time.perf_counter()
        self._cancel_event.clear()
        threading.Thread(
            target=self._save_worker,
            args=(fmt, path, cleaned_df, cleaning_log, detail_log, run_ts),
            kwargs={"cancel_event": self._cancel_event},
            daemon=True,
        ).start()
        self._poll_id = self.after(100, self._poll_queue)

    def _on_save_complete(
        self, fmt: str, path: str, audit_path: str, detail_path: str
    ) -> None:
        """Hide the saving indicator and show a success message listing all written files."""
        self._hide_saving_indicator()
        self._btn_json.config(state=tk.NORMAL)
        self._btn_csv.config(state=tk.NORMAL)
        # Release the raw DataFrame now that the cleaned version is safely on disk.
        # Disabling the clean export buttons forces the user to re-analyse before cleaning again.
        self._df = None
        self._btn_clean_csv.config(state=tk.DISABLED)
        self._btn_clean_parquet.config(state=tk.DISABLED)
        label = "CSV" if fmt == "csv" else "Parquet"
        messagebox.showinfo(
            "Export complete",
            f"Clean {label} saved to:\n{path}\n\n"
            f"Audit log saved to:\n{audit_path}\n\n"
            f"Detail log saved to:\n{detail_path}",
        )

    def _on_clean_error(self, message: str) -> None:
        """Restore controls and show an error dialog after a cleaning failure."""
        self._progress_frame.pack_forget()
        self._set_controls_enabled(True)
        self._btn_json.config(state=tk.NORMAL)
        self._btn_csv.config(state=tk.NORMAL)
        self._btn_clean_csv.config(state=tk.NORMAL)
        self._btn_clean_parquet.config(state=tk.NORMAL)
        messagebox.showerror("Cleaning failed", message)


def launch() -> None:
    """Create the root window, instantiate the app, and start the Tkinter event loop."""
    root = tk.Tk()
    root.title("Python Data Profiler")
    root.minsize(700, 500)      # minimum size to prevent the layout from collapsing
    DataProfilerApp(root)
    root.mainloop()             # blocks until the window is closed
