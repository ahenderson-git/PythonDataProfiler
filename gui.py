import csv
import io
import json
import os
import pathlib
import queue
import threading
import time
import tkinter as tk
import tkinter.font as tkfont
from tkinter import filedialog, messagebox, ttk

from rich.console import Console

import db_connector
from profiler import load_file, profile_dataframe, print_profile
from interpreter import interpret_profile, format_findings
from cleaner import clean_dataframe


class DataProfilerApp(tk.Frame):
    def __init__(self, master: tk.Tk) -> None:
        super().__init__(master, padx=12, pady=12)
        self.pack(fill=tk.BOTH, expand=True)

        # Shared state
        self._profile = None
        self._findings = None
        self._source_label = ""   # used for export filename and findings header
        self._sql_connection_string = ""
        self._queue = queue.Queue()
        self._poll_id = None
        self._start_time = 0.0
        self._polling_for = "analysis"  # "connect", "analysis", or "cleaning"
        self._df = None  # loaded DataFrame, retained for clean export

        # SQL panel StringVars
        self._source_var   = tk.StringVar(value="file")
        self._auth_var     = tk.StringVar(value="sql")
        self._server_var   = tk.StringVar()
        self._database_var = tk.StringVar()
        self._username_var = tk.StringVar()
        self._password_var = tk.StringVar()
        self._table_var    = tk.StringVar()

        self._build_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        tk.Label(self, text="Python Data Profiler", font=("", 14, "bold")).pack(anchor="w", pady=(0, 6))

        # Source toggle row
        source_row = tk.Frame(self)
        source_row.pack(anchor="w", pady=(0, 6))
        tk.Label(source_row, text="Source:").pack(side=tk.LEFT)
        tk.Radiobutton(source_row, text="File", variable=self._source_var, value="file",
                       command=self._on_source_change).pack(side=tk.LEFT, padx=(6, 0))
        tk.Radiobutton(source_row, text="SQL Server", variable=self._source_var, value="sql",
                       command=self._on_source_change).pack(side=tk.LEFT, padx=(4, 0))

        # --- File frame ---
        self._file_frame = tk.Frame(self)
        self._file_frame.pack(fill=tk.X, pady=(0, 6))

        tk.Label(self._file_frame, text="File:").pack(side=tk.LEFT)
        self._file_var = tk.StringVar()
        tk.Entry(self._file_frame, textvariable=self._file_var, state="readonly", width=60).pack(side=tk.LEFT, padx=(6, 6))
        self._btn_browse = tk.Button(self._file_frame, text="Browse", command=self._browse)
        self._btn_browse.pack(side=tk.LEFT)

        # --- SQL frame (hidden initially) ---
        self._sql_frame = tk.Frame(self)
        self._build_sql_panel()

        # Action buttons row
        btn_row = tk.Frame(self)
        btn_row.pack(anchor="w", pady=(0, 6))

        self._btn_analyse = tk.Button(btn_row, text="Analyse", command=self._analyse, width=12)
        self._btn_analyse.pack(side=tk.LEFT, padx=(0, 8))

        self._btn_json = tk.Button(btn_row, text="Export JSON", command=self._export_json,
                                   state=tk.DISABLED, width=12)
        self._btn_json.pack(side=tk.LEFT, padx=(0, 4))

        self._btn_csv = tk.Button(btn_row, text="Export CSV", command=self._export_csv,
                                  state=tk.DISABLED, width=12)
        self._btn_csv.pack(side=tk.LEFT)

        self._btn_clean_csv = tk.Button(btn_row, text="Export Clean CSV",
                                        command=self._export_clean_csv,
                                        state=tk.DISABLED, width=16)
        self._btn_clean_csv.pack(side=tk.LEFT, padx=(8, 0))

        self._btn_clean_parquet = tk.Button(btn_row, text="Export Clean Parquet",
                                            command=self._export_clean_parquet,
                                            state=tk.DISABLED, width=18)
        self._btn_clean_parquet.pack(side=tk.LEFT, padx=(4, 0))

        # Progress frame (hidden until analysis/connect starts)
        self._progress_frame = tk.Frame(self)
        self._progress_bar = ttk.Progressbar(self._progress_frame, mode="determinate", maximum=100, length=400)
        self._progress_bar.pack(side=tk.LEFT, padx=(0, 8))
        self._status_label = tk.Label(self._progress_frame, text="", anchor="w")
        self._status_label.pack(side=tk.LEFT, expand=True, fill=tk.X)
        self._timer_label = tk.Label(self._progress_frame, text="", width=8, anchor="e")
        self._timer_label.pack(side=tk.RIGHT)

        # Results area
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
        v_scrollbar.config(command=self._results.yview)
        h_scrollbar.config(command=self._results.xview)

    def _build_sql_panel(self) -> None:
        f = self._sql_frame
        f.columnconfigure(1, weight=1)

        tk.Label(f, text="Server:").grid(row=0, column=0, sticky="e", pady=2, padx=(0, 4))
        tk.Entry(f, textvariable=self._server_var, width=40).grid(row=0, column=1, columnspan=3, sticky="ew")

        tk.Label(f, text="Database:").grid(row=1, column=0, sticky="e", pady=2, padx=(0, 4))
        tk.Entry(f, textvariable=self._database_var, width=40).grid(row=1, column=1, columnspan=3, sticky="ew")

        tk.Label(f, text="Auth:").grid(row=2, column=0, sticky="e", pady=2, padx=(0, 4))
        tk.Radiobutton(f, text="SQL auth", variable=self._auth_var, value="sql",
                       command=self._on_auth_change).grid(row=2, column=1, sticky="w")
        tk.Radiobutton(f, text="AAD Interactive", variable=self._auth_var, value="aad",
                       command=self._on_auth_change).grid(row=2, column=2, sticky="w")

        self._lbl_username = tk.Label(f, text="Username:")
        self._lbl_username.grid(row=3, column=0, sticky="e", pady=2, padx=(0, 4))
        self._ent_username = tk.Entry(f, textvariable=self._username_var, width=30)
        self._ent_username.grid(row=3, column=1, columnspan=2, sticky="ew")

        self._lbl_password = tk.Label(f, text="Password:")
        self._lbl_password.grid(row=4, column=0, sticky="e", pady=2, padx=(0, 4))
        self._ent_password = tk.Entry(f, textvariable=self._password_var, show="*", width=30)
        self._ent_password.grid(row=4, column=1, columnspan=2, sticky="ew")

        self._btn_connect = tk.Button(f, text="Connect", command=self._connect, width=10)
        self._btn_connect.grid(row=5, column=0, sticky="w", pady=(8, 2))

        self._table_combo = ttk.Combobox(f, textvariable=self._table_var, state="disabled", width=38)
        self._table_combo.grid(row=5, column=1, columnspan=3, sticky="ew", pady=(8, 2))

        tk.Label(f, text="Custom SQL:").grid(row=6, column=0, sticky="ne", pady=(6, 0), padx=(0, 4))
        self._sql_text = tk.Text(f, height=3, width=50, wrap=tk.WORD)
        self._sql_text.grid(row=6, column=1, columnspan=3, sticky="ew", pady=(6, 0))

        tk.Label(f, text="If provided, custom SQL overrides the table selection above.",
                 fg="gray").grid(row=7, column=1, columnspan=3, sticky="w")

        self._sql_status_label = tk.Label(f, text="", anchor="w")
        self._sql_status_label.grid(row=8, column=0, columnspan=4, sticky="ew", pady=(4, 0))

    # ------------------------------------------------------------------
    # Source / auth toggle handlers
    # ------------------------------------------------------------------

    def _on_source_change(self) -> None:
        if self._source_var.get() == "file":
            self._sql_frame.pack_forget()
            self._file_frame.pack(fill=tk.X, pady=(0, 6),
                                  before=self._btn_analyse.master)
        else:
            self._file_frame.pack_forget()
            self._sql_frame.pack(fill=tk.X, pady=(0, 6),
                                 before=self._btn_analyse.master)

    def _on_auth_change(self) -> None:
        if self._auth_var.get() == "aad":
            self._lbl_username.grid_remove()
            self._ent_username.grid_remove()
            self._lbl_password.grid_remove()
            self._ent_password.grid_remove()
        else:
            self._lbl_username.grid()
            self._ent_username.grid()
            self._lbl_password.grid()
            self._ent_password.grid()

    # ------------------------------------------------------------------
    # File browse
    # ------------------------------------------------------------------

    def _browse(self) -> None:
        path = filedialog.askopenfilename(
            title="Select a file to analyse",
            filetypes=[("Supported files", "*.csv *.parquet"),
                       ("CSV files", "*.csv"), ("Parquet files", "*.parquet")],
        )
        if path:
            self._file_var.set(path)

    # ------------------------------------------------------------------
    # SQL connect
    # ------------------------------------------------------------------

    def _connect(self) -> None:
        server   = self._server_var.get().strip()
        database = self._database_var.get().strip()
        auth     = self._auth_var.get()
        username = self._username_var.get().strip()
        password = self._password_var.get()

        try:
            conn_str = db_connector.build_connection_string(server, database, auth, username, password)
        except ValueError as exc:
            messagebox.showwarning("Missing fields", str(exc))
            return

        self._btn_connect.config(state=tk.DISABLED)
        self._table_combo.config(state="disabled")
        self._sql_status_label.config(text="Connecting...", fg="black")

        self._polling_for = "connect"
        threading.Thread(target=self._connect_worker, args=(conn_str,), daemon=True).start()
        self.after(100, self._poll_queue)

    def _connect_worker(self, connection_string: str) -> None:
        try:
            tables = db_connector.list_tables(connection_string)
            self._queue.put(("tables", tables, connection_string))
        except Exception as exc:
            self._queue.put(("connect_error", str(exc)))

    # ------------------------------------------------------------------
    # Analyse
    # ------------------------------------------------------------------

    def _set_controls_enabled(self, enabled: bool) -> None:
        state = tk.NORMAL if enabled else tk.DISABLED
        self._btn_browse.config(state=state)
        self._btn_analyse.config(state=state)
        self._btn_connect.config(state=state)
        combo_state = "readonly" if enabled and self._table_combo["values"] else "disabled"
        self._table_combo.config(state=combo_state)

    def _show_saving_indicator(self, status: str = "Saving...") -> None:
        self._progress_bar.config(mode="indeterminate")
        self._progress_bar.start(10)
        self._status_label.config(text=status)
        self._timer_label.config(text="")
        self._progress_frame.pack(fill=tk.X, pady=(0, 6), before=self._results.master)
        self.update_idletasks()

    def _hide_saving_indicator(self) -> None:
        self._progress_bar.stop()
        self._progress_bar.config(mode="determinate", value=0)
        self._progress_frame.pack_forget()

    def _analyse(self) -> None:
        font = tkfont.Font(font=self._results.cget("font"))
        char_width = max(40, self._results.winfo_width() // font.measure("0"))

        if self._source_var.get() == "file":
            path = self._file_var.get()
            if not path:
                messagebox.showwarning("No file selected", "Please select a file before clicking Analyse.")
                return
            thread_args = ("file", path, char_width)
        else:
            if not self._sql_connection_string:
                messagebox.showwarning("Not connected", "Please click Connect before clicking Analyse.")
                return
            custom_sql = self._sql_text.get("1.0", tk.END).strip()
            table_name = self._table_var.get().strip()
            if not custom_sql and not table_name:
                messagebox.showwarning("No table selected",
                                       "Please select a table from the list or enter a custom SQL query.")
                return
            thread_args = ("sql", self._sql_connection_string, char_width, table_name, custom_sql)

        self._progress_bar["value"] = 0
        self._status_label.config(text="Starting...")
        self._timer_label.config(text="0.0s")
        self._progress_frame.pack(fill=tk.X, pady=(0, 6), before=self._results.master)
        self._set_controls_enabled(False)
        self._btn_json.config(state=tk.DISABLED)
        self._btn_csv.config(state=tk.DISABLED)
        self._btn_clean_csv.config(state=tk.DISABLED)
        self._btn_clean_parquet.config(state=tk.DISABLED)

        self._polling_for = "analysis"
        self._start_time = time.perf_counter()
        threading.Thread(target=self._worker, args=thread_args, daemon=True).start()
        self._poll_id = self.after(100, self._poll_queue)

    # ------------------------------------------------------------------
    # Worker thread
    # ------------------------------------------------------------------

    def _worker(self, source: str, source_arg: str, char_width: int,
                table_name: str = "", custom_sql: str = "") -> None:
        try:
            t0 = time.perf_counter()

            if source == "sql":
                if custom_sql:
                    self._queue.put(("progress", 2, "Running query..."))
                    df = db_connector.fetch_query(source_arg, custom_sql)
                    source_label = "Custom SQL"
                else:
                    self._queue.put(("progress", 2, f"Fetching {table_name}..."))
                    df = db_connector.fetch_table(source_arg, table_name)
                    source_label = table_name
                encoding_info = {"encoding": "SQL (driver)", "confidence": 1.0, "detected": False}
            else:
                self._queue.put(("progress", 2, "Loading file..."))
                df, encoding_info = load_file(source_arg)
                source_label = os.path.basename(source_arg)

            t1 = time.perf_counter()

            def col_cb(n, total):
                pct = 5 + (n / total * 75)
                self._queue.put(("progress", pct, f"Profiling column {n}/{total}..."))

            self._queue.put(("progress", 5, "Profiling columns..."))
            profile = profile_dataframe(df, progress_callback=col_cb)
            t2 = time.perf_counter()

            profile["summary"]["encoding"] = encoding_info["encoding"]
            profile["summary"]["encoding_confidence"] = encoding_info["confidence"]

            self._queue.put(("progress", 82, "Interpreting findings..."))
            findings = interpret_profile(profile, df)
            t3 = time.perf_counter()

            self._queue.put(("progress", 95, "Rendering..."))
            findings_text = format_findings(findings, file_name=source_label)
            buf = io.StringIO()
            console = Console(file=buf, highlight=False, width=char_width)
            print_profile(profile, console=console, encoding_info=encoding_info)
            t4 = time.perf_counter()

            timings = {
                "Data loading":     t1 - t0,
                "Column profiling": t2 - t1,
                "Interpretation":   t3 - t2,
                "Rendering":        t4 - t3,
            }
            self._queue.put(("done", df, profile, findings, findings_text, buf.getvalue(),
                             timings, source_label))
        except Exception as exc:
            self._queue.put(("error", str(exc)))

    # ------------------------------------------------------------------
    # Queue polling
    # ------------------------------------------------------------------

    def _poll_queue(self) -> None:
        try:
            while True:
                msg = self._queue.get_nowait()

                if msg[0] == "progress":
                    _, pct, status = msg
                    self._progress_bar["value"] = pct
                    self._status_label.config(text=status)

                elif msg[0] == "done":
                    _, df, profile, findings, findings_text, rich_output, timings, source_label = msg
                    elapsed = time.perf_counter() - self._start_time
                    self._on_analysis_complete(df, profile, findings, findings_text,
                                               rich_output, elapsed, timings, source_label)
                    return

                elif msg[0] == "clean_done":
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
                    self._hide_saving_indicator()
                    self._btn_json.config(state=tk.NORMAL)
                    self._btn_csv.config(state=tk.NORMAL)
                    self._btn_clean_csv.config(state=tk.NORMAL)
                    self._btn_clean_parquet.config(state=tk.NORMAL)
                    messagebox.showerror("Save failed", msg[1])
                    return

                elif msg[0] == "error":
                    self._on_analysis_error(msg[1])
                    return

                elif msg[0] == "tables":
                    _, tables, conn_str = msg
                    self._on_connect_complete(tables, conn_str)
                    return

                elif msg[0] == "connect_error":
                    self._on_connect_error(msg[1])
                    return

        except queue.Empty:
            pass

        if self._polling_for in ("analysis", "cleaning", "saving"):
            self._timer_label.config(text=f"{time.perf_counter() - self._start_time:.1f}s")

        self._poll_id = self.after(100, self._poll_queue)

    # ------------------------------------------------------------------
    # Connect completion handlers
    # ------------------------------------------------------------------

    def _on_connect_complete(self, tables: list, conn_str: str) -> None:
        self._sql_connection_string = conn_str
        self._btn_connect.config(state=tk.NORMAL)

        if not tables:
            messagebox.showwarning("No tables found",
                                   "No tables or views are accessible with this connection.")
            self._sql_status_label.config(text="Connected — no accessible tables found.", fg="orange")
            return

        names = [t["full_name"] for t in tables]
        self._table_combo["values"] = names
        self._table_combo.config(state="readonly")
        self._table_combo.current(0)
        self._sql_status_label.config(
            text=f"Connected — {len(tables)} table(s)/view(s) found.", fg="green"
        )

    def _on_connect_error(self, message: str) -> None:
        self._btn_connect.config(state=tk.NORMAL)
        self._sql_status_label.config(text=f"Connection failed: {message}", fg="red")

    # ------------------------------------------------------------------
    # Analysis completion handlers
    # ------------------------------------------------------------------

    def _on_analysis_complete(self, df, profile, findings, findings_text,
                               rich_output, elapsed, timings, source_label) -> None:
        self._df = df
        self._profile = profile
        self._findings = findings
        self._source_label = source_label

        timing_lines = "\n".join(f"  {label:<20}{secs:.2f}s" for label, secs in timings.items())
        header = f"Analysed in {elapsed:.2f}s\n{timing_lines}\n"
        output = header + "\n" + findings_text + rich_output

        self._results.config(state=tk.NORMAL)
        self._results.delete("1.0", tk.END)
        self._results.insert(tk.END, output)
        self._results.config(state=tk.DISABLED)

        self._progress_frame.pack_forget()
        self._set_controls_enabled(True)
        self._btn_json.config(state=tk.NORMAL)
        self._btn_csv.config(state=tk.NORMAL)
        self._btn_clean_csv.config(state=tk.NORMAL)
        self._btn_clean_parquet.config(state=tk.NORMAL)

    def _on_analysis_error(self, message: str) -> None:
        self._df = None
        self._profile = None
        self._findings = None

        self._results.config(state=tk.NORMAL)
        self._results.delete("1.0", tk.END)
        self._results.insert(tk.END, f"Error: {message}")
        self._results.config(state=tk.DISABLED)

        self._progress_frame.pack_forget()
        self._set_controls_enabled(True)
        self._btn_clean_csv.config(state=tk.DISABLED)
        self._btn_clean_parquet.config(state=tk.DISABLED)

    # ------------------------------------------------------------------
    # Export helpers
    # ------------------------------------------------------------------

    def _export_stem(self) -> str:
        return self._source_label.replace(".", "_").replace(" ", "_").replace("/", "_")

    def _export_json(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Save profile as JSON",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json")],
            initialfile=f"{self._export_stem()}_profile.json",
        )
        if not path:
            return
        export = {
            "source": self._source_label,
            "summary": self._profile["summary"],
            "columns": self._profile["columns"],
            "findings": self._findings,
        }
        self._show_saving_indicator("Saving JSON...")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(export, f, indent=2)
            messagebox.showinfo("Export complete", f"JSON saved to:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))
        finally:
            self._hide_saving_indicator()

    def _export_csv(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Save profile as CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            initialfile=f"{self._export_stem()}_profile.csv",
        )
        if not path:
            return

        fieldnames = [
            "column", "dtype",
            "null_count", "null_pct", "unique_count", "unique_pct",
            "min", "max", "mean", "median", "mode", "std", "q1", "q3",
            "skewness", "zeros", "negatives",
            "top_values", "avg_length",
            "findings",
        ]

        s = self._profile["summary"]
        summary_row = {
            "column": "__summary__", "dtype": "",
            "null_count": s["total_nulls"], "null_pct": s["total_null_pct"],
            "unique_count": "", "unique_pct": "",
            "min": "", "max": "", "mean": "", "median": "", "mode": "",
            "std": "", "q1": "", "q3": "", "skewness": "",
            "zeros": "", "negatives": "",
            "top_values": (f"rows:{s['rows']}; columns:{s['columns']}; "
                           f"duplicates:{s['duplicate_rows']}; memory_mb:{s['memory_mb']}"),
            "avg_length": "",
            "findings": " | ".join(self._findings["table"]),
        }

        col_rows = []
        for col, stats in self._profile["columns"].items():
            is_numeric = "mean" in stats
            top_values_str = ""
            if not is_numeric:
                top_values_str = "; ".join(f"{k}:{v}" for k, v in stats.get("top_5_values", {}).items())
            row = {
                "column": col, "dtype": stats["dtype"],
                "null_count": stats["null_count"], "null_pct": stats["null_pct"],
                "unique_count": stats["unique_count"], "unique_pct": stats["unique_pct"],
                "min": stats.get("min", ""), "max": stats.get("max", ""),
                "mean": stats.get("mean", ""), "median": stats.get("median", ""),
                "mode": stats.get("mode", ""), "std": stats.get("std", ""),
                "q1": stats.get("q1", ""), "q3": stats.get("q3", ""),
                "skewness": stats.get("skewness", ""),
                "zeros": stats.get("zeros", ""), "negatives": stats.get("negatives", ""),
                "top_values": top_values_str, "avg_length": stats.get("avg_length", ""),
                "findings": " | ".join(self._findings["columns"].get(col, [])),
            }
            col_rows.append(row)

        self._show_saving_indicator("Saving CSV...")
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(summary_row)
                writer.writerows(col_rows)
            messagebox.showinfo("Export complete", f"CSV saved to:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))
        finally:
            self._hide_saving_indicator()

    # ------------------------------------------------------------------
    # Clean export
    # ------------------------------------------------------------------

    def _save_audit_log(self, clean_path: str, cleaning_log: list, run_ts: str) -> str:
        """Write an audit log CSV alongside clean_path. Returns the audit log path."""
        p = pathlib.Path(clean_path)
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
                    continue
                writer.writerow({
                    "run_timestamp": run_ts,
                    "source": self._source_label,
                    "operation": entry["operation"],
                    "column_name": entry.get("column_name", ""),
                    "affected_count": entry.get("affected_count", -1),
                    "description": entry["description"],
                })
        return str(audit_path)

    def _save_detail_log(self, clean_path: str, detail_log: list, run_ts: str) -> str:
        """Write a per-cell/per-row detail audit log alongside clean_path. Returns the detail log path."""
        p = pathlib.Path(clean_path)
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
                    continue
                writer.writerow({
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
        self._progress_bar["value"] = 0
        self._status_label.config(text="Cleaning data...")
        self._timer_label.config(text="0.0s")
        self._progress_frame.pack(fill=tk.X, pady=(0, 6), before=self._results.master)
        self._set_controls_enabled(False)
        self._btn_json.config(state=tk.DISABLED)
        self._btn_csv.config(state=tk.DISABLED)
        self._btn_clean_csv.config(state=tk.DISABLED)
        self._btn_clean_parquet.config(state=tk.DISABLED)

        self._polling_for = "cleaning"
        self._start_time = time.perf_counter()
        threading.Thread(target=self._clean_worker, args=(fmt,), daemon=True).start()
        self._poll_id = self.after(100, self._poll_queue)

    def _clean_worker(self, fmt: str) -> None:
        try:
            self._queue.put(("progress", 10, "Cleaning data..."))
            cleaned_df, cleaning_log, detail_log, run_ts = clean_dataframe(self._df)
            self._queue.put(("progress", 100, "Done."))
            self._queue.put(("clean_done", cleaned_df, cleaning_log, detail_log, run_ts, fmt))
        except Exception as exc:
            self._queue.put(("clean_error", str(exc)))

    def _save_worker(
        self,
        fmt: str,
        path: str,
        cleaned_df,
        cleaning_log: list,
        detail_log: list,
        run_ts: str,
    ) -> None:
        try:
            if fmt == "csv":
                cleaned_df.to_csv(path, index=False)
            else:
                cleaned_df.to_parquet(path, index=False)
            audit_path = self._save_audit_log(path, cleaning_log, run_ts)
            detail_path = self._save_detail_log(path, detail_log, run_ts)
            self._queue.put(("save_done", fmt, path, audit_path, detail_path))
        except Exception as exc:
            self._queue.put(("save_error", str(exc)))

    def _on_clean_complete(self, cleaned_df, cleaning_log: list, detail_log: list, run_ts: str, fmt: str) -> None:
        self._progress_frame.pack_forget()
        self._set_controls_enabled(True)
        self._btn_json.config(state=tk.NORMAL)
        self._btn_csv.config(state=tk.NORMAL)
        self._btn_clean_csv.config(state=tk.NORMAL)
        self._btn_clean_parquet.config(state=tk.NORMAL)

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
            return

        self._show_saving_indicator("Saving files...")
        self._btn_json.config(state=tk.DISABLED)
        self._btn_csv.config(state=tk.DISABLED)
        self._btn_clean_csv.config(state=tk.DISABLED)
        self._btn_clean_parquet.config(state=tk.DISABLED)
        self._polling_for = "saving"
        self._start_time = time.perf_counter()
        threading.Thread(
            target=self._save_worker,
            args=(fmt, path, cleaned_df, cleaning_log, detail_log, run_ts),
            daemon=True,
        ).start()
        self._poll_id = self.after(100, self._poll_queue)

    def _on_save_complete(self, fmt: str, path: str, audit_path: str, detail_path: str) -> None:
        self._hide_saving_indicator()
        self._btn_json.config(state=tk.NORMAL)
        self._btn_csv.config(state=tk.NORMAL)
        self._btn_clean_csv.config(state=tk.NORMAL)
        self._btn_clean_parquet.config(state=tk.NORMAL)
        label = "CSV" if fmt == "csv" else "Parquet"
        messagebox.showinfo(
            "Export complete",
            f"Clean {label} saved to:\n{path}\n\n"
            f"Audit log saved to:\n{audit_path}\n\n"
            f"Detail log saved to:\n{detail_path}",
        )

    def _on_clean_error(self, message: str) -> None:
        self._progress_frame.pack_forget()
        self._set_controls_enabled(True)
        self._btn_json.config(state=tk.NORMAL)
        self._btn_csv.config(state=tk.NORMAL)
        self._btn_clean_csv.config(state=tk.NORMAL)
        self._btn_clean_parquet.config(state=tk.NORMAL)
        messagebox.showerror("Cleaning failed", message)


def launch() -> None:
    root = tk.Tk()
    root.title("Python Data Profiler")
    root.minsize(700, 500)
    DataProfilerApp(root)
    root.mainloop()
