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

from profiler import load_file, profile_dataframe, print_profile
from interpreter import interpret_profile, format_findings


class DataProfilerApp(tk.Frame):
    def __init__(self, master: tk.Tk) -> None:
        super().__init__(master, padx=12, pady=12)
        self.pack(fill=tk.BOTH, expand=True)
        self._profile = None
        self._findings = None
        self._source_path = ""
        self._queue = queue.Queue()
        self._poll_id = None
        self._start_time = 0.0
        self._build_ui()

    def _build_ui(self) -> None:
        tk.Label(self, text="Python Data Profiler", font=("", 14, "bold")).pack(anchor="w", pady=(0, 10))

        # File selector row
        file_row = tk.Frame(self)
        file_row.pack(fill=tk.X, pady=(0, 8))

        tk.Label(file_row, text="File:").pack(side=tk.LEFT)

        self._file_var = tk.StringVar()
        self._entry = tk.Entry(file_row, textvariable=self._file_var, state="readonly", width=60)
        self._entry.pack(side=tk.LEFT, padx=(6, 6))

        self._btn_browse = tk.Button(file_row, text="Browse", command=self._browse)
        self._btn_browse.pack(side=tk.LEFT)

        # Action buttons row
        btn_row = tk.Frame(self)
        btn_row.pack(anchor="w", pady=(0, 8))

        self._btn_analyse = tk.Button(btn_row, text="Analyse", command=self._analyse, width=12)
        self._btn_analyse.pack(side=tk.LEFT, padx=(0, 8))

        self._btn_json = tk.Button(btn_row, text="Export JSON", command=self._export_json, state=tk.DISABLED, width=12)
        self._btn_json.pack(side=tk.LEFT, padx=(0, 4))

        self._btn_csv = tk.Button(btn_row, text="Export CSV", command=self._export_csv, state=tk.DISABLED, width=12)
        self._btn_csv.pack(side=tk.LEFT)

        # Progress frame (hidden until analysis starts)
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
            text_frame,
            wrap=tk.NONE,
            font=("Courier", 10),
            state=tk.DISABLED,
            yscrollcommand=v_scrollbar.set,
            xscrollcommand=h_scrollbar.set,
        )
        self._results.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        v_scrollbar.config(command=self._results.yview)
        h_scrollbar.config(command=self._results.xview)

    def _browse(self) -> None:
        path = filedialog.askopenfilename(
            title="Select a file to analyse",
            filetypes=[("Supported files", "*.csv *.parquet"), ("CSV files", "*.csv"), ("Parquet files", "*.parquet")],
        )
        if path:
            self._file_var.set(path)

    def _set_controls_enabled(self, enabled: bool) -> None:
        state = tk.NORMAL if enabled else tk.DISABLED
        self._btn_browse.config(state=state)
        self._btn_analyse.config(state=state)

    def _analyse(self) -> None:
        path = self._file_var.get()
        if not path:
            messagebox.showwarning("No file selected", "Please select a file before clicking Analyse.")
            return

        # Measure char width on the main thread (tkfont is not thread-safe)
        font = tkfont.Font(font=self._results.cget("font"))
        char_width = max(40, self._results.winfo_width() // font.measure("0"))

        # Show progress UI and disable controls
        self._progress_bar["value"] = 0
        self._status_label.config(text="Loading file...")
        self._timer_label.config(text="0.0s")
        self._progress_frame.pack(fill=tk.X, pady=(0, 6), before=self._results.master)
        self._set_controls_enabled(False)
        self._btn_json.config(state=tk.DISABLED)
        self._btn_csv.config(state=tk.DISABLED)

        self._start_time = time.perf_counter()
        threading.Thread(target=self._worker, args=(path, char_width), daemon=True).start()
        self._poll_id = self.after(100, self._poll_queue)

    def _worker(self, path: str, char_width: int) -> None:
        """Runs in background thread. Must not touch Tkinter widgets directly."""
        try:
            t0 = time.perf_counter()
            df = load_file(path)
            t1 = time.perf_counter()
            self._queue.put(("progress", 5, "Loading file..."))

            def col_cb(n, total):
                pct = 5 + (n / total * 75)
                self._queue.put(("progress", pct, f"Profiling column {n}/{total}..."))

            profile = profile_dataframe(df, progress_callback=col_cb)
            t2 = time.perf_counter()
            self._queue.put(("progress", 82, "Interpreting findings..."))

            findings = interpret_profile(profile, df)
            t3 = time.perf_counter()
            self._queue.put(("progress", 95, "Rendering..."))

            findings_text = format_findings(findings, file_name=os.path.basename(path))
            buf = io.StringIO()
            console = Console(file=buf, highlight=False, width=char_width)
            print_profile(profile, console=console)
            t4 = time.perf_counter()

            timings = {
                "File loading":     t1 - t0,
                "Column profiling": t2 - t1,
                "Interpretation":   t3 - t2,
                "Rendering":        t4 - t3,
            }
            self._queue.put(("done", profile, findings, findings_text, buf.getvalue(), timings))
        except Exception as exc:
            self._queue.put(("error", str(exc)))

    def _poll_queue(self) -> None:
        """Runs on the main thread every 100ms to drain the worker queue."""
        try:
            while True:
                msg = self._queue.get_nowait()
                if msg[0] == "progress":
                    _, pct, status = msg
                    self._progress_bar["value"] = pct
                    self._status_label.config(text=status)
                elif msg[0] == "done":
                    _, profile, findings, findings_text, rich_output, timings = msg
                    elapsed = time.perf_counter() - self._start_time
                    self._on_analysis_complete(profile, findings, findings_text, rich_output, elapsed, timings)
                    return
                elif msg[0] == "error":
                    self._on_analysis_error(msg[1])
                    return
        except queue.Empty:
            pass

        # Update live timer
        self._timer_label.config(text=f"{time.perf_counter() - self._start_time:.1f}s")
        self._poll_id = self.after(100, self._poll_queue)

    def _on_analysis_complete(self, profile, findings, findings_text, rich_output, elapsed, timings) -> None:
        self._profile = profile
        self._findings = findings
        self._source_path = self._file_var.get()

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

    def _on_analysis_error(self, message: str) -> None:
        self._profile = None
        self._findings = None

        self._results.config(state=tk.NORMAL)
        self._results.delete("1.0", tk.END)
        self._results.insert(tk.END, f"Error: {message}")
        self._results.config(state=tk.DISABLED)

        self._progress_frame.pack_forget()
        self._set_controls_enabled(True)

    def _export_json(self) -> None:
        stem = pathlib.Path(self._source_path).stem
        path = filedialog.asksaveasfilename(
            title="Save profile as JSON",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json")],
            initialfile=f"{stem}_profile.json",
        )
        if not path:
            return
        export = {
            "source_file": os.path.basename(self._source_path),
            "summary": self._profile["summary"],
            "columns": self._profile["columns"],
            "findings": self._findings,
        }
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(export, f, indent=2)
            messagebox.showinfo("Export complete", f"JSON saved to:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))

    def _export_csv(self) -> None:
        stem = pathlib.Path(self._source_path).stem
        path = filedialog.asksaveasfilename(
            title="Save profile as CSV",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            initialfile=f"{stem}_profile.csv",
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
        table_findings_str = " | ".join(self._findings["table"])

        summary_row = {
            "column": "__summary__",
            "dtype": "",
            "null_count": s["total_nulls"],
            "null_pct": s["total_null_pct"],
            "unique_count": "",
            "unique_pct": "",
            "min": "", "max": "", "mean": "", "median": "", "mode": "",
            "std": "", "q1": "", "q3": "", "skewness": "",
            "zeros": "", "negatives": "",
            "top_values": f"rows:{s['rows']}; columns:{s['columns']}; duplicates:{s['duplicate_rows']}; memory_mb:{s['memory_mb']}",
            "avg_length": "",
            "findings": table_findings_str,
        }

        col_rows = []
        for col, stats in self._profile["columns"].items():
            is_numeric = "mean" in stats
            top_values_str = ""
            if not is_numeric:
                top_values_str = "; ".join(f"{k}:{v}" for k, v in stats.get("top_5_values", {}).items())

            col_findings_str = " | ".join(self._findings["columns"].get(col, []))

            row = {
                "column": col,
                "dtype": stats["dtype"],
                "null_count": stats["null_count"],
                "null_pct": stats["null_pct"],
                "unique_count": stats["unique_count"],
                "unique_pct": stats["unique_pct"],
                "min": stats.get("min", ""),
                "max": stats.get("max", ""),
                "mean": stats.get("mean", ""),
                "median": stats.get("median", ""),
                "mode": stats.get("mode", ""),
                "std": stats.get("std", ""),
                "q1": stats.get("q1", ""),
                "q3": stats.get("q3", ""),
                "skewness": stats.get("skewness", ""),
                "zeros": stats.get("zeros", ""),
                "negatives": stats.get("negatives", ""),
                "top_values": top_values_str,
                "avg_length": stats.get("avg_length", ""),
                "findings": col_findings_str,
            }
            col_rows.append(row)

        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(summary_row)
                writer.writerows(col_rows)
            messagebox.showinfo("Export complete", f"CSV saved to:\n{path}")
        except Exception as exc:
            messagebox.showerror("Export failed", str(exc))


def launch() -> None:
    root = tk.Tk()
    root.title("Python Data Profiler")
    root.minsize(700, 500)
    DataProfilerApp(root)
    root.mainloop()
