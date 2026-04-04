import csv
import io
import json
import os
import pathlib
import time
import tkinter as tk
import tkinter.font as tkfont
from tkinter import filedialog, messagebox

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
        self._build_ui()

    def _build_ui(self) -> None:
        tk.Label(self, text="Python Data Profiler", font=("", 14, "bold")).pack(anchor="w", pady=(0, 10))

        # File selector row
        file_row = tk.Frame(self)
        file_row.pack(fill=tk.X, pady=(0, 8))

        tk.Label(file_row, text="File:").pack(side=tk.LEFT)

        self._file_var = tk.StringVar()
        tk.Entry(file_row, textvariable=self._file_var, state="readonly", width=60).pack(side=tk.LEFT, padx=(6, 6))

        tk.Button(file_row, text="Browse", command=self._browse).pack(side=tk.LEFT)

        # Action buttons row
        btn_row = tk.Frame(self)
        btn_row.pack(anchor="w", pady=(0, 8))

        tk.Button(btn_row, text="Analyse", command=self._analyse, width=12).pack(side=tk.LEFT, padx=(0, 8))

        self._btn_json = tk.Button(btn_row, text="Export JSON", command=self._export_json, state=tk.DISABLED, width=12)
        self._btn_json.pack(side=tk.LEFT, padx=(0, 4))

        self._btn_csv = tk.Button(btn_row, text="Export CSV", command=self._export_csv, state=tk.DISABLED, width=12)
        self._btn_csv.pack(side=tk.LEFT)

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

    def _analyse(self) -> None:
        path = self._file_var.get()
        if not path:
            messagebox.showwarning("No file selected", "Please select a file before clicking Analyse.")
            return

        try:
            start = time.perf_counter()
            df = load_file(path)
            profile = profile_dataframe(df)
            findings = interpret_profile(profile, df)
            findings_text = format_findings(findings, file_name=os.path.basename(path))
            font = tkfont.Font(font=self._results.cget("font"))
            char_width = max(40, self._results.winfo_width() // font.measure("0"))
            buf = io.StringIO()
            console = Console(file=buf, highlight=False, width=char_width)
            print_profile(profile, console=console)
            elapsed = time.perf_counter() - start
            output = f"Analysed in {elapsed:.2f}s\n\n" + findings_text + buf.getvalue()
            self._profile = profile
            self._findings = findings
            self._source_path = path
            self._btn_json.config(state=tk.NORMAL)
            self._btn_csv.config(state=tk.NORMAL)
        except Exception as exc:
            output = f"Error: {exc}"
            self._profile = None
            self._findings = None
            self._btn_json.config(state=tk.DISABLED)
            self._btn_csv.config(state=tk.DISABLED)

        self._results.config(state=tk.NORMAL)
        self._results.delete("1.0", tk.END)
        self._results.insert(tk.END, output)
        self._results.config(state=tk.DISABLED)

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
