"""
Device Data Viewer — Desktop application for viewing device data
from CSV files in the data/ folder with statistics & graphs.
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
import csv
import math
import os
import shutil
import threading
import statistics
from collections import defaultdict
import matplotlib
matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure


# ── Data directory ────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
COLUMNS = ("id", "device_id", "device_time", "server_time", "sat", "hgb", "sensor")
EXPECTED_COLUMNS = {"Device Time", "Saturation", "tGb", "Sensor", "Marker"}
DATE_FORMATS = ("%m/%d/%Y %H:%M", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p")

def _try_parse(value: str, fmt: str) -> datetime | None:
    try:
        return datetime.strptime(value, fmt)
    except ValueError:
        return None

def _discover_csv_files() -> list[str]:
    """Return sorted list of CSV filenames in DATA_DIR."""
    if not os.path.isdir(DATA_DIR):
        return []
    return sorted(f for f in os.listdir(DATA_DIR) if f.lower().endswith(".csv"))

class DeviceDataViewer(tk.Tk):
    """Main application window."""
    def __init__(self):
        super().__init__()
        self.title("Device Data Viewer — Statistics")
        self.geometry("1200x800")
        self.minsize(1000, 650)
        self.configure(bg="#f0f0f0")

        self._rows: list[tuple] = []
        self._csv_files: list[str] = []

        self._build_ui()
        self._load_device_list()
        self._on_fetch()

    # ── UI Construction ───────────────────────────────────────────────
    def _build_ui(self):
        # Top toolbar
        toolbar = ttk.Frame(self, padding=10)
        toolbar.pack(fill=tk.X, padx=10, pady=(10, 5))

        self.fetch_btn = ttk.Button(toolbar, text="Load Data", command=self._on_fetch)
        self.fetch_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.export_btn = ttk.Button(
            toolbar, text="Export CSV", command=self._export_csv, state=tk.DISABLED
        )
        self.export_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.upload_btn = ttk.Button(toolbar, text="Upload File", command=self._upload_file)
        self.upload_btn.pack(side=tk.LEFT, padx=(0, 5))

        self.remove_btn = ttk.Button(toolbar, text="Remove File", command=self._remove_file)
        self.remove_btn.pack(side=tk.LEFT, padx=(0, 5))

        # Status bar
        status_frame = ttk.Frame(self)
        status_frame.pack(fill=tk.X, padx=10, pady=(0, 5))
        self.status_var = tk.StringVar(value="Ready — select filters and click Fetch Data")
        ttk.Label(status_frame, textvariable=self.status_var).pack(side=tk.LEFT)
        self.progress = ttk.Progressbar(status_frame, mode="determinate", length=250)
        self.progress.pack(side=tk.RIGHT)

        # ── Main content: tabbed notebook ─────────────────────────────
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        # Tab 1 — Summary statistics
        self.stats_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.stats_frame, text="  Summary Statistics  ")
        self._build_stats_tab()

        # Tab 2 — Distribution histograms
        self.hist_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.hist_frame, text="  Distributions  ")

    def _build_stats_tab(self):
        # Overall stats table
        cols = ("Metric", "SAT", "HGB")
        self.stats_tree = ttk.Treeview(self.stats_frame, columns=cols, show="headings", height=10)
        for c in cols:
            self.stats_tree.heading(c, text=c)
            self.stats_tree.column(c, width=200 if c == "Metric" else 160, anchor=tk.CENTER)
        self.stats_tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Per-device breakdown
        ttk.Label(self.stats_frame, text="Per-Device Breakdown",
                  font=("Segoe UI", 10, "bold")).pack(anchor=tk.W, padx=10)

        dev_cols = ("Device", "Records", "SAT Mean", "HGB Mean")
        dev_container = ttk.Frame(self.stats_frame)
        dev_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self.dev_stats_tree = ttk.Treeview(dev_container, columns=dev_cols, show="headings", height=8)
        for c in dev_cols:
            self.dev_stats_tree.heading(c, text=c)
            self.dev_stats_tree.column(c, width=110, anchor=tk.CENTER)
        dev_vsb = ttk.Scrollbar(dev_container, orient=tk.VERTICAL, command=self.dev_stats_tree.yview)
        self.dev_stats_tree.configure(yscrollcommand=dev_vsb.set)
        self.dev_stats_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        dev_vsb.pack(side=tk.LEFT, fill=tk.Y)

    # ── Load device list ────────────────────────────────────────────────
    def _load_device_list(self):
        self._csv_files = _discover_csv_files()
        self.status_var.set(f"Ready — {len(self._csv_files)} file(s) available")

    # ── Fetch data ────────────────────────────────────────────────────
    def _on_fetch(self):
        self.fetch_btn.config(state=tk.DISABLED)
        self.export_btn.config(state=tk.DISABLED)
        self.progress["value"] = 0
        self.status_var.set("Loading CSV files…")

        threading.Thread(
            target=self._fetch_worker,
            daemon=True,
        ).start()

    def _fetch_worker(self):
        files = self._csv_files
        all_rows: list[tuple] = []
        total = len(files)

        for idx, fname in enumerate(files, 1):
            # Derive a label from the filename (strip extension)
            label = os.path.splitext(fname)[0]
            self.after(0, lambda lb=label, i=idx, t=total, n=len(all_rows):
                       self.status_var.set(
                           f"Loading {lb}… ({i}/{t}) — {n:,} rows so far"))

            csv_path = os.path.join(DATA_DIR, fname)

            try:
                with open(csv_path, "r", encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        raw = row.get("Device Time", "").strip()
                        if not raw:
                            continue
                        dt = None
                        for fmt in DATE_FORMATS:
                            dt = _try_parse(raw, fmt)
                            if dt:
                                break
                        if dt is None:
                            continue
                        sat = float(row.get("Saturation", 0))
                        hgb = float(row.get("tGb", 0))
                        sensor = row.get("Sensor", "")
                        all_rows.append((0, label, dt, dt, sat, hgb, sensor))
            except OSError as exc:
                self.after(0, lambda lb=label, e=str(exc):
                           self._query_error(f"Error reading {lb}: {e}"))
                return

            pct = int(idx / total * 100)
            self.after(0, lambda p=pct: self.progress.configure(value=p))

        # Sort by device_time across all devices, then assign sequential IDs
        all_rows.sort(key=lambda r: r[2])
        all_rows = [(i + 1, r[1], r[2], r[3], r[4], r[5], r[6])
                     for i, r in enumerate(all_rows)]
        self.after(0, lambda: self._display_results(all_rows))

    # ── Display results ───────────────────────────────────────────────
    def _display_results(self, rows: list[tuple]):
        self.progress["value"] = 100
        self.fetch_btn.config(state=tk.NORMAL)

        self._rows = rows
        count = len(rows)
        self.status_var.set(f"Returned {count:,} row{'s' if count != 1 else ''}")

        if not count:
            messagebox.showinfo("No Data", "No records found for the selected filters.")
            return

        self.export_btn.config(state=tk.NORMAL)

        # Parse columns
        device_ids = [r[1] for r in rows]
        sats = [float(r[4]) for r in rows]
        hgbs = [float(r[5]) for r in rows]

        self._update_summary_stats(device_ids, sats, hgbs)
        self._update_histograms(device_ids, sats, hgbs)

    def _query_error(self, msg: str):
        self.progress["value"] = 0
        self.fetch_btn.config(state=tk.NORMAL)
        self.status_var.set("Load failed")
        messagebox.showerror("Data Error", msg)

    # ── Tab 1: Summary Statistics ─────────────────────────────────────
    def _update_summary_stats(self, device_ids, sats, hgbs):
        for item in self.stats_tree.get_children():
            self.stats_tree.delete(item)
        for item in self.dev_stats_tree.get_children():
            self.dev_stats_tree.delete(item)

        def _f(v):
            return f"{v:.4f}"

        rows = [
            ("Count", str(len(sats)), str(len(hgbs))),
            ("Mean", _f(statistics.mean(sats)), _f(statistics.mean(hgbs))),
            ("Min", _f(min(sats)), _f(min(hgbs))),
            ("Max", _f(max(sats)), _f(max(hgbs))),
        ]

        for r in rows:
            self.stats_tree.insert("", tk.END, values=r)

        # Per-device breakdown
        buckets = defaultdict(lambda: {"sat": [], "hgb": []})
        for did, s, h in zip(device_ids, sats, hgbs):
            buckets[did]["sat"].append(s)
            buckets[did]["hgb"].append(h)

        for did in sorted(buckets):
            b = buckets[did]
            n = len(b["sat"])
            self.dev_stats_tree.insert("", tk.END, values=(
                did, n,
                _f(statistics.mean(b["sat"])),
                _f(statistics.mean(b["hgb"])),
            ))

    # ── Tab 2: Distribution histograms ────────────────────────────────
    def _update_histograms(self, device_ids, sats, hgbs):
        for w in self.hist_frame.winfo_children():
            w.destroy()

        # Group data by device
        buckets = defaultdict(lambda: {"sat": [], "hgb": []})
        for did, s, h in zip(device_ids, sats, hgbs):
            buckets[did]["sat"].append(s)
            buckets[did]["hgb"].append(h)

        devices = sorted(buckets)
        n_devices = len(devices)
        if not n_devices:
            return

        fig = Figure(figsize=(11, 3.5 * n_devices), dpi=96)
        fig.subplots_adjust(wspace=0.35, hspace=0.45,
                            left=0.06, right=0.97, top=0.96, bottom=0.04)

        for row_idx, dev in enumerate(devices):
            b = buckets[dev]
            for col_idx, (label, data, color) in enumerate([
                ("SAT", b["sat"], "#1f77b4"),
                ("HGB", b["hgb"], "#ff7f0e"),
            ]):
                ax = fig.add_subplot(n_devices, 2, row_idx * 2 + col_idx + 1)
                n = len(data)

                # Sturges' rule for bin count: ceil(log2(n) + 1)
                bins = max(1, int(math.ceil(math.log2(n) + 1))) if n > 1 else 1

                ax.hist(data, bins=bins, color=color, edgecolor="white", alpha=0.85,
                        density=False)
                ax.set_title(f"{dev} — {label} Distribution (N={n:,})", fontsize=9)
                ax.set_xlabel(label, fontsize=8)
                ax.set_ylabel("Frequency", fontsize=8)
                ax.tick_params(labelsize=7)
                ax.grid(True, alpha=0.3, axis="y")

                m = statistics.mean(data)
                med = statistics.median(data)
                sd = statistics.stdev(data) if n > 1 else 0.0
                ax.axvline(m, color="red", linestyle="--", linewidth=1,
                           label=f"Mean: {m:.4f}")
                ax.axvline(med, color="black", linestyle=":", linewidth=1,
                           label=f"Median: {med:.4f}")
                # Show ±1 SD band
                if sd > 0:
                    ax.axvspan(m - sd, m + sd, alpha=0.1, color="red",
                               label=f"±1 SD: {sd:.4f}")
                ax.legend(fontsize=6)

        # Scrollable canvas to handle many devices
        canvas_widget = tk.Canvas(self.hist_frame)
        scrollbar = ttk.Scrollbar(self.hist_frame, orient=tk.VERTICAL,
                                  command=canvas_widget.yview)
        canvas_widget.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        inner = ttk.Frame(canvas_widget)
        canvas_widget.create_window((0, 0), window=inner, anchor=tk.NW)

        chart = FigureCanvasTkAgg(fig, master=inner)
        chart.draw()
        chart.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        toolbar = NavigationToolbar2Tk(chart, inner)
        toolbar.update()

        inner.update_idletasks()
        canvas_widget.configure(scrollregion=canvas_widget.bbox("all"))
        canvas_widget.bind("<MouseWheel>",
                           lambda e: canvas_widget.yview_scroll(-1 * (e.delta // 120), "units"))

    # ── Upload File ────────────────────────────────────────────────────
    def _upload_file(self):
        paths = filedialog.askopenfilenames(
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Select CSV file(s) to upload",
        )
        if not paths:
            return

        os.makedirs(DATA_DIR, exist_ok=True)
        added = 0
        for src in paths:
            fname = os.path.basename(src)
            error = self._validate_csv(src)
            if error:
                messagebox.showwarning(
                    "Invalid File",
                    f"Data for \"{fname}\" can't be loaded due to incorrect format.\n\n{error}",
                )
                continue
            dest = os.path.join(DATA_DIR, fname)
            shutil.copy2(src, dest)
            added += 1

        if added:
            self._load_device_list()
            self._on_fetch()
            self.status_var.set(f"Uploaded {added} file(s) — data reloaded")

    # ── Remove File ───────────────────────────────────────────────────
    def _remove_file(self):
        files = self._csv_files
        if not files:
            messagebox.showinfo("No Files", "There are no files in the data folder to remove.")
            return

        dialog = tk.Toplevel(self)
        dialog.title("Remove File")
        dialog.geometry("400x350")
        dialog.transient(self)
        dialog.grab_set()

        ttk.Label(dialog, text="Select file(s) to remove:").pack(anchor=tk.W, padx=10, pady=(10, 5))

        listbox = tk.Listbox(dialog, selectmode=tk.EXTENDED)
        for f in files:
            listbox.insert(tk.END, f)
        listbox.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        def do_remove():
            selected = [listbox.get(i) for i in listbox.curselection()]
            if not selected:
                return
            confirm = messagebox.askyesno(
                "Confirm Removal",
                f"Remove {len(selected)} file(s) from the data folder?\n\n"
                + "\n".join(selected),
                parent=dialog,
            )
            if not confirm:
                return
            for fname in selected:
                try:
                    os.remove(os.path.join(DATA_DIR, fname))
                except OSError:
                    pass
            dialog.destroy()
            self._load_device_list()
            self._on_fetch()
            self.status_var.set(f"Removed {len(selected)} file(s) — data reloaded")

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        ttk.Button(btn_frame, text="Remove Selected", command=do_remove).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="Cancel", command=dialog.destroy).pack(side=tk.RIGHT)

    @staticmethod
    def _validate_csv(path: str) -> str | None:
        """Return an error message if the CSV doesn't match expected format, else None."""
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None:
                    return "File is empty or has no header row."
                headers = {h.strip() for h in reader.fieldnames}
                missing = EXPECTED_COLUMNS - headers
                if missing:
                    return f"Missing required columns: {', '.join(sorted(missing))}"

                # Check that at least the first data row can be parsed
                first = next(reader, None)
                if first is None:
                    return "File has a header but no data rows."
                raw_dt = first.get("Device Time", "").strip()
                if not any(_try_parse(raw_dt, fmt) for fmt in DATE_FORMATS):
                    return f"Cannot parse 'Device Time' value: \"{raw_dt}\""
                try:
                    float(first.get("Saturation", ""))
                except (ValueError, TypeError):
                    return f"Cannot parse 'Saturation' as a number."
                try:
                    float(first.get("tGb", ""))
                except (ValueError, TypeError):
                    return f"Cannot parse 'tGb' as a number."
        except OSError as exc:
            return f"Cannot read file: {exc}"
        return None

    # ── CSV Export ────────────────────────────────────────────────────
    def _export_csv(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export Data to CSV",
        )
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([c.upper() for c in COLUMNS])
                writer.writerows(self._rows)
            self.status_var.set(f"Exported {len(self._rows):,} rows to {path}")
        except OSError as exc:
            messagebox.showerror("Export Error", str(exc))

if __name__ == "__main__":
    app = DeviceDataViewer()
    app.mainloop()
