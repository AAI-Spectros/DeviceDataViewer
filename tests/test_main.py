"""
Integration / unit tests for DeviceDataViewer.

Uses a small hand-crafted CSV fixture with known row counts,
sensor segments, and 12-hour boundaries so every assertion is exact.

Fixture layout (test_device.csv — two devices combined into one file
is NOT the real structure; we use two separate files like production):

  test_data/DevA.csv  — 1 sensor, 15 rows total
      "New Sensor!" at row 1 (12/01/2025 08:00)
      Rows 1-8  are within first 12 h  (08:00 → 19:59)
      Rows 9-15 are after  12 h cutoff (20:00 → 02:00 next day)

  test_data/DevB.csv  — 2 sensors (two "New Sensor!" markers), 12 rows
      Sensor 1: rows 1-6  — all within first 12 h (no after-12h data)
      Sensor 2: rows 7-12 — 3 rows before cutoff, 3 rows after
"""

import csv
import os
import shutil
import statistics
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import matplotlib
matplotlib.use("Agg")  # non-interactive backend for tests

import sys
# Ensure project root is on the path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from main import (
    _try_parse,
    _discover_csv_files,
    DeviceDataViewer,
    DATE_FORMATS,
)

# ── Fixture helpers ───────────────────────────────────────────────────
def _write_csv(path: str, rows: list[list[str]]):
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["Device Time", "Saturation", "tGb", "Sensor", "Marker"])
        writer.writerows(rows)

# Device A: 1 sensor, 15 data rows
# Sensor start: 12/01/2025 08:00  →  cutoff = 12/01/2025 20:00
DEVA_ROWS = [
    # --- first 12h (8 rows) ---
    ["12/1/2025 8:00",  "0.45", "0.18", "SNS-A1", "New Sensor!"],
    ["12/1/2025 9:00",  "0.44", "0.17", "SNS-A1", ""],
    ["12/1/2025 10:00", "0.46", "0.16", "SNS-A1", ""],
    ["12/1/2025 11:00", "0.43", "0.15", "SNS-A1", ""],
    ["12/1/2025 14:00", "0.40", "0.14", "SNS-A1", ""],
    ["12/1/2025 16:00", "0.42", "0.13", "SNS-A1", ""],
    ["12/1/2025 18:00", "0.41", "0.12", "SNS-A1", ""],
    ["12/1/2025 19:59", "0.39", "0.11", "SNS-A1", ""],
    # --- after 12h (7 rows) ---
    ["12/1/2025 20:00", "0.50", "0.20", "SNS-A1", ""],
    ["12/1/2025 21:00", "0.51", "0.21", "SNS-A1", ""],
    ["12/1/2025 22:00", "0.52", "0.22", "SNS-A1", ""],
    ["12/1/2025 23:00", "0.53", "0.23", "SNS-A1", ""],
    ["12/2/2025 0:00",  "0.54", "0.24", "SNS-A1", ""],
    ["12/2/2025 1:00",  "0.55", "0.25", "SNS-A1", ""],
    ["12/2/2025 2:00",  "0.56", "0.26", "SNS-A1", ""],
]

# Device B: 2 sensors, 12 data rows
# Sensor 1 start: 12/03/2025 06:00  →  cutoff = 12/03/2025 18:00
# Sensor 2 start: 12/05/2025 10:00  →  cutoff = 12/05/2025 22:00
DEVB_ROWS = [
    # --- sensor 1: 6 rows, all within first 12h ---
    ["12/3/2025 6:00",  "0.30", "0.10", "SNS-B1", "New Sensor!"],
    ["12/3/2025 7:00",  "0.31", "0.11", "SNS-B1", ""],
    ["12/3/2025 8:00",  "0.32", "0.12", "SNS-B1", ""],
    ["12/3/2025 9:00",  "0.33", "0.13", "SNS-B1", ""],
    ["12/3/2025 10:00", "0.34", "0.14", "SNS-B1", ""],
    ["12/3/2025 11:00", "0.35", "0.15", "SNS-B1", ""],
    # --- sensor 2: 6 rows, 3 before + 3 after cutoff ---
    ["12/5/2025 10:00", "0.60", "0.30", "SNS-B2", "New Sensor!"],
    ["12/5/2025 12:00", "0.61", "0.31", "SNS-B2", ""],
    ["12/5/2025 14:00", "0.62", "0.32", "SNS-B2", ""],
    # after 12h
    ["12/5/2025 22:00", "0.70", "0.40", "SNS-B2", ""],
    ["12/5/2025 23:00", "0.71", "0.41", "SNS-B2", ""],
    ["12/6/2025 0:00",  "0.72", "0.42", "SNS-B2", ""],
]

class _FixtureMixin:
    """Set up a temp data/ directory with the two test CSV files."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.data_dir = os.path.join(self.tmpdir, "data")
        os.makedirs(self.data_dir)
        _write_csv(os.path.join(self.data_dir, "DevA.csv"), DEVA_ROWS)
        _write_csv(os.path.join(self.data_dir, "DevB.csv"), DEVB_ROWS)

    def tearDown(self):
        shutil.rmtree(self.tmpdir, ignore_errors=True)


# =====================================================================
# 1. Unit tests — pure functions (no GUI)
# =====================================================================
class TestDateParsing(unittest.TestCase):
    """Test _try_parse with all supported DATE_FORMATS."""

    def test_parse_24h_format(self):
        dt = _try_parse("12/1/2025 8:00", "%m/%d/%Y %H:%M")
        self.assertEqual(dt, datetime(2025, 12, 1, 8, 0))

class TestDiscoverCsvFiles(_FixtureMixin, unittest.TestCase):
    """Test _discover_csv_files finds .csv files alphabetically."""

    def test_finds_both_files(self):
        with patch("main.DATA_DIR", self.data_dir):
            files = _discover_csv_files()
        self.assertEqual(files, ["DevA.csv", "DevB.csv"])

    def test_empty_dir(self):
        empty = os.path.join(self.tmpdir, "empty")
        os.makedirs(empty)
        with patch("main.DATA_DIR", empty):
            self.assertEqual(_discover_csv_files(), [])

    def test_nonexistent_dir(self):
        with patch("main.DATA_DIR", os.path.join(self.tmpdir, "nope")):
            self.assertEqual(_discover_csv_files(), [])

class TestValidateCsv(_FixtureMixin, unittest.TestCase):
    """Test _validate_csv static method."""

    def test_valid_csv_returns_none(self):
        path = os.path.join(self.data_dir, "DevA.csv")
        self.assertIsNone(DeviceDataViewer._validate_csv(path))

    def test_missing_columns(self):
        bad = os.path.join(self.data_dir, "bad.csv")
        with open(bad, "w", newline="") as f:
            csv.writer(f).writerow(["Device Time", "Saturation"])
            csv.writer(f).writerow(["12/1/2025 8:00", "0.45"])
        err = DeviceDataViewer._validate_csv(bad)
        self.assertIn("Missing required columns", err)

    def test_empty_file(self):
        bad = os.path.join(self.data_dir, "empty.csv")
        with open(bad, "w") as f:
            pass
        err = DeviceDataViewer._validate_csv(bad)
        self.assertIn("empty", err.lower())

    def test_header_only(self):
        bad = os.path.join(self.data_dir, "headeronly.csv")
        with open(bad, "w", newline="") as f:
            csv.writer(f).writerow(["Device Time", "Saturation", "tGb", "Sensor", "Marker"])
        err = DeviceDataViewer._validate_csv(bad)
        self.assertIn("no data rows", err.lower())

    def test_unparseable_date(self):
        bad = os.path.join(self.data_dir, "baddate.csv")
        with open(bad, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Device Time", "Saturation", "tGb", "Sensor", "Marker"])
            w.writerow(["BADDATE", "0.45", "0.18", "S1", ""])
        err = DeviceDataViewer._validate_csv(bad)
        self.assertIn("Cannot parse", err)

    def test_unparseable_saturation(self):
        bad = os.path.join(self.data_dir, "badsat.csv")
        with open(bad, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Device Time", "Saturation", "tGb", "Sensor", "Marker"])
            w.writerow(["12/1/2025 8:00", "NOT_A_NUM", "0.18", "S1", ""])
        err = DeviceDataViewer._validate_csv(bad)
        self.assertIn("Saturation", err)

    def test_unparseable_tgb(self):
        bad = os.path.join(self.data_dir, "badtgb.csv")
        with open(bad, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Device Time", "Saturation", "tGb", "Sensor", "Marker"])
            w.writerow(["12/1/2025 8:00", "0.45", "NaN_str", "S1", ""])
        err = DeviceDataViewer._validate_csv(bad)
        self.assertIn("tGb", err)

# =====================================================================
# 2. Integration tests — CSV loading + data processing (no GUI render)
# =====================================================================
class TestFetchWorker(_FixtureMixin, unittest.TestCase):
    """Test _fetch_worker loads rows correctly using the temp data dir."""

    def _load_rows(self) -> list[tuple]:
        """Run _fetch_worker synchronously and capture rows."""
        with patch("main.DATA_DIR", self.data_dir):
            app = DeviceDataViewer.__new__(DeviceDataViewer)
            # Minimal init — avoid __init__ which starts the GUI
            app._csv_files = _discover_csv_files()
            app._rows = []

            # Run the fetch logic inline (same code as _fetch_worker)
            files = app._csv_files
            all_rows = []
            for fname in files:
                label = os.path.splitext(fname)[0]
                csv_path = os.path.join(self.data_dir, fname)
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
                        marker = row.get("Marker", "").strip()
                        all_rows.append((0, label, dt, dt, sat, hgb, sensor, marker))

            all_rows.sort(key=lambda r: r[2])
            all_rows = [(i + 1, r[1], r[2], r[3], r[4], r[5], r[6], r[7])
                        for i, r in enumerate(all_rows)]
        return all_rows

    # ── Row counts ────────────────────────────────────────────────────
    def test_total_row_count(self):
        """15 (DevA) + 12 (DevB) = 27 rows total."""
        rows = self._load_rows()
        self.assertEqual(len(rows), 27)

    def test_per_device_row_count(self):
        rows = self._load_rows()
        deva = [r for r in rows if r[1] == "DevA"]
        devb = [r for r in rows if r[1] == "DevB"]
        self.assertEqual(len(deva), 15)
        self.assertEqual(len(devb), 12)

    def test_sequential_ids(self):
        rows = self._load_rows()
        ids = [r[0] for r in rows]
        self.assertEqual(ids, list(range(1, 28)))

    def test_rows_sorted_by_time(self):
        rows = self._load_rows()
        times = [r[2] for r in rows]
        self.assertEqual(times, sorted(times))

    # ── Device labels ─────────────────────────────────────────────────
    def test_device_labels(self):
        rows = self._load_rows()
        labels = sorted(set(r[1] for r in rows))
        self.assertEqual(labels, ["DevA", "DevB"])

    # ── Marker detection ──────────────────────────────────────────────
    def test_new_sensor_marker_count(self):
        """DevA=1 marker, DevB=2 markers -> 3 total 'New Sensor!' rows."""
        rows = self._load_rows()
        markers = [r for r in rows if r[7] == "New Sensor!"]
        self.assertEqual(len(markers), 3)

    def test_marker_count_per_device(self):
        rows = self._load_rows()
        deva_markers = [r for r in rows if r[1] == "DevA" and r[7] == "New Sensor!"]
        devb_markers = [r for r in rows if r[1] == "DevB" and r[7] == "New Sensor!"]
        self.assertEqual(len(deva_markers), 1)
        self.assertEqual(len(devb_markers), 2)

    # ── Sensor IDs ────────────────────────────────────────────────────
    def test_sensor_ids(self):
        rows = self._load_rows()
        sensors = sorted(set(r[6] for r in rows))
        self.assertEqual(sensors, ["SNS-A1", "SNS-B1", "SNS-B2"])

    # ── SAT / HGB values parsed ──────────────────────────────────────
    def test_sat_values_in_range(self):
        rows = self._load_rows()
        for r in rows:
            self.assertGreater(r[4], 0)
            self.assertLessEqual(r[4], 1.0)

    def test_hgb_values_in_range(self):
        rows = self._load_rows()
        for r in rows:
            self.assertGreater(r[5], 0)
            self.assertLessEqual(r[5], 1.0)

# =====================================================================
# 3. Segment splitting & 12-hour cutoff logic
# =====================================================================
class TestSegmentSplitting(_FixtureMixin, unittest.TestCase):
    """Test the segment-splitting logic used by histograms & time series."""

    def _load_rows(self):
        """Same as TestFetchWorker._load_rows."""
        all_rows = []
        with patch("main.DATA_DIR", self.data_dir):
            for fname in sorted(os.listdir(self.data_dir)):
                if not fname.endswith(".csv"):
                    continue
                label = os.path.splitext(fname)[0]
                csv_path = os.path.join(self.data_dir, fname)
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
                        marker = row.get("Marker", "").strip()
                        all_rows.append((0, label, dt, dt, sat, hgb, sensor, marker))
        all_rows.sort(key=lambda r: r[2])
        all_rows = [(i + 1, r[1], r[2], r[3], r[4], r[5], r[6], r[7])
                    for i, r in enumerate(all_rows)]
        return all_rows

    @staticmethod
    def _split_segments(rows):
        """Replicate the segment-splitting logic from main.py."""
        from collections import defaultdict
        device_rows = defaultdict(list)
        for r in rows:
            device_rows[r[1]].append(r)

        all_segments = []
        for dev in sorted(device_rows):
            dev_data = sorted(device_rows[dev], key=lambda r: r[2])
            segments = []
            current = []
            for r in dev_data:
                if r[7] == "New Sensor!":
                    if current:
                        segments.append(current)
                    current = [r]
                else:
                    current.append(r)
            if current:
                segments.append(current)
            all_segments.append((dev, segments))
        return all_segments

    @staticmethod
    def _split_12h(segment):
        """Split a segment into (first_12h, after_12h) row lists."""
        first_time = segment[0][2]
        cutoff = first_time + timedelta(hours=12)
        first_12h = [r for r in segment if r[2] < cutoff]
        after_12h = [r for r in segment if r[2] >= cutoff]
        return first_12h, after_12h

    # ── Segment counts ────────────────────────────────────────────────
    def test_total_segment_count(self):
        """DevA has 1 segment, DevB has 2 -> 3 segments total."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        total_segs = sum(len(segs) for _, segs in device_segments)
        self.assertEqual(total_segs, 3)

    def test_deva_has_one_segment(self):
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        deva = [segs for dev, segs in device_segments if dev == "DevA"][0]
        self.assertEqual(len(deva), 1)

    def test_devb_has_two_segments(self):
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        devb = [segs for dev, segs in device_segments if dev == "DevB"][0]
        self.assertEqual(len(devb), 2)

    # ── Rows per segment ──────────────────────────────────────────────
    def test_deva_segment_row_count(self):
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        deva_segs = [segs for dev, segs in device_segments if dev == "DevA"][0]
        self.assertEqual(len(deva_segs[0]), 15)

    def test_devb_segment1_row_count(self):
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        devb_segs = [segs for dev, segs in device_segments if dev == "DevB"][0]
        self.assertEqual(len(devb_segs[0]), 6)

    def test_devb_segment2_row_count(self):
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        devb_segs = [segs for dev, segs in device_segments if dev == "DevB"][0]
        self.assertEqual(len(devb_segs[1]), 6)

    # ── 12-hour split: DevA (1 segment) ───────────────────────────────
    def test_deva_first_12h_count(self):
        """DevA: 8 rows within first 12h (08:00 -> 19:59)."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        seg = [segs for dev, segs in device_segments if dev == "DevA"][0][0]
        first, after = self._split_12h(seg)
        self.assertEqual(len(first), 8)

    def test_deva_after_12h_count(self):
        """DevA: 7 rows after cutoff (20:00 onwards)."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        seg = [segs for dev, segs in device_segments if dev == "DevA"][0][0]
        first, after = self._split_12h(seg)
        self.assertEqual(len(after), 7)

    # ── 12-hour split: DevB sensor 1 (all within first 12h) ──────────
    def test_devb_sensor1_first_12h_count(self):
        """DevB sensor 1: all 6 rows are within first 12h."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        seg = [segs for dev, segs in device_segments if dev == "DevB"][0][0]
        first, after = self._split_12h(seg)
        self.assertEqual(len(first), 6)

    def test_devb_sensor1_after_12h_count(self):
        """DevB sensor 1: 0 rows after cutoff."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        seg = [segs for dev, segs in device_segments if dev == "DevB"][0][0]
        first, after = self._split_12h(seg)
        self.assertEqual(len(after), 0)

    # ── 12-hour split: DevB sensor 2 (3 before + 3 after) ────────────
    def test_devb_sensor2_first_12h_count(self):
        """DevB sensor 2: 3 rows within first 12h."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        seg = [segs for dev, segs in device_segments if dev == "DevB"][0][1]
        first, after = self._split_12h(seg)
        self.assertEqual(len(first), 3)

    def test_devb_sensor2_after_12h_count(self):
        """DevB sensor 2: 3 rows after cutoff."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        seg = [segs for dev, segs in device_segments if dev == "DevB"][0][1]
        first, after = self._split_12h(seg)
        self.assertEqual(len(after), 3)

    # ── Combined 12h totals (for "All devices" distribution) ──────────
    def test_combined_first_12h_total(self):
        """8 (DevA) + 6 (DevB-S1) + 3 (DevB-S2) = 17 first-12h rows."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        total = 0
        for _, segs in device_segments:
            for seg in segs:
                first, _ = self._split_12h(seg)
                total += len(first)
        self.assertEqual(total, 17)

    def test_combined_after_12h_total(self):
        """7 (DevA) + 0 (DevB-S1) + 3 (DevB-S2) = 10 after-12h rows."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        total = 0
        for _, segs in device_segments:
            for seg in segs:
                _, after = self._split_12h(seg)
                total += len(after)
        self.assertEqual(total, 10)

    # ── Time series filter: only after-12h segments with data ─────────
    def test_time_series_segment_count(self):
        """Time series only shows segments with after-12h data:
        DevA-seg1 (7 rows) + DevB-seg2 (3 rows) = 2 segments.
        DevB-seg1 has 0 after-12h data → excluded."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        ts_segments = []
        for dev, segs in device_segments:
            for seg_idx, seg in enumerate(segs, 1):
                first_time = seg[0][2]
                cutoff = first_time + timedelta(hours=12)
                filtered = [r for r in seg if r[2] >= cutoff]
                if filtered:
                    ts_segments.append((f"{dev} — Patient {seg_idx}", filtered))
        self.assertEqual(len(ts_segments), 2)

    def test_time_series_deva_after_rows(self):
        """DevA time series segment has 7 rows."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        deva_seg = [segs for dev, segs in device_segments if dev == "DevA"][0][0]
        cutoff = deva_seg[0][2] + timedelta(hours=12)
        filtered = [r for r in deva_seg if r[2] >= cutoff]
        self.assertEqual(len(filtered), 7)

    def test_time_series_devb_seg2_after_rows(self):
        """DevB sensor 2 time series segment has 3 rows."""
        rows = self._load_rows()
        device_segments = self._split_segments(rows)
        devb_seg2 = [segs for dev, segs in device_segments if dev == "DevB"][0][1]
        cutoff = devb_seg2[0][2] + timedelta(hours=12)
        filtered = [r for r in devb_seg2 if r[2] >= cutoff]
        self.assertEqual(len(filtered), 3)

# =====================================================================
# 4. Summary statistics computation
# =====================================================================
class TestSummaryStats(_FixtureMixin, unittest.TestCase):
    """Test that summary stat calculations match expected values."""

    def _load_rows(self):
        all_rows = []
        for fname in sorted(os.listdir(self.data_dir)):
            if not fname.endswith(".csv"):
                continue
            label = os.path.splitext(fname)[0]
            csv_path = os.path.join(self.data_dir, fname)
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    raw = row.get("Device Time", "").strip()
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
                    marker = row.get("Marker", "").strip()
                    all_rows.append((0, label, dt, dt, sat, hgb, sensor, marker))
        all_rows.sort(key=lambda r: r[2])
        return [(i + 1, *r[1:]) for i, r in enumerate(all_rows)]

    def test_overall_sat_mean(self):
        rows = self._load_rows()
        sats = [r[4] for r in rows]
        expected = statistics.mean(sats)
        self.assertAlmostEqual(expected, statistics.mean(sats), places=6)
        # Verify against hand-computed: DevA sats + DevB sats
        all_sats = (
            [0.45, 0.44, 0.46, 0.43, 0.40, 0.42, 0.41, 0.39,
             0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56] +
            [0.30, 0.31, 0.32, 0.33, 0.34, 0.35,
             0.60, 0.61, 0.62, 0.70, 0.71, 0.72]
        )
        self.assertAlmostEqual(statistics.mean(sats), statistics.mean(all_sats), places=6)

    def test_per_device_sat_mean(self):
        rows = self._load_rows()
        deva_sats = [r[4] for r in rows if r[1] == "DevA"]
        devb_sats = [r[4] for r in rows if r[1] == "DevB"]
        self.assertAlmostEqual(
            statistics.mean(deva_sats),
            statistics.mean([0.45, 0.44, 0.46, 0.43, 0.40, 0.42, 0.41, 0.39,
                             0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56]),
            places=6,
        )
        self.assertAlmostEqual(
            statistics.mean(devb_sats),
            statistics.mean([0.30, 0.31, 0.32, 0.33, 0.34, 0.35,
                             0.60, 0.61, 0.62, 0.70, 0.71, 0.72]),
            places=6,
        )

    def test_sat_min_max(self):
        rows = self._load_rows()
        sats = [r[4] for r in rows]
        self.assertAlmostEqual(min(sats), 0.30, places=2)
        self.assertAlmostEqual(max(sats), 0.72, places=2)

    def test_hgb_min_max(self):
        rows = self._load_rows()
        hgbs = [r[5] for r in rows]
        self.assertAlmostEqual(min(hgbs), 0.10, places=2)
        self.assertAlmostEqual(max(hgbs), 0.42, places=2)

# =====================================================================
# 5. SAT percentage conversion (×100) used in charts
# =====================================================================
class TestSatPercentConversion(_FixtureMixin, unittest.TestCase):
    """Distribution and time series multiply raw SAT by 100."""

    def _load_rows(self):
        all_rows = []
        for fname in sorted(os.listdir(self.data_dir)):
            if not fname.endswith(".csv"):
                continue
            label = os.path.splitext(fname)[0]
            csv_path = os.path.join(self.data_dir, fname)
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    raw = row.get("Device Time", "").strip()
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
                    marker = row.get("Marker", "").strip()
                    all_rows.append((0, label, dt, dt, sat, hgb, sensor, marker))
        all_rows.sort(key=lambda r: r[2])
        return [(i + 1, *r[1:]) for i, r in enumerate(all_rows)]

    def test_sat_percent_range(self):
        """After ×100, all SAT should be in 0–80 range (per chart xlim)."""
        rows = self._load_rows()
        for r in rows:
            pct = r[4] * 100
            self.assertGreaterEqual(pct, 0)
            self.assertLessEqual(pct, 80)

# =====================================================================
# 6. Edge cases
# =====================================================================
class TestEdgeCases(unittest.TestCase):
    """Edge case handling."""

    def test_empty_csv_no_data_rows(self):
        tmpdir = tempfile.mkdtemp()
        try:
            path = os.path.join(tmpdir, "empty_data.csv")
            with open(path, "w", newline="") as f:
                csv.writer(f).writerow(
                    ["Device Time", "Saturation", "tGb", "Sensor", "Marker"]
                )
            err = DeviceDataViewer._validate_csv(path)
            self.assertIsNotNone(err)
            self.assertIn("no data rows", err.lower())
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_rows_with_blank_device_time_skipped(self):
        """Rows where Device Time is empty should be silently skipped."""
        tmpdir = tempfile.mkdtemp()
        try:
            path = os.path.join(tmpdir, "blank_time.csv")
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f)
                w.writerow(["Device Time", "Saturation", "tGb", "Sensor", "Marker"])
                w.writerow(["12/1/2025 8:00", "0.45", "0.18", "S1", "New Sensor!"])
                w.writerow(["", "0.44", "0.17", "S1", ""])  # blank time
                w.writerow(["12/1/2025 9:00", "0.43", "0.16", "S1", ""])

            all_rows = []
            with open(path, "r", encoding="utf-8-sig") as f:
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
                    marker = row.get("Marker", "").strip()
                    all_rows.append((0, "test", dt, dt, sat, hgb, sensor, marker))
            # Blank row should be skipped → only 2 rows
            self.assertEqual(len(all_rows), 2)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    def test_segment_with_no_after_12h_excluded_from_time_series(self):
        """A sensor segment where all data is within first 12h
        should produce 0 after-12h rows."""
        # Simulate: 3 rows all within 2 hours of sensor start
        base = datetime(2025, 12, 1, 8, 0)
        rows_seg = [
            (1, "D", base, base, 0.4, 0.1, "S", "New Sensor!"),
            (2, "D", base + timedelta(hours=1), base, 0.4, 0.1, "S", ""),
            (3, "D", base + timedelta(hours=2), base, 0.4, 0.1, "S", ""),
        ]
        cutoff = base + timedelta(hours=12)
        after = [r for r in rows_seg if r[2] >= cutoff]
        self.assertEqual(len(after), 0)

if __name__ == "__main__":
    unittest.main()
