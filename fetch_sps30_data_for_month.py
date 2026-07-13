#!/usr/bin/env python3
"""
Download all SPS30 sensor data for a given month from archive.sensor.community
Usage: python3 fetch_sps30_month.py 2021-01 [--workers 8]
"""

import argparse
import datetime
import gzip
import io
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL    = "https://archive.sensor.community"
MAX_RETRIES = 3
RETRY_DELAY = 2   # seconds between retries
CHUNK_SIZE  = 1 << 16  # 64 KB


# ── Helpers ───────────────────────────────────────────────────────────────────

def days_in_month(month: str):
    """Yield ISO date strings for every day in YYYY-MM."""
    year, mon = map(int, month.split("-"))
    d = datetime.date(year, mon, 1)
    while d.month == mon:
        yield d.isoformat()
        d += datetime.timedelta(days=1)


def fetch_sps30_links(day: str, session: requests.Session) -> list[str]:
    """Return list of full URLs for sps30 gz files on a given day."""
    url = f"{BASE_URL}/{day[:4]}/{day}/"
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            return [
                url + a["href"]
                for a in soup.find_all("a", href=True)
                if "sps30" in a["href"] and a["href"].endswith(".csv.gz")
            ]
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"  [WARN] Could not fetch index for {day}: {e}")
                return []


def download_file(url: str, dest: Path, session: requests.Session) -> bool:
    """Download a single gz file to dest. Skip if already exists. Returns success."""
    if dest.exists():
        return True  # already downloaded in a previous (interrupted) run

    tmp = dest.with_suffix(".tmp")
    for attempt in range(MAX_RETRIES):
        try:
            with session.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for chunk in r.iter_content(CHUNK_SIZE):
                        f.write(chunk)
            tmp.rename(dest)
            return True
        except Exception as e:
            tmp.unlink(missing_ok=True)
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY)
            else:
                print(f"  [FAIL] {url}: {e}")
                return False


def merge_and_sort(raw_dir: Path, out_file: Path):
    """Read all gz files into memory, sort by timestamp (col 5), write CSV."""
    print("\n=== Loading files into memory ===")
    header = None
    rows = []

    gz_files = sorted(raw_dir.glob("*sps30*.csv.gz"))
    for gz in gz_files:
        try:
            with gzip.open(gz, "rt", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
            if not lines:
                continue
            if header is None:
                header = lines[0]
            rows.extend(lines[1:])
        except Exception as e:
            print(f"  [WARN] Could not read {gz.name}: {e}")

    print(f"Loaded {len(rows):,} rows from {len(gz_files)} files — sorting...")

    # Sort by timestamp column (index 5, format: 2021-01-01T00:00:00)
    rows.sort(key=lambda line: line.split(";")[5] if ";" in line else line.split(",")[5])

    print(f"Writing to {out_file} ...")
    with open(out_file, "w", encoding="utf-8") as f:
        if header:
            f.write(header)
        f.writelines(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("month", help="Month in YYYY-MM format, e.g. 2021-01")
    parser.add_argument("--workers", type=int, default=8, help="Parallel download threads")
    args = parser.parse_args()

    month   = args.month
    raw_dir = Path(f"data/{month}/raw")
    out_file = Path(f"data/sps30_{month}.csv")
    raw_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers["User-Agent"] = "sps30-downloader/1.0"

    # ── Step 1: collect all download URLs ────────────────────────────────────
    print(f"=== Collecting SPS30 file links for {month} ===")
    all_urls = []
    for day in days_in_month(month):
        links = fetch_sps30_links(day, session)
        print(f"  {day}: {len(links)} files")
        all_urls.extend(links)

    print(f"\nTotal files to download: {len(all_urls)}")
    if not all_urls:
        print("Nothing to download. Exiting.")
        sys.exit(1)

    # ── Step 2: parallel download ─────────────────────────────────────────────
    print(f"\n=== Downloading with {args.workers} workers ===")
    ok = fail = skip = 0

    def download_task(url):
        fname = url.split("/")[-1]
        dest  = raw_dir / fname
        if dest.exists():
            return "skip"
        return "ok" if download_file(url, dest, session) else "fail"

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(download_task, url): url for url in all_urls}
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            if result == "ok":   ok   += 1
            elif result == "fail": fail += 1
            else:                skip += 1
            if i % 50 == 0 or i == len(all_urls):
                print(f"  {i}/{len(all_urls)} — ok:{ok} skip:{skip} fail:{fail}")

    print(f"\nDownload complete. ok={ok} skipped={skip} failed={fail}")

    # ── Step 3: merge + sort ─────────────────────────────────────────────────
    merge_and_sort(raw_dir, out_file)

    print("\n=== Summary ===")
    print(f"Output     : {out_file}")
    print(f"Size       : {out_file.stat().st_size / 1e6:.1f} MB")
    print(f"Rows       : {sum(1 for _ in open(out_file)):,}")


if __name__ == "__main__":
    main()