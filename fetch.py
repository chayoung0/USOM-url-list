#!/usr/bin/env python3
"""
Fetches all entries from the siberguvenlik.gov.tr API and writes one txt file
per type.  Each file starts with comment lines showing the latest entry ID
(so any update to the list is immediately visible in a diff).
"""

import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

BASE_URL = "https://siberguvenlik.gov.tr/api/address/index"
TYPES = ["ip", "domain", "url"]
WORKERS = 20
RETRY_LIMIT = 5
RETRY_BACKOFF = 2  # seconds


def fetch_page(session: requests.Session, type_: str, page: int) -> dict:
    for attempt in range(RETRY_LIMIT):
        try:
            r = session.get(BASE_URL, params={"type": type_, "page": page}, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == RETRY_LIMIT - 1:
                raise
            wait = RETRY_BACKOFF * (2 ** attempt)
            print(f"  [warn] page {page} attempt {attempt+1} failed ({exc}), retrying in {wait}s")
            time.sleep(wait)


def fetch_type(type_: str) -> tuple[int, int, list[str]]:
    """Return (latest_id, total_count, [url, ...]) for the given type."""
    with requests.Session() as session:
        first = fetch_page(session, type_, 1)
        total = first["totalCount"]
        page_count = first["pageCount"]
        latest_id = first["models"][0]["id"] if first["models"] else 0
        entries: list[str] = [m["url"] for m in first["models"]]

        if page_count <= 1:
            return latest_id, total, entries

        def load(page):
            data = fetch_page(session, type_, page)
            return page, [m["url"] for m in data["models"]]

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(load, p): p for p in range(2, page_count + 1)}
            pages: dict[int, list[str]] = {}
            done = 0
            total_pages = page_count
            for fut in as_completed(futures):
                p, urls = fut.result()
                pages[p] = urls
                done += 1
                if done % 200 == 0 or done == total_pages - 1:
                    print(f"  [{type_}] {done}/{total_pages - 1} pages fetched")

        for p in range(2, page_count + 1):
            entries.extend(pages[p])

    return latest_id, total, entries


def write_list(type_: str, latest_id: int, total: int, entries: list[str]) -> str:
    filename = f"{type_}-list.txt"
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"# latest_id: {latest_id}\n")
        f.write(f"# total: {total}\n")
        f.write(f"# generated: {generated}\n")
        for entry in entries:
            f.write(entry + "\n")
    return filename


def main():
    for type_ in TYPES:
        print(f"Fetching {type_}...")
        t0 = time.time()
        latest_id, total, entries = fetch_type(type_)
        elapsed = time.time() - t0
        filename = write_list(type_, latest_id, total, entries)
        print(f"  -> {filename}: {len(entries)} entries, latest_id={latest_id} ({elapsed:.1f}s)")
    print("Done.")


if __name__ == "__main__":
    main()
