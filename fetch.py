#!/usr/bin/env python3
"""
Threat list fetcher for siberguvenlik.gov.tr

Two modes:
  incremental (default) — fetches only entries newer than the last known ID,
                           prepends them to the existing file.  Fast.
  full (--full)          — re-fetches everything and rewrites the file.
                           Catches any entries USOM has removed.

State (latest ID per type) is stored in state.json and committed to the repo
so it persists across GitHub Actions runs.
"""

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE_URL = "https://siberguvenlik.gov.tr/api/address/index"

TYPES = {
    "ip":     "ip-list.txt",
    "ip6":    "ip6-list.txt",
    "domain": "url-list.txt",
}

STATE_FILE = "state.json"
WORKERS_INCREMENTAL = 20
WORKERS_FULL = 100
RETRY_LIMIT = 5


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
        f.write("\n")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def fetch_page(session: requests.Session, type_: str, page: int) -> dict:
    for attempt in range(RETRY_LIMIT):
        try:
            r = session.get(BASE_URL, params={"type": type_, "page": page}, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            if attempt == RETRY_LIMIT - 1:
                raise
            wait = 2 ** attempt
            print(f"  [warn] {type_} page {page} attempt {attempt + 1} failed ({exc}), retry in {wait}s")
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Fetch strategies
# ---------------------------------------------------------------------------

def fetch_full(type_: str) -> tuple[int, list[str]]:
    """Fetch every page concurrently. Returns (latest_id, entries_newest_first)."""
    with requests.Session() as session:
        first = fetch_page(session, type_, 1)
        page_count = first["pageCount"]
        total = first["totalCount"]
        latest_id = first["models"][0]["id"] if first["models"] else 0
        entries: list[str] = [m["url"] for m in first["models"]]

        print(f"  [{type_}] total={total}, pages={page_count}")

        if page_count <= 1:
            return latest_id, entries

        def load(page):
            data = fetch_page(session, type_, page)
            return page, [m["url"] for m in data["models"]]

        with ThreadPoolExecutor(max_workers=WORKERS_FULL) as pool:
            futures = {pool.submit(load, p): p for p in range(2, page_count + 1)}
            pages: dict[int, list[str]] = {}
            done = 0
            for fut in as_completed(futures):
                p, urls = fut.result()
                pages[p] = urls
                done += 1
                if done % 500 == 0:
                    print(f"  [{type_}] {done}/{page_count - 1} pages done")

        for p in range(2, page_count + 1):
            entries.extend(pages[p])

    return latest_id, entries


def fetch_incremental(type_: str, known_id: int) -> tuple[int, list[str]]:
    """
    Walk pages newest-first and stop as soon as we hit an entry whose ID is
    already known.  Returns (new_latest_id, new_entries_newest_first).
    """
    new_entries: list[str] = []
    new_latest_id = known_id

    with requests.Session() as session:
        page = 1
        while True:
            data = fetch_page(session, type_, page)
            models = data.get("models", [])
            if not models:
                break

            if page == 1:
                new_latest_id = models[0]["id"]
                if new_latest_id <= known_id:
                    break  # nothing new at all

            reached_known = False
            for m in models:
                if m["id"] > known_id:
                    new_entries.append(m["url"])
                else:
                    reached_known = True
                    break  # rest of this page (and all later pages) are old

            if reached_known:
                break
            page += 1

    return new_latest_id, new_entries


# ---------------------------------------------------------------------------
# Run modes
# ---------------------------------------------------------------------------

def run_full(state: dict):
    for type_, filename in TYPES.items():
        print(f"[full] {type_} -> {filename}")
        t0 = time.time()
        latest_id, entries = fetch_full(type_)
        with open(filename, "w", encoding="utf-8") as f:
            for entry in entries:
                f.write(entry + "\n")
        state[type_] = latest_id
        print(f"  -> {len(entries)} entries, latest_id={latest_id} ({time.time() - t0:.1f}s)")


def run_incremental(state: dict):
    for type_, filename in TYPES.items():
        known_id = state.get(type_, 0)

        if known_id == 0 or not os.path.exists(filename):
            print(f"[incremental] {type_}: no prior state — running full fetch")
            t0 = time.time()
            latest_id, entries = fetch_full(type_)
            with open(filename, "w", encoding="utf-8") as f:
                for entry in entries:
                    f.write(entry + "\n")
            state[type_] = latest_id
            print(f"  -> {len(entries)} entries ({time.time() - t0:.1f}s)")
            continue

        print(f"[incremental] {type_} (known_id={known_id}) -> {filename}")
        t0 = time.time()
        new_latest_id, new_entries = fetch_incremental(type_, known_id)

        if not new_entries:
            print(f"  -> no new entries")
            continue

        # Prepend new entries to the existing file
        with open(filename, "r", encoding="utf-8") as f:
            existing = f.read()
        with open(filename, "w", encoding="utf-8") as f:
            for entry in new_entries:
                f.write(entry + "\n")
            f.write(existing)

        state[type_] = new_latest_id
        print(f"  -> +{len(new_entries)} new entries, latest_id={new_latest_id} ({time.time() - t0:.1f}s)")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--full", action="store_true", help="Full regeneration (syncs removals)")
    args = parser.parse_args()

    state = load_state()

    if args.full:
        run_full(state)
    else:
        run_incremental(state)

    save_state(state)
    print("Done.")


if __name__ == "__main__":
    main()
