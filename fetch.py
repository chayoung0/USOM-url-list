#!/usr/bin/env python3
"""
Incrementally updates url-list.txt with new entries from siberguvenlik.gov.tr.

All four types (ip, ip6, domain, url) are combined into a single file.
New entries are prepended to the top on every run.
State (latest ID per type) is stored in state.json so each run only fetches
what it hasn't seen before.

Run manually for a first-time / forced refresh:
  python fetch.py
"""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE_URL = "https://siberguvenlik.gov.tr/api/address/index"
OUTPUT_FILE = "url-list.txt"
STATE_FILE = "state.json"

TYPES = ["ip", "ip6", "domain", "url"]

WORKERS = 20
RETRY_LIMIT = 5


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)
        f.write("\n")


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


def fetch_new_entries(type_: str, known_id: int) -> tuple[int, list[str]]:
    """
    Fetch pages newest-first, stopping when we hit an entry already seen.
    Returns (new_latest_id, new_entries) — entries are newest-first.
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
                    break  # nothing new

            reached_known = False
            for m in models:
                if m["id"] > known_id:
                    new_entries.append(m["url"])
                else:
                    reached_known = True
                    break

            if reached_known:
                break
            page += 1

    return new_latest_id, new_entries


def main():
    state = load_state()
    all_new: list[str] = []

    for type_ in TYPES:
        known_id = state.get(type_, 0)
        print(f"[{type_}] known_id={known_id}")
        t0 = time.time()
        new_latest_id, new_entries = fetch_new_entries(type_, known_id)

        if new_entries:
            print(f"  -> +{len(new_entries)} new entries ({time.time() - t0:.1f}s)")
            all_new.extend(new_entries)
            state[type_] = new_latest_id
        else:
            print(f"  -> no new entries")

    if all_new:
        if os.path.exists(OUTPUT_FILE):
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                existing = f.read()
        else:
            existing = ""

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for entry in all_new:
                f.write(entry + "\n")
            f.write(existing)

        print(f"\nPrepended {len(all_new)} new entries to {OUTPUT_FILE}")
    else:
        print(f"\nNo new entries — {OUTPUT_FILE} unchanged")

    save_state(state)
    print("Done.")


if __name__ == "__main__":
    main()
