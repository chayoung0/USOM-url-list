#!/usr/bin/env python3
"""
Fetches all entries from the siberguvenlik.gov.tr API and writes one txt file
per type — plain newline-separated lists, ready to be served as HTTP resources
(e.g. as a GitHub Pages feed for FortiGate / other NGFW threat-feed imports).
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE_URL = "https://siberguvenlik.gov.tr/api/address/index"

# API type -> output filename
TYPES = {
    "ip":     "ip-list.txt",
    "ip6":    "ip6-list.txt",
    "domain": "url-list.txt",
}

WORKERS = 20
RETRY_LIMIT = 5


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
            print(f"  [warn] {type_} page {page} attempt {attempt+1} failed ({exc}), retry in {wait}s")
            time.sleep(wait)


def fetch_type(type_: str) -> list[str]:
    with requests.Session() as session:
        first = fetch_page(session, type_, 1)
        page_count = first["pageCount"]
        total = first["totalCount"]
        entries: list[str] = [m["url"] for m in first["models"]]

        print(f"  [{type_}] total={total}, pages={page_count}")

        if page_count <= 1:
            return entries

        def load(page):
            data = fetch_page(session, type_, page)
            return page, [m["url"] for m in data["models"]]

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
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

    return entries


def main():
    for type_, filename in TYPES.items():
        print(f"Fetching type={type_} -> {filename}")
        t0 = time.time()
        entries = fetch_type(type_)
        elapsed = time.time() - t0

        with open(filename, "w", encoding="utf-8") as f:
            for entry in entries:
                f.write(entry + "\n")

        print(f"  -> {filename}: {len(entries)} entries ({elapsed:.1f}s)")

    print("Done.")


if __name__ == "__main__":
    main()
