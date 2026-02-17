
"""
Fill missing Title values in ProductMappingTest using the Listings API.

Usage (from project root):
    python -m app.fba.update_titles

The script:
 - selects SKUs with empty/null Title from ProductMappingTest
 - fetches itemName from Listings API in batches (respects rate limits)
 - updates rows where a title was obtained, setting fba_updated_at = GETDATE()
"""
import time
from math import ceil
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..database import connect_database
from .helpers import fetch_listing_title, chunk
from .config import MAX_WORKERS, MAX_RETRIES, INITIAL_RETRY_DELAY

BATCH_SIZE = 500  # number of SKUs to process per DB/fetch batch
WORKER_CAP = max(1, min(int(MAX_WORKERS or 4), 10))


def _select_missing_titles(cursor):
    sql = "SELECT sku FROM ProductMappingTest WHERE Title IS NULL OR LTRIM(RTRIM(ISNULL(Title,''))) = ''"
    cursor.execute(sql)
    return [row[0] for row in cursor.fetchall()]


def _update_titles_many(cursor, updates):
    """
    updates: list of (title, sku)
    Use fast_executemany for bulk update performance.
    """
    try:
        cursor.fast_executemany = True
    except Exception:
        pass

    update_sql = "UPDATE ProductMappingTest SET Title = ?, fba_updated_at = GETDATE() WHERE sku = ?"
    cursor.executemany(update_sql, updates)


def _fetch_batch_titles(skus):
    """
    Fetch titles for skus with retries on quota errors.
    Returns dict {sku: title_or_None}.
    """
    to_fetch = list(skus)
    results = {}
    delay = INITIAL_RETRY_DELAY

    for attempt in range(1, MAX_RETRIES + 1):
        if not to_fetch:
            break

        print(f"[titles] Attempt {attempt}/{MAX_RETRIES} fetching {len(to_fetch)} SKUs")
        quota_hit = False
        with ThreadPoolExecutor(max_workers=WORKER_CAP) as ex:
            futures = {ex.submit(fetch_listing_title, sku): sku for sku in to_fetch}
            for fut in as_completed(futures):
                sku = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    print(f"[titles] Error fetching {sku}: {e}")
                    res = None

                # fetch_listing_title may return a quota sentinel dict {"_quota": True}
                if isinstance(res, dict) and res.get("_quota"):
                    quota_hit = True
                    # leave sku for next attempt
                else:
                    results[sku] = res

        if quota_hit and attempt < MAX_RETRIES:
            print(f"[titles] QuotaHit detected, backing off {delay}s before retrying remaining {len([s for s in to_fetch if s not in results])} SKUs")
            time.sleep(delay)
            delay *= 2
            # prepare next attempt list (those still not in results)
            to_fetch = [s for s in to_fetch if s not in results]
            continue
        else:
            # no quota or no further attempts, accept current results and mark missing as None
            for s in to_fetch:
                if s not in results:
                    results[s] = None
            break

    return results


def run_fill_titles(dry_run=False, limit=None):
    """
    Main entry point.
    dry_run: if True, don't write to DB (just show counts)
    limit: optional cap on number of SKUs to process (for testing)
    """
    conn = connect_database()
    cur = conn.cursor()

    skus = _select_missing_titles(cur)
    if limit:
        skus = skus[:limit]

    total = len(skus)
    print(f"[titles] SKUs missing titles: {total}")
    if total == 0:
        cur.close()
        conn.close()
        return 0

    processed = 0
    updated = 0

    try:
        for batch_idx, batch in enumerate(chunk(skus, BATCH_SIZE), start=1):
            print(f"[titles] Processing batch {batch_idx} ({len(batch)} SKUs)")
            fetched = _fetch_batch_titles(batch)

            # Prepare updates for those SKUs with a non-empty title
            updates = []
            for sku, title in fetched.items():
                if title:
                    updates.append((title, sku))

            print(f"[titles] Batch {batch_idx}: fetched titles={len([t for t in fetched.values() if t])} -> will update {len(updates)} rows")

            if updates and not dry_run:
                _update_titles_many(cur, updates)
                conn.commit()
                updated += len(updates)

            processed += len(batch)
            # gentle pause to avoid accidental bursts
            time.sleep(0.2)

        print(f"[titles] Completed. processed={processed}, updated={updated}")
        return updated
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    # simple CLI: run with optional environment-driven limits or dry-run by editing below
    run_fill_titles(dry_run=False)