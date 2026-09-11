import asyncio
import base64
import csv
import gzip
import io
import lzma
import os
import re
import zlib
from pathlib import Path

import asyncpg

DATABASE_URL = os.environ["DATABASE_URL"]
REQUIRED = {"history_id", "date_text", "sender_name", "full_number"}


def _decode_csv_bytes(data: bytes):
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return None
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or not REQUIRED.issubset(set(reader.fieldnames)):
        return None
    return list(reader)


def _try_unpack(data: bytes):
    candidates = [data]
    for fn in (lzma.decompress, gzip.decompress, zlib.decompress):
        try:
            candidates.append(fn(data))
        except Exception:
            pass
    for item in candidates:
        rows = _decode_csv_bytes(item)
        if rows is not None:
            return rows
    return None


def _seed_candidates():
    seeds = []
    for key in sorted(k for k in os.environ if k.startswith("HISTORY_SEED_")):
        value = (os.getenv(key) or "").strip()
        if value:
            seeds.append(value)
    if not seeds:
        return []

    joined = "".join(seeds)
    out = [joined.encode("utf-8")]

    for value in (joined, *seeds):
        compact = re.sub(r"\s+", "", value)
        for decoder in (base64.b64decode, base64.urlsafe_b64decode):
            try:
                pad = "=" * ((4 - len(compact) % 4) % 4)
                out.append(decoder(compact + pad))
            except Exception:
                pass

    try:
        pieces = []
        for value in seeds:
            compact = re.sub(r"\s+", "", value)
            pad = "=" * ((4 - len(compact) % 4) % 4)
            pieces.append(base64.b64decode(compact + pad))
        out.append(b"".join(pieces))
    except Exception:
        pass

    return out


def collect_rows():
    sources = []
    seen_ids = set()
    rows_out = []

    for path in sorted(Path("history_parts").glob("*.xz")):
        try:
            rows = _try_unpack(path.read_bytes())
            if rows is not None:
                sources.append(f"file:{path}")
                for row in rows:
                    hid = (row.get("history_id") or "").strip()
                    if hid and hid not in seen_ids:
                        seen_ids.add(hid)
                        rows_out.append(row)
        except Exception as exc:
            print(f"history importer: failed {path}: {exc!r}", flush=True)

    root_xz = Path("history.csv.xz")
    if root_xz.exists():
        try:
            rows = _try_unpack(root_xz.read_bytes())
            if rows is not None:
                sources.append("file:history.csv.xz")
                for row in rows:
                    hid = (row.get("history_id") or "").strip()
                    if hid and hid not in seen_ids:
                        seen_ids.add(hid)
                        rows_out.append(row)
        except Exception as exc:
            print(f"history importer: failed history.csv.xz: {exc!r}", flush=True)

    for idx, candidate in enumerate(_seed_candidates()):
        rows = _try_unpack(candidate)
        if rows is None:
            continue
        sources.append(f"env-candidate:{idx}")
        for row in rows:
            hid = (row.get("history_id") or "").strip()
            if hid and hid not in seen_ids:
                seen_ids.add(hid)
                rows_out.append(row)

    return rows_out, sources


async def main():
    rows, sources = collect_rows()
    print(f"history importer: decoded {len(rows)} unique rows from {sources}", flush=True)
    if not rows:
        return

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS number_records (
            id BIGSERIAL PRIMARY KEY,
            source TEXT NOT NULL,
            source_key TEXT NOT NULL UNIQUE,
            event_ts TIMESTAMPTZ,
            date_text TEXT NOT NULL,
            sender_name TEXT NOT NULL,
            full_number TEXT NOT NULL,
            last4 TEXT NOT NULL,
            group_id TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_number_records_last4 ON number_records(last4);
        CREATE INDEX IF NOT EXISTS idx_number_records_date_text ON number_records(date_text);
        CREATE INDEX IF NOT EXISTS idx_number_records_sender_name ON number_records(sender_name);
        """)

        batch = []
        valid = 0
        for row in rows:
            full_number = (row.get("full_number") or "").strip()
            digits = re.sub(r"\D", "", full_number)
            if len(digits) < 4:
                continue
            history_id = (row.get("history_id") or "").strip()
            if not history_id:
                continue
            valid += 1
            batch.append((
                "history",
                history_id,
                None,
                (row.get("date_text") or "").strip(),
                (row.get("sender_name") or "").strip(),
                full_number,
                digits[-4:],
                None,
            ))

        if batch:
            await conn.executemany("""
                INSERT INTO number_records
                (source, source_key, event_ts, date_text, sender_name, full_number, last4, group_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (source_key) DO NOTHING
            """, batch)

        total = await conn.fetchval("SELECT COUNT(*) FROM number_records WHERE source='history'")
        print(f"history importer: valid={valid}, database_history_total={total}", flush=True)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
