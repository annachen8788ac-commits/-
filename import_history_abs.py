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
BASE = Path("/app")
REQUIRED = {"history_id", "date_text", "sender_name", "full_number"}


def parse_csv(data: bytes):
    candidates = [data]
    for fn in (lzma.decompress, gzip.decompress, zlib.decompress):
        try:
            candidates.append(fn(data))
        except Exception:
            pass
    for blob in candidates:
        try:
            text = blob.decode("utf-8-sig")
        except Exception:
            continue
        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames and REQUIRED.issubset(set(reader.fieldnames)):
            return list(reader), reader.fieldnames
    return None, None


def add_rows(rows, out, seen):
    if not rows:
        return
    for row in rows:
        hid = (row.get("history_id") or "").strip()
        if hid and hid not in seen:
            seen.add(hid)
            out.append(row)


def collect():
    out, seen, sources = [], set(), []
    parts = sorted((BASE / "history_parts").glob("*.xz"))
    for path in parts:
        rows, fields = parse_csv(path.read_bytes())
        if rows is not None:
            sources.append(f"file:{path.name}:{len(rows)}")
            print(f"history importer: {path.name} header={fields} rows={len(rows)}", flush=True)
            add_rows(rows, out, seen)
        else:
            print(f"history importer: {path.name} could not be decoded as expected CSV", flush=True)

    seeds = []
    for i in range(100):
        v = (os.getenv(f"HISTORY_SEED_{i:02d}") or "").strip()
        if v:
            seeds.append(v)
    if seeds:
        joined = "".join(seeds)
        candidates = [joined.encode()]
        compact = re.sub(r"\s+", "", joined)
        try:
            candidates.append(base64.b64decode(compact + "=" * ((4-len(compact)%4)%4)))
        except Exception:
            pass
        try:
            candidates.append(base64.urlsafe_b64decode(compact + "=" * ((4-len(compact)%4)%4)))
        except Exception:
            pass
        for idx, blob in enumerate(candidates):
            rows, fields = parse_csv(blob)
            if rows is not None:
                sources.append(f"seed:{idx}:{len(rows)}")
                print(f"history importer: seed header={fields} rows={len(rows)}", flush=True)
                add_rows(rows, out, seen)
                break

    return out, sources


async def main():
    rows, sources = collect()
    print(f"history importer: decoded_unique={len(rows)} sources={sources}", flush=True)
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
        """)
        batch=[]
        valid=0
        for row in rows:
            num=(row.get("full_number") or "").strip()
            digits=re.sub(r"\D", "", num)
            hid=(row.get("history_id") or "").strip()
            if not hid or len(digits)<4:
                continue
            valid+=1
            batch.append(("history",hid,None,(row.get("date_text") or "").strip(),(row.get("sender_name") or "").strip(),num,digits[-4:],None))
        if batch:
            await conn.executemany("""
            INSERT INTO number_records(source,source_key,event_ts,date_text,sender_name,full_number,last4,group_id)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT(source_key) DO NOTHING
            """, batch)
        total=await conn.fetchval("SELECT COUNT(*) FROM number_records WHERE source='history'")
        print(f"history importer: valid={valid} database_history_total={total}", flush=True)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
