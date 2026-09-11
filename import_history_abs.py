import asyncio
import base64
import csv
import gzip
import io
import json
import lzma
import os
import re
import zlib
from pathlib import Path

import asyncpg

DATABASE_URL = os.environ["DATABASE_URL"]
BASE = Path("/app")
REQUIRED = {"history_id", "date_text", "sender_name", "full_number"}


def unpack_candidates(data: bytes):
    out = [("raw", data)]
    for name, fn in (("xz", lzma.decompress), ("gzip", gzip.decompress), ("zlib", zlib.decompress)):
        try:
            out.append((name, fn(data)))
        except Exception:
            pass
    return out


def inspect_blob(label: str, blob: bytes):
    info = {"label": label, "bytes": len(blob), "utf8": False, "format": "binary", "fields": []}
    try:
        text = blob.decode("utf-8-sig")
    except Exception:
        print(f"history inspect: {label} bytes={len(blob)} utf8=no format=binary", flush=True)
        return info

    info["utf8"] = True
    stripped = text.lstrip()

    # JSON / JSONL
    if stripped.startswith("[") or stripped.startswith("{"):
        try:
            obj = json.loads(text)
            info["format"] = "json"
            if isinstance(obj, list) and obj and isinstance(obj[0], dict):
                info["fields"] = sorted(obj[0].keys())
            elif isinstance(obj, dict):
                info["fields"] = sorted(obj.keys())
            print(f"history inspect: {label} bytes={len(blob)} utf8=yes format=json fields={info['fields']}", flush=True)
            return info
        except Exception:
            pass

    lines = [ln for ln in text.splitlines() if ln.strip()]
    if lines:
        try:
            sample_objs = []
            for ln in lines[:5]:
                v = json.loads(ln)
                if isinstance(v, dict):
                    sample_objs.append(v)
            if sample_objs:
                info["format"] = "jsonl"
                info["fields"] = sorted(sample_objs[0].keys())
                print(f"history inspect: {label} bytes={len(blob)} utf8=yes format=jsonl fields={info['fields']}", flush=True)
                return info
        except Exception:
            pass

    # Delimited text. Only field names are emitted.
    for delim, fmt in ((",", "csv"), ("\t", "tsv"), (";", "semicolon"), ("|", "pipe")):
        try:
            reader = csv.reader(io.StringIO(text), delimiter=delim)
            header = next(reader, [])
            if len(header) >= 2:
                fields = [str(x).strip() for x in header]
                info["format"] = fmt
                info["fields"] = fields
                print(f"history inspect: {label} bytes={len(blob)} utf8=yes format={fmt} fields={fields}", flush=True)
                return info
        except Exception:
            pass

    info["format"] = "plain-text"
    print(f"history inspect: {label} bytes={len(blob)} utf8=yes format=plain-text fields=[] lines={len(lines)}", flush=True)
    return info


def parse_rows(blob: bytes):
    try:
        text = blob.decode("utf-8-sig")
    except Exception:
        return None, None

    # Expected CSV
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames and REQUIRED.issubset(set(reader.fieldnames)):
        return list(reader), reader.fieldnames

    # TSV with expected field names
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    if reader.fieldnames and REQUIRED.issubset(set(reader.fieldnames)):
        return list(reader), reader.fieldnames

    # JSON / JSONL with expected field names
    try:
        obj = json.loads(text)
        if isinstance(obj, list) and all(isinstance(x, dict) for x in obj):
            fields = sorted(set().union(*(x.keys() for x in obj))) if obj else []
            if REQUIRED.issubset(set(fields)):
                return obj, fields
    except Exception:
        pass

    try:
        rows = [json.loads(ln) for ln in text.splitlines() if ln.strip()]
        if rows and all(isinstance(x, dict) for x in rows):
            fields = sorted(set().union(*(x.keys() for x in rows)))
            if REQUIRED.issubset(set(fields)):
                return rows, fields
    except Exception:
        pass

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
        data = path.read_bytes()
        parsed = False
        for kind, blob in unpack_candidates(data):
            inspect_blob(f"{path.name}:{kind}", blob)
            rows, fields = parse_rows(blob)
            if rows is not None:
                sources.append(f"file:{path.name}:{kind}:{len(rows)}")
                print(f"history importer: {path.name} parser={kind} fields={fields} rows={len(rows)}", flush=True)
                add_rows(rows, out, seen)
                parsed = True
                break
        if not parsed:
            print(f"history importer: {path.name} no supported record parser matched", flush=True)

    seeds = []
    for i in range(100):
        v = (os.getenv(f"HISTORY_SEED_{i:02d}") or "").strip()
        if v:
            seeds.append(v)
    if seeds:
        joined = "".join(seeds)
        candidates = [("seed-text", joined.encode())]
        compact = re.sub(r"\s+", "", joined)
        for name, decoder in (("seed-b64", base64.b64decode), ("seed-urlb64", base64.urlsafe_b64decode)):
            try:
                candidates.append((name, decoder(compact + "=" * ((4-len(compact)%4)%4))))
            except Exception:
                pass
        for name, raw in candidates:
            for kind, blob in unpack_candidates(raw):
                inspect_blob(f"{name}:{kind}", blob)
                rows, fields = parse_rows(blob)
                if rows is not None:
                    sources.append(f"seed:{name}:{kind}:{len(rows)}")
                    print(f"history importer: seed parser={name}:{kind} fields={fields} rows={len(rows)}", flush=True)
                    add_rows(rows, out, seen)
                    return out, sources

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
