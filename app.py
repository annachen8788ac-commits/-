import asyncio
import csv
import json
import lzma
import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg
import websockets
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse

DATABASE_URL = os.environ["DATABASE_URL"]
SIGNAL_API_URL = os.getenv("SIGNAL_API_URL", "http://signal:8080").rstrip("/")
SIGNAL_NUMBER = os.getenv("SIGNAL_NUMBER", "")
SIGNAL_GROUP_ID = os.getenv("SIGNAL_GROUP_ID", "")
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "America/Los_Angeles")
HISTORY_CSV = os.getenv("HISTORY_CSV", "/app/history.csv")

app = FastAPI(title="Signal Group Number Tracker")
pool = None
listener_task = None

NUM_RE = re.compile(r'\+?\s*\(?\d[\d\s\-\u2010-\u2015\u2212\u00a0\u2007\u202f().+]*\d')

def extract_numbers(text: str):
    out = []
    for m in NUM_RE.finditer(text or ""):
        raw = re.sub(r"[\u00a0\u2007\u202f\t]+", " ", m.group(0).strip())
        raw = re.sub(r"[\u2010-\u2015\u2212\u2011]", "-", raw)
        raw = re.sub(r" +", " ", raw).strip()
        digits = re.sub(r"\D", "", raw)
        if len(digits) >= 4:
            out.append((raw, digits[-4:]))
    return out

async def ensure_schema():
    async with pool.acquire() as conn:
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


def ensure_history_file():
    path = Path(HISTORY_CSV)
    if path.exists():
        return
    compressed = Path("/app/history.csv.xz")
    if compressed.exists():
        with lzma.open(compressed, "rb") as src, path.open("wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)

async def import_history():
    path = Path(HISTORY_CSV)
    if not path.exists():
        return
    async with pool.acquire() as conn:
        batch = []
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                digits = re.sub(r"\D", "", row["full_number"])
                if len(digits) < 4:
                    continue
                batch.append((
                    "history",
                    row["history_id"],
                    None,
                    row["date_text"],
                    row["sender_name"],
                    row["full_number"],
                    digits[-4:],
                    None,
                ))
                if len(batch) >= 2000:
                    await conn.executemany("""
                        INSERT INTO number_records
                        (source, source_key, event_ts, date_text, sender_name, full_number, last4, group_id)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                        ON CONFLICT (source_key) DO NOTHING
                    """, batch)
                    batch.clear()
            if batch:
                await conn.executemany("""
                    INSERT INTO number_records
                    (source, source_key, event_ts, date_text, sender_name, full_number, last4, group_id)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                    ON CONFLICT (source_key) DO NOTHING
                """, batch)

def websocket_url():
    url = SIGNAL_API_URL
    if url.startswith("https://"):
        url = "wss://" + url[8:]
    elif url.startswith("http://"):
        url = "ws://" + url[7:]
    return f"{url}/v1/receive/{SIGNAL_NUMBER}"

async def store_signal_payload(payload):
    items = payload.get("payload") if isinstance(payload, dict) and isinstance(payload.get("payload"), list) else [payload]
    for item in items:
        if not isinstance(item, dict):
            continue
        env = item.get("envelope", item)
        data = env.get("dataMessage") or {}
        group = data.get("groupInfo") or {}
        group_id = group.get("groupId") or group.get("group_id")
        if group_id != SIGNAL_GROUP_ID:
            continue

        message = data.get("message") or ""
        nums = extract_numbers(message)
        if not nums:
            continue

        sender = env.get("sourceName") or env.get("sourceNumber") or env.get("source") or "未知发送者"
        ts_ms = data.get("timestamp") or env.get("timestamp")
        try:
            ts = datetime.fromtimestamp(int(ts_ms) / 1000, tz=ZoneInfo(APP_TIMEZONE))
        except Exception:
            ts = datetime.now(tz=ZoneInfo(APP_TIMEZONE))

        base_key = f"signal:{group_id}:{sender}:{ts_ms}"
        async with pool.acquire() as conn:
            for idx, (full_number, last4) in enumerate(nums):
                await conn.execute("""
                    INSERT INTO number_records
                    (source, source_key, event_ts, date_text, sender_name, full_number, last4, group_id)
                    VALUES ('signal',$1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (source_key) DO NOTHING
                """,
                f"{base_key}:{idx}",
                ts,
                ts.strftime("%Y-%m-%d"),
                sender,
                full_number,
                last4,
                group_id)

async def signal_listener():
    while True:
        if not SIGNAL_NUMBER or not SIGNAL_GROUP_ID:
            await asyncio.sleep(10)
            continue
        try:
            async with websockets.connect(websocket_url(), ping_interval=20, ping_timeout=20, max_size=4_000_000) as ws:
                async for raw in ws:
                    try:
                        await store_signal_payload(json.loads(raw))
                    except Exception as e:
                        print("message processing error:", repr(e), flush=True)
        except Exception as e:
            print("signal listener disconnected:", repr(e), flush=True)
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup():
    global pool, listener_task
    ensure_history_file()
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    await ensure_schema()
    await import_history()
    listener_task = asyncio.create_task(signal_listener())

@app.on_event("shutdown")
async def shutdown():
    if listener_task:
        listener_task.cancel()
    if pool:
        await pool.close()

@app.get("/health")
async def health():
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM number_records")
    return {"ok": True, "records": total, "signal_configured": bool(SIGNAL_NUMBER and SIGNAL_GROUP_ID)}

@app.get("/api/search")
async def search(last4: str = Query(..., min_length=1, max_length=4)):
    if not re.fullmatch(r"\d{1,4}", last4):
        return JSONResponse({"error": "last4 must contain 1 to 4 digits"}, status_code=400)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT date_text, sender_name, full_number, source
            FROM number_records
            WHERE last4 LIKE $1
            ORDER BY COALESCE(event_ts, created_at) DESC, id DESC
            LIMIT 5000
        """, last4 + "%")
    return [dict(r) for r in rows]

@app.get("/api/stats")
async def stats():
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM number_records")
        history = await conn.fetchval("SELECT COUNT(*) FROM number_records WHERE source='history'")
        signal = await conn.fetchval("SELECT COUNT(*) FROM number_records WHERE source='signal'")
    return {"total": total, "history": history, "signal": signal}

PAGE = r"""<!doctype html>
<html lang="zh-CN"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>号码记录查询</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#f5f7fb;color:#172033;font-family:Arial,"Microsoft YaHei",sans-serif}
.wrap{max-width:1100px;margin:36px auto;padding:0 20px}.card{background:#fff;border-radius:14px;padding:24px;box-shadow:0 5px 24px rgba(0,0,0,.07)}
h1{margin:0 0 8px}.sub{color:#667085;margin-bottom:20px}
input{width:100%;font-size:26px;padding:15px 17px;border:2px solid #cfd6e4;border-radius:10px;outline:none}
input:focus{border-color:#315efb}.meta{margin:16px 0;color:#475467;font-weight:600}
.table{max-height:68vh;overflow:auto;border:1px solid #e4e7ec;border-radius:10px}
table{width:100%;border-collapse:collapse}th{position:sticky;top:0;background:#1f4e78;color:white;text-align:left;padding:11px}
td{padding:10px 11px;border-bottom:1px solid #edf0f5}.num{font-family:Consolas,monospace}
.badge{font-size:12px;background:#eef2ff;border-radius:20px;padding:3px 8px}.empty{text-align:center;color:#98a2b3;padding:42px}
</style></head><body><div class="wrap"><div class="card">
<h1>号码记录查询</h1>
<div class="sub">只针对号码最后四位匹配。输入第 1 位开始自动显示，继续输入会实时缩小结果；删除数字也会立即刷新。</div>
<input id="q" maxlength="4" inputmode="numeric" autocomplete="off" placeholder="输入后四位，例如 0698" autofocus>
<div id="meta" class="meta">正在读取数据库...</div>
<div class="table"><table><thead><tr><th>年月日</th><th>发送者</th><th>完整号码</th><th>来源</th></tr></thead>
<tbody id="body"><tr><td colspan="4" class="empty">输入 1 至 4 位尾号后自动显示</td></tr></tbody></table></div>
</div></div>
<script>
const q=document.getElementById('q'), meta=document.getElementById('meta'), body=document.getElementById('body');
let controller=null;
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

function clearResults(){
  body.innerHTML='<tr><td colspan="4" class="empty">输入 1 至 4 位尾号后自动显示</td></tr>';
  meta.textContent='请输入 1 至 4 位尾号';
}

async function stats(){
  try{
    const x=await fetch('/api/stats').then(r=>r.json());
    meta.textContent=`数据库共 ${x.total.toLocaleString()} 条，其中历史 ${x.history.toLocaleString()} 条，Signal 新增 ${x.signal.toLocaleString()} 条`;
  }catch(e){ meta.textContent='数据库连接中...'; }
}

async function searchNow(x){
  if(controller) controller.abort();
  controller=new AbortController();
  meta.textContent='查询中...';
  try{
    const a=await fetch('/api/search?last4='+encodeURIComponent(x),{signal:controller.signal}).then(r=>r.json());
    meta.textContent=`尾号输入 ${x}：找到 ${a.length.toLocaleString()} 条（仅匹配号码最后四位）`;
    body.innerHTML=a.length
      ? a.map(r=>`<tr><td>${esc(r.date_text)}</td><td>${esc(r.sender_name)}</td><td class="num">${esc(r.full_number)}</td><td><span class="badge">${r.source==='signal'?'实时':'历史'}</span></td></tr>`).join('')
      : '<tr><td colspan="4" class="empty">没有匹配记录</td></tr>';
  }catch(e){
    if(e.name!=='AbortError'){ meta.textContent='查询失败，请稍后重试'; }
  }
}

q.addEventListener('input',()=>{
  const x=q.value.replace(/\D/g,'').slice(0,4);
  q.value=x;
  if(x.length>=1){
    searchNow(x);
  }else{
    if(controller) controller.abort();
    clearResults();
  }
});

stats();
</script></body></html>"""

@app.get("/", response_class=HTMLResponse)
async def page():
    return PAGE
