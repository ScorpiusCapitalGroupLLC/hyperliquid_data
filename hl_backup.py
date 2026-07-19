#!/usr/bin/env python3
"""
Hyperliquid 每日增量 CSV 备份（仅用标准库）。
- 抓取官方 Info API 针对某地址公开的历史数据流: 成交/资金费/非资金费账本/TWAP
- 每月每个数据流一个文件: <out_dir>/<stream>_YYYY-MM.csv (按北京时间划分)
- 仅追加、增量、去重(按 _key)、带重叠窗口防漏记、带文件锁防并发
建议每天 04:00 (Asia/Shanghai) 跑一次，重复跑也安全。
"""

import csv, json, os, time, hashlib, logging
import urllib.request, urllib.error
from datetime import datetime, timezone, timedelta

# ----------------------------- 配置 -----------------------------
# 钱包地址必须通过环境变量提供（不再硬编码默认地址，避免配错时静默备份别人）。
WALLET     = os.environ.get("HL_WALLET", "").strip().lower()
API_URL    = "https://api.hyperliquid.xyz/info"
OUT_DIR    = os.environ.get("HL_OUT_DIR",
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), "hyperliquid_data"))
STATE_FILE = os.path.join(OUT_DIR, "_state.json")
LOCK_FILE  = os.path.join(OUT_DIR, "_lock")

TZ_OFFSET_HOURS  = float(os.environ.get("HL_TZ_OFFSET", "8"))     # 月份/时间按此时区，北京=+8
LOCAL_TZ         = timezone(timedelta(hours=TZ_OFFSET_HOURS))
DEFAULT_START_MS = int(os.environ.get("HL_START_MS", "0"))        # 首次回填起点(ms)，0=尽量往前
OVERLAP_MS       = int(os.environ.get("HL_OVERLAP_MS", str(2*24*3600*1000)))  # 重叠窗口=2天
REQUEST_PAUSE_S  = 0.25                                           # 翻页间隔，礼貌限速

# 各接口分页上限（来自官方文档）：
#  - userFillsByTime 每页最多 2000 条
#  - 带时间范围的接口(userFunding / userNonFundingLedgerUpdates /
#    userTwapSliceFillsByTime) 每页最多 500 条
# 提成常量，避免散落的魔数；接口若调整这里改一处即可。
FILLS_PAGE  = int(os.environ.get("HL_FILLS_PAGE", "2000"))
LEDGER_PAGE = int(os.environ.get("HL_LEDGER_PAGE", "500"))
TWAP_PAGE   = int(os.environ.get("HL_TWAP_PAGE", "500"))

# ----------------------------- 日志 ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)
log = logging.getLogger("hl_backup")

# --------------------------- HTTP 工具 ---------------------------
def post_info(body, max_retries=6):
    data = json.dumps(body).encode("utf-8")
    last = None
    for i in range(max_retries):
        req = urllib.request.Request(API_URL, data=data,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = e
            time.sleep((i+1) * (5 if e.code == 429 else 2))   # 429 退避更久
        except (urllib.error.URLError, TimeoutError) as e:
            last = e
            time.sleep((i+1) * 2)
    raise RuntimeError(f"请求失败(重试{max_retries}次): {last}")

def post_list(body):
    r = post_info(body)
    if r is None:
        return []
    if not isinstance(r, list):
        raise RuntimeError(f"预期数组，实际返回: {str(r)[:200]}")
    return r

# ----------------------------- 工具 ------------------------------
def now_ms(): return int(time.time() * 1000)
def iso(ms):  return datetime.fromtimestamp(ms/1000, tz=LOCAL_TZ).isoformat()
def month_tag(ms): return datetime.fromtimestamp(ms/1000, tz=LOCAL_TZ).strftime("%Y-%m")
def content_key(prefix, obj):
    h = hashlib.sha1(json.dumps(obj, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    return f"{prefix}-{h}"

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f: return json.load(f)
    return {}
def save_state(s):
    os.makedirs(OUT_DIR, exist_ok=True)
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f: json.dump(s, f, indent=2)
    os.replace(tmp, STATE_FILE)

def acquire_lock():
    os.makedirs(OUT_DIR, exist_ok=True)
    f = open(LOCK_FILE, "w")
    try:
        import fcntl
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except ImportError:
        pass  # 非 POSIX 系统跳过锁
    except BlockingIOError:
        log.warning("已有实例在运行，退出。")
        raise SystemExit(0)
    return f  # 返回并保持打开，进程结束时自动释放

def month_path(stream, ts_ms):
    return os.path.join(OUT_DIR, f"{stream}_{month_tag(ts_ms)}.csv")

def load_seen_keys(stream, since_ms):
    """只读取重叠窗口涉及的月份文件，且只收集 _ts>=since_ms 的键，开销恒定。"""
    seen = set()
    if not os.path.isdir(OUT_DIR): return seen
    since_tag = month_tag(since_ms)
    for name in os.listdir(OUT_DIR):
        if not (name.startswith(stream + "_") and name.endswith(".csv")): continue
        tag = name[len(stream)+1:-4]          # YYYY-MM
        if tag < since_tag: continue          # 字典序比较即可
        with open(os.path.join(OUT_DIR, name), encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                try: ts = int(row.get("_ts", "0"))
                except ValueError: ts = 0
                if ts >= since_ms and row.get("_key"):
                    seen.add(row["_key"])
    return seen

def append_rows(stream, rows, columns):
    if not rows: return 0
    os.makedirs(OUT_DIR, exist_ok=True)
    by_file = {}
    for r in rows:
        by_file.setdefault(month_path(stream, r["_ts"]), []).append(r)
    written = 0
    for path, recs in by_file.items():
        new = not os.path.exists(path)
        with open(path, "a", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            if new: w.writeheader()
            for r in sorted(recs, key=lambda x: x["_ts"]):
                w.writerow(r); written += 1
    return written

def commit(stream, rows, columns, state, since_ms):
    seen = load_seen_keys(stream, since_ms)
    fresh, batch_seen = [], set()
    for r in rows:
        k = r["_key"]
        if k in seen or k in batch_seen:  # 不重复
            continue
        batch_seen.add(k); fresh.append(r)
    n = append_rows(stream, fresh, columns)
    if rows:
        state.setdefault("last_time", {})
        mx = max(r["_ts"] for r in rows)
        state["last_time"][stream] = max(state["last_time"].get(stream, 0), mx)
    log.info("[%s] 拉取 %d 条, 新增 %d 条", stream, len(rows), n)
    return n

def start_for(stream, state):
    last = state.get("last_time", {}).get(stream, 0)
    return max(DEFAULT_START_MS, last - OVERLAP_MS) if last else DEFAULT_START_MS

# --------------------------- 各数据流 ----------------------------
FILLS_COLS = ["_key","_ts","time_ms","time_iso","coin","dir","side","px","sz",
              "startPosition","closedPnl","fee","feeToken","builderFee","oid","tid",
              "hash","crossed","liquidation"]
def fetch_fills(start_ms, end_ms, page=FILLS_PAGE):
    out, cur = [], start_ms
    while True:
        batch = post_list({"type":"userFillsByTime","user":WALLET,
                           "startTime":cur,"endTime":end_ms})
        if not batch: break
        out += batch
        if len(batch) < page: break
        mx = max(f["time"] for f in batch)
        cur = mx if mx > cur else cur + 1   # 防死循环；重叠由去重处理
        time.sleep(REQUEST_PAUSE_S)
    return out
def fills_rows(fills):
    rows = []
    for f in fills:
        rows.append({"_key":f"fill-{f.get('tid')}","_ts":f["time"],
            "time_ms":f["time"],"time_iso":iso(f["time"]),"coin":f.get("coin"),
            "dir":f.get("dir"),"side":f.get("side"),"px":f.get("px"),"sz":f.get("sz"),
            "startPosition":f.get("startPosition"),"closedPnl":f.get("closedPnl"),
            "fee":f.get("fee"),"feeToken":f.get("feeToken"),"builderFee":f.get("builderFee"),
            "oid":f.get("oid"),"tid":f.get("tid"),"hash":f.get("hash"),
            "crossed":f.get("crossed"),
            "liquidation":json.dumps(f["liquidation"],ensure_ascii=False) if f.get("liquidation") else ""})
    return rows

def fetch_ledger(req_type, start_ms, end_ms, page=LEDGER_PAGE):
    out, cur = [], start_ms
    while True:
        batch = post_list({"type":req_type,"user":WALLET,
                           "startTime":cur,"endTime":end_ms})
        if not batch: break
        out += batch
        if len(batch) < page: break
        mx = max(x["time"] for x in batch)
        cur = mx if mx > cur else cur + 1
        time.sleep(REQUEST_PAUSE_S)
    return out

FUNDING_COLS = ["_key","_ts","time_ms","time_iso","hash","coin","usdc","szi",
                "fundingRate","nSamples","type"]
def funding_rows(items):
    rows = []
    for x in items:
        d = x.get("delta",{}) or {}
        rows.append({"_key":content_key("funding",x),"_ts":x["time"],
            "time_ms":x["time"],"time_iso":iso(x["time"]),"hash":x.get("hash"),
            "coin":d.get("coin"),"usdc":d.get("usdc"),"szi":d.get("szi"),
            "fundingRate":d.get("fundingRate"),"nSamples":d.get("nSamples"),
            "type":d.get("type")})
    return rows

LEDGER_COLS = ["_key","_ts","time_ms","time_iso","hash","type","usdc","amount",
               "token","delta_json"]
def ledger_rows(items):
    rows = []
    for x in items:
        d = x.get("delta",{}) or {}
        rows.append({"_key":content_key("ledger",x),"_ts":x["time"],
            "time_ms":x["time"],"time_iso":iso(x["time"]),"hash":x.get("hash"),
            "type":d.get("type"),"usdc":d.get("usdc"),"amount":d.get("amount"),
            "token":d.get("token"),
            "delta_json":json.dumps(d,ensure_ascii=False)})  # 完整保留，避免漏字段
    return rows

TWAP_COLS = ["_key","_ts","time_ms","time_iso","twapId","coin","side","px","sz",
             "closedPnl","fee","oid","tid","hash"]
def fetch_twap(start_ms, end_ms, page=TWAP_PAGE):
    """使用带时间窗的 userTwapSliceFillsByTime，支持增量与翻页。
    旧的 userTwapSliceFills 只返回最近 2000 条且无法翻页，会漏历史数据。"""
    out, cur = [], start_ms
    while True:
        batch = post_list({"type":"userTwapSliceFillsByTime","user":WALLET,
                           "startTime":cur,"endTime":end_ms})
        if not batch: break
        out += batch
        if len(batch) < page: break
        times = [x["fill"]["time"] for x in batch if x.get("fill") and x["fill"].get("time")]
        if not times: break
        mx = max(times)
        cur = mx if mx > cur else cur + 1
        time.sleep(REQUEST_PAUSE_S)
    return out
def twap_rows(items):
    rows = []
    for x in items:
        f = x.get("fill",{}) or {}
        if not f.get("time"): continue
        rows.append({"_key":f"twap-{f.get('tid')}","_ts":f["time"],
            "time_ms":f["time"],"time_iso":iso(f["time"]),
            "twapId":x.get("twapId"),"coin":f.get("coin"),"side":f.get("side"),
            "px":f.get("px"),"sz":f.get("sz"),"closedPnl":f.get("closedPnl"),
            "fee":f.get("fee"),"oid":f.get("oid"),"tid":f.get("tid"),"hash":f.get("hash")})
    return rows

# ------------------------------ 主流程 ----------------------------
def run_stream(name, build_rows, columns, state):
    since = start_for(name, state)
    try:
        rows = build_rows(since)
        commit(name, rows, columns, state, since)
        save_state(state)          # 每个流跑完即保存进度
    except Exception as e:
        log.error("[%s] 失败，已跳过: %s", name, e)

def main():
    if not WALLET:
        log.error("未设置 HL_WALLET 环境变量，拒绝运行。请先 export HL_WALLET=0x...")
        raise SystemExit(1)
    if not (WALLET.startswith("0x") and len(WALLET) == 42
            and all(c in "0123456789abcdef" for c in WALLET[2:])):
        log.error("HL_WALLET 不是合法的地址(应为 0x + 40 位十六进制)，当前值=%r。"
                  "请检查 GitHub 仓库的 HL_WALLET secret 是否已正确设置。", WALLET)
        raise SystemExit(1)
    os.makedirs(OUT_DIR, exist_ok=True)
    _lock = acquire_lock()         # 防止与上一次未结束的任务并发(仅本地有效)
    state = load_state()
    end = now_ms()
    log.info("=== Hyperliquid 备份 %s 地址 %s ===", iso(end), WALLET)

    run_stream("fills",   lambda s: fills_rows(fetch_fills(s, end)), FILLS_COLS, state)
    run_stream("funding", lambda s: funding_rows(fetch_ledger("userFunding", s, end)), FUNDING_COLS, state)
    run_stream("ledger",  lambda s: ledger_rows(fetch_ledger("userNonFundingLedgerUpdates", s, end)), LEDGER_COLS, state)
    run_stream("twap",    lambda s: twap_rows(fetch_twap(s, end)), TWAP_COLS, state)

    save_state(state)
    log.info("=== 完成 ===")

if __name__ == "__main__":
    main()
