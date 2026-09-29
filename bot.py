#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
اسکنر توالی رد/گرین روی همه فیوچرزهای Bybit، روی چند تایم‌فریم همزمان.
هیچ کتابخونه بیرونی لازم نداره (فقط استاندارد پایتون).
هر بار اجرا می‌شه: یه دور کامل اسکن می‌زنه، سیگنال‌های جدید رو به تلگرام می‌فرسته،
و وضعیت (state.json) رو برای اجرای بعدی ذخیره می‌کنه.
"""
import json, os, time, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

BASES = ["https://api.bybit.com", "https://api.bytick.com"]
TIMEFRAMES = os.environ.get("TIMEFRAMES", "1,3,5,15,30,60,240,D").split(",")
QUOTE = os.environ.get("QUOTE", "USDT")  # USDT / USDC / ALL
TF_MS = {"1":60_000,"3":180_000,"5":300_000,"15":900_000,"30":1_800_000,
         "60":3_600_000,"240":14_400_000,"D":86_400_000}
TF_LABEL = {"1":"1m","3":"3m","5":"5m","15":"15m","30":"30m","60":"1H","240":"4H","D":"1D"}
STATE_FILE = os.path.join(os.path.dirname(__file__), "state.json")
BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
WORKERS = int(os.environ.get("WORKERS", "20"))
FRESH_WINDOW_MULT = 3  # فقط سیگنال‌هایی که داخل این چند کندل اخیر باشن رو اطلاع بده (جلوگیری از اسپم موقع اجرای اول)


def http_get_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def pick_base():
    for b in BASES:
        try:
            j = http_get_json(b + "/v5/market/time")
            if j.get("retCode") == 0:
                return b
        except Exception:
            continue
    raise RuntimeError("Bybit در دسترس نیست")


def load_symbols(base):
    out, cursor = [], ""
    while True:
        url = base + "/v5/market/instruments-info?category=linear&limit=1000"
        if cursor:
            url += "&cursor=" + urllib.parse.quote(cursor)
        j = http_get_json(url)
        if j.get("retCode") != 0:
            raise RuntimeError(j.get("retMsg"))
        out += j["result"]["list"]
        cursor = j["result"].get("nextPageCursor") or ""
        if not cursor:
            break
    out = [i for i in out if i["status"] == "Trading" and i["contractType"] == "LinearPerpetual"]
    if QUOTE != "ALL":
        out = [i for i in out if i["quoteCoin"] == QUOTE]
    return [i["symbol"] for i in out]


def fetch_klines(base, sym, tf, limit):
    url = f"{base}/v5/market/kline?category=linear&symbol={sym}&interval={tf}&limit={limit}"
    j = http_get_json(url)
    if j.get("retCode") != 0:
        raise RuntimeError(j.get("retMsg"))
    rows = j["result"]["list"]
    out = [{"t": int(a[0]), "open": float(a[1]), "high": float(a[2]),
            "low": float(a[3]), "close": float(a[4])} for a in rows]
    out.reverse()
    return out


# ===================== منطق اصلی (همون رد/گرین + توالی) =====================
def detect(c0, c1, c2):
    is_red = (c0["close"] > c0["open"] and c2["close"] < c2["open"] and
              c1["high"] > c0["high"] and c1["high"] > c2["high"] and
              c1["low"] > c0["low"] and c2["close"] < c0["low"])
    is_green = (c0["close"] < c0["open"] and c2["close"] > c2["open"] and
                c1["low"] < c0["low"] and c1["low"] < c2["low"] and
                c1["high"] < c0["high"] and c2["close"] > c0["high"])
    return is_red, is_green


def fresh_state():
    return {"lastTime": 0, "buf": [], "price": None,
            "sellStage": 0, "sellP1": None, "sellP2": None,
            "buyStage": 0, "buyP1": None, "buyP2": None,
            "lastSignal": None}


def step(st, is_red, is_green, close):
    ev = []
    if is_green:
        st["sellStage"], st["sellP1"], st["sellP2"] = 1, None, None
    elif is_red:
        if st["sellStage"] == 1:
            st["sellP1"], st["sellStage"] = close, 2
        elif st["sellStage"] == 2:
            if close < st["sellP1"]:
                st["sellP2"], st["sellStage"] = close, 3
            else:
                st["sellStage"] = 0
        elif st["sellStage"] == 3:
            if close > st["sellP1"] and close > st["sellP2"]:
                ev.append("SELL")
            st["sellStage"] = 0

    if is_red:
        st["buyStage"], st["buyP1"], st["buyP2"] = 1, None, None
    elif is_green:
        if st["buyStage"] == 1:
            st["buyP1"], st["buyStage"] = close, 2
        elif st["buyStage"] == 2:
            if close > st["buyP1"]:
                st["buyP2"], st["buyStage"] = close, 3
            else:
                st["buyStage"] = 0
        elif st["buyStage"] == 3:
            if close < st["buyP1"] and close < st["buyP2"]:
                ev.append("BUY")
            st["buyStage"] = 0
    return ev


def process_candle(st, c):
    st["buf"].append(c)
    if len(st["buf"]) > 3:
        st["buf"].pop(0)
    st["lastTime"], st["price"] = c["t"], c["close"]
    if len(st["buf"]) < 3:
        return []
    is_red, is_green = detect(st["buf"][0], st["buf"][1], st["buf"][2])
    ev = step(st, is_red, is_green, c["close"])
    for e in ev:
        st["lastSignal"] = {"side": e, "time": c["t"], "price": c["close"]}
    return ev
# ============================================================================


def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": CHAT_ID, "text": text}).encode()
    req = urllib.request.Request(url, data=data)
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        print("خطا در ارسال تلگرام:", e)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def scan_one(base, sym, tf, state, now_ms, alerts):
    key = f"{sym}|{tf}"
    st = state.get(key) or fresh_state()
    initial = st["lastTime"] == 0
    tf_ms = TF_MS[tf]
    need = 200 if initial else min(200, int((now_ms - st["lastTime"]) / tf_ms) + 2)
    try:
        candles = fetch_klines(base, sym, tf, need)
    except Exception as e:
        return
    candles = [c for c in candles if c["t"] + tf_ms <= now_ms and c["t"] > st["lastTime"]]
    for c in candles:
        ev = process_candle(st, c)
        recent = (not initial) and c["t"] >= now_ms - FRESH_WINDOW_MULT * tf_ms
        if recent:
            for e in ev:
                alerts.append(f"سیگنال {e}\n{sym} — تایم‌فریم {TF_LABEL[tf]}\nقیمت: {c['close']}")
    state[key] = st


def main():
    base = pick_base()
    symbols = load_symbols(base)
    print(f"{len(symbols)} نماد | تایم‌فریم‌ها: {TIMEFRAMES}")
    state = load_state()
    now_ms = int(time.time() * 1000)
    alerts = []

    tasks = [(sym, tf) for sym in symbols for tf in TIMEFRAMES]
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(scan_one, base, sym, tf, state, now_ms, alerts) for sym, tf in tasks]
        for _ in as_completed(futs):
            pass

    save_state(state)
    print(f"{len(alerts)} سیگنال جدید")
    for a in alerts:
        send_telegram(a)
        time.sleep(0.3)  # رعایت محدودیت تلگرام (حداکثر حدود ۳۰ پیام در ثانیه، ولی محتاط‌تر بهتره)


if __name__ == "__main__":
    main()
