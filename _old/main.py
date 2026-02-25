# SCAN v2.4 (Prime universe + 2-stage filter + ranking score + 1H confirm)
import yfinance as yf
import pandas as pd
import numpy as np
import ta
import sys
import time
import random

print("=== SCAN v2.4 (4h pullback scanner, Prime + 2-stage + score + 1H confirm) ===", flush=True)

# -----------------------------
# UNIVERSE
# -----------------------------
UNIVERSE_XLSX = "tse_listed_issues.xlsx"  # JPXのファイル（列: Local Code / Section/Products）

# -----------------------------
# DATA SETTINGS
# -----------------------------
PERIOD_1H = "30d"
INTERVAL_1H = "60m"
RESAMPLE = "4h"

# -----------------------------
# STRATEGY PARAMS
# -----------------------------
ATR_MULT = 1.2
RR = 1.8

START_EQUITY = 1_000_000
RISK_PCT = 0.005
MAX_POSITIONS = 4

LOOKBACK_BARS = 4         # 押し目の鮮度（4h×4=16h）
MAX_EXTEND = 0.02         # シグナルEMA20から上で掴み禁止（+2%）
NOW_MIN_DIST = 0.00       # EMA20以上だけ
NOW_MAX_DIST = 0.01       # EMA20から+1%以内

USE_MARKET_TREND_FILTER = True

# 取れないときの地合いフォールバック
# - "skip": 地合いフィルタをスキップして続行（True扱い）
# - "deny": 取れない日は見送り（False扱い）
MARKET_FALLBACK = "skip"

# -----------------------------
# STAGE A: DAILY PREFILTER PARAMS
# -----------------------------
DAILY_PERIOD = "1y"
MIN_AVG_VALUE_20D = 500_000_000  # 売買代金(概算) 20日平均 5億円以上
MIN_PRICE = 200
MAX_PRICE = 20000

# 押し目レンジ（20日高値から -1%〜-12%）
DD_FROM_HH_MIN = -0.12
DD_FROM_HH_MAX = -0.01

# -----------------------------
# STAGE B: RATE LIMIT AVOID
# -----------------------------
B_SLEEP_MIN = 0.15
B_SLEEP_MAX = 0.45
B_RETRIES = 3

# -----------------------------
# STAGE B: 1H CONFIRM (recommended)
# -----------------------------
USE_1H_CONFIRM = True
VOL_MULT_1H = 1.2     # 最新1h出来高が20本平均の1.2倍以上
MIN_1H_CANDLE_PCT = 0 # 最新1hが陽線（Close>Open）を要求


def load_prime_universe_from_jpx_xlsx(path: str) -> list[str]:
    """
    JPXの一覧Excel（あなたの列構成）から Prime の銘柄コードを抽出して '####.T' にする
    columns: Local Code, Section/Products
    """
    uni = pd.read_excel(path)

    code_col = "Local Code"
    seg_col = "Section/Products"

    if code_col not in uni.columns:
        raise ValueError(f"銘柄コード列 '{code_col}' が見つかりません。")
    if seg_col not in uni.columns:
        raise ValueError(f"市場区分列 '{seg_col}' が見つかりません。")

    seg = uni[seg_col].astype(str)
    df = uni[seg.str.contains("Prime|プライム", na=False)].copy()

    codes = (
        df[code_col]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
        .dropna()
        .unique()
        .tolist()
    )

    tickers = sorted({f"{c}.T" for c in codes})
    return tickers


def resample_4h(df: pd.DataFrame) -> pd.DataFrame:
    o = df["Open"].resample(RESAMPLE).first()
    h = df["High"].resample(RESAMPLE).max()
    l = df["Low"].resample(RESAMPLE).min()
    c = df["Close"].resample(RESAMPLE).last()
    v = df["Volume"].resample(RESAMPLE).sum()
    out = pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c, "Volume": v}).dropna()
    out = out[out["Volume"] > 0]
    return out


def market_ok() -> bool:
    """
    日経平均(^N225)で地合い判定。
    レート制限で取れない時は落ちない＆フォールバック。
    """
    retries = 4
    wait_sec = 2

    nikkei = None
    for attempt in range(1, retries + 1):
        try:
            nikkei = yf.download("^N225", period="2y", interval="1d", progress=False, threads=False)
            if nikkei is not None:
                nikkei = nikkei.dropna()
            if nikkei is not None and len(nikkei) > 0:
                break
        except Exception as e:
            print(f"[MARKET][WARN] download failed attempt {attempt}/{retries}: {e}", flush=True)

        time.sleep(wait_sec)
        wait_sec *= 2

    if nikkei is None or len(nikkei) == 0:
        print("[MARKET][WARN] ^N225 unavailable (rate limit / network).", flush=True)
        if MARKET_FALLBACK == "skip":
            print("[MARKET] fallback=skip -> treat as OK", flush=True)
            return True
        else:
            print("[MARKET] fallback=deny -> treat as NG", flush=True)
            return False

    if isinstance(nikkei.columns, pd.MultiIndex):
        nikkei.columns = nikkei.columns.get_level_values(0)

    nikkei["SMA50"] = nikkei["Close"].rolling(50).mean()
    nikkei["SMA200"] = nikkei["Close"].rolling(200).mean()
    nikkei["SMA200_20"] = nikkei["SMA200"].shift(20)
    nikkei = nikkei.dropna()

    if len(nikkei) == 0:
        print("[MARKET][WARN] ^N225 data too short after indicators.", flush=True)
        return False

    last = nikkei.iloc[-1]
    close = float(last["Close"])
    sma50 = float(last["SMA50"])
    sma200 = float(last["SMA200"])
    sma200_20 = float(last["SMA200_20"])

    ok = (close > sma200) and (sma50 > sma200) and (sma200 > sma200_20)
    print(f"[MARKET] close={close:.1f} sma50={sma50:.1f} sma200={sma200:.1f} sma200_20={sma200_20:.1f} -> ok={ok}")
    return ok


def prefilter_daily(tickers: list[str]) -> list[str]:
    """
    日足で「強くて流動性ある＋押し目中」銘柄に絞る（高速チャンク版）
    - 売買代金(概算) 20日平均
    - トレンド（Close>SMA200、SMA50>SMA200）
    - 20日高値からの押し目レンジ
    """

    def _normalize_single(df):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df

    chunk_size = 200
    keep = []
    total = len(tickers)

    print(f"[STAGE A] Daily prefilter: {total} tickers (chunk={chunk_size})")

    for start_i in range(0, total, chunk_size):
        chunk = tickers[start_i:start_i + chunk_size]
        print(f"[STAGE A] Downloading {start_i+1}-{min(start_i+chunk_size, total)} / {total}", flush=True)

        try:
            data = yf.download(
                chunk,
                period=DAILY_PERIOD,
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                progress=False,
                threads=True,
            )
        except Exception as e:
            print(f"[STAGE A][WARN] chunk download failed: {e}", flush=True)
            continue

        for t in chunk:
            try:
                # MultiIndex対応
                if isinstance(data.columns, pd.MultiIndex) and t in data.columns.get_level_values(0):
                    d = data[t].dropna()
                elif isinstance(data.columns, pd.MultiIndex) and t in data.columns.get_level_values(1):
                    d = data.xs(t, level=1, axis=1).dropna()
                else:
                    d = _normalize_single(data).dropna()

                if d is None or len(d) < 210:
                    continue

                if isinstance(d.columns, pd.MultiIndex):
                    d.columns = d.columns.get_level_values(0)

                close = d["Close"]
                vol = d["Volume"]

                price = float(close.iloc[-1])
                if not (MIN_PRICE <= price <= MAX_PRICE):
                    continue

                sma50 = close.rolling(50).mean().iloc[-1]
                sma200 = close.rolling(200).mean().iloc[-1]
                if np.isnan(sma50) or np.isnan(sma200):
                    continue

                if not (price > sma200 and sma50 > sma200):
                    continue

                value = close * vol
                avg_value_20 = value.rolling(20).mean().iloc[-1]
                if np.isnan(avg_value_20) or avg_value_20 < MIN_AVG_VALUE_20D:
                    continue

                hh20 = close.rolling(20).max().iloc[-1]
                dd_from_hh = (price / hh20) - 1.0
                if not (DD_FROM_HH_MIN <= dd_from_hh <= DD_FROM_HH_MAX):
                    continue

                keep.append(t)

            except Exception:
                continue

    keep = sorted(set(keep))
    return keep


# -----------------------------
# RUN
# -----------------------------
print("=== LOAD UNIVERSE (TSE Prime) ===")
try:
    TICKERS = load_prime_universe_from_jpx_xlsx(UNIVERSE_XLSX)
except Exception as e:
    print(f"[ERROR] ユニバース読込失敗: {e}")
    print("JPXのExcelを 'tse_listed_issues.xlsx' で保存してから再実行してね。")
    input()
    sys.exit()

print("Prime universe tickers:", len(TICKERS))

print("=== RUN SCAN ===")
print(
    f"[PARAM] NOW_MIN_DIST={NOW_MIN_DIST} NOW_MAX_DIST={NOW_MAX_DIST} LOOKBACK_BARS={LOOKBACK_BARS} MAX_EXTEND={MAX_EXTEND} "
    f"| 1H_CONFIRM={USE_1H_CONFIRM} VOL_MULT_1H={VOL_MULT_1H}",
    flush=True,
)

if USE_MARKET_TREND_FILTER:
    if not market_ok():
        print("地合いNG → 本日はエントリー見送り")
        input()
        sys.exit()
    print("地合いOK → スキャン開始")
else:
    print("地合いフィルタOFF → スキャン開始")

print("=== STAGE A: DAILY PREFILTER ===")
TICKERS = prefilter_daily(TICKERS)
print("prefiltered tickers:", len(TICKERS))

candidates = []
sig_hits_total = 0

print("=== STAGE B: 1H -> 4H PULLBACK SCAN (entry=1H, confirm=1H) ===")
failed = 0
processed = 0

for idx, ticker in enumerate(TICKERS, start=1):
    if idx % 20 == 0 or idx == 1:
        print(f"[STAGE B] {idx}/{len(TICKERS)} processed={processed} failed={failed}", flush=True)

    df = None
    for attempt in range(1, B_RETRIES + 1):
        try:
            df = yf.download(
                ticker,
                period=PERIOD_1H,
                interval=INTERVAL_1H,
                progress=False,
                threads=False,
            )
            if df is not None and len(df) >= 80:
                break
        except Exception as e:
            print(f"[STAGE B][WARN] {ticker} download failed attempt {attempt}/{B_RETRIES}: {e}", flush=True)

        time.sleep(0.8 * attempt)

    if df is None or len(df) < 80:
        failed += 1
        time.sleep(random.uniform(B_SLEEP_MIN, B_SLEEP_MAX))
        continue

    processed += 1

    df = df.dropna()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.index = pd.to_datetime(df.index)

    # 4hで構造（EMA/ATR/押し目反発）
    df4 = resample_4h(df)
    if len(df4) < 40:
        time.sleep(random.uniform(B_SLEEP_MIN, B_SLEEP_MAX))
        continue

    df4["EMA20"] = df4["Close"].ewm(span=20, adjust=False).mean()
    df4["ATR"] = ta.volatility.AverageTrueRange(
        high=df4["High"], low=df4["Low"], close=df4["Close"], window=14
    ).average_true_range()

    # -----------------------------
    # entry：最新1h Close（B案）
    # -----------------------------
    entry = float(df.iloc[-1]["Close"])  # 最新1h終値
    ema_now = float(df4.iloc[-1]["EMA20"])  # 4h EMA20
    dist_now = (entry / ema_now) - 1.0

    # 今の強さフィルタ（4h EMA20基準）
    if not (NOW_MIN_DIST <= dist_now <= NOW_MAX_DIST):
        time.sleep(random.uniform(B_SLEEP_MIN, B_SLEEP_MAX))
        continue

    # -----------------------------
    # 1H confirm（B案②）
    # -----------------------------
    last_1h_open = float(df.iloc[-1]["Open"])
    last_1h_close = float(df.iloc[-1]["Close"])
    candle1h_pct = (last_1h_close / last_1h_open - 1.0) * 100.0

    # 出来高増（20本平均比）
    vol1h = float(df.iloc[-1]["Volume"])
    vol1h_ma20 = float(df["Volume"].rolling(20).mean().iloc[-1])

    if USE_1H_CONFIRM:
        if candle1h_pct <= MIN_1H_CANDLE_PCT:
            time.sleep(random.uniform(B_SLEEP_MIN, B_SLEEP_MAX))
            continue
        if np.isnan(vol1h_ma20) or vol1h_ma20 <= 0:
            time.sleep(random.uniform(B_SLEEP_MIN, B_SLEEP_MAX))
            continue
        if vol1h < vol1h_ma20 * VOL_MULT_1H:
            time.sleep(random.uniform(B_SLEEP_MIN, B_SLEEP_MAX))
            continue

    # 押し目反発の足を探す（直近から）
    start = max(0, len(df4) - LOOKBACK_BARS)
    best_i = None

    for i in range(len(df4) - 1, start - 1, -1):
        sig = df4.iloc[i]
        if np.isnan(sig["ATR"]) or np.isnan(sig["EMA20"]):
            continue

        if (sig["Low"] <= sig["EMA20"]) and (sig["Close"] > sig["EMA20"]):
            sig_hits_total += 1
            # 上で掴み禁止（シグナル足EMA20から+MAX_EXTEND以内）
            if entry <= float(sig["EMA20"]) * (1.0 + MAX_EXTEND):
                best_i = i
                break

    if best_i is None:
        time.sleep(random.uniform(B_SLEEP_MIN, B_SLEEP_MAX))
        continue

    sig = df4.iloc[best_i]
    atr = float(sig["ATR"])

    # SL/TP：エントリー（1h）基準でATRを当てる
    sl = entry - ATR_MULT * atr
    risk_per_share = entry - sl
    if risk_per_share <= 0:
        time.sleep(random.uniform(B_SLEEP_MIN, B_SLEEP_MAX))
        continue

    tp = entry + RR * risk_per_share
    shares = int((START_EQUITY * RISK_PCT) / risk_per_share)
    if shares <= 0:
        time.sleep(random.uniform(B_SLEEP_MIN, B_SLEEP_MAX))
        continue

    # -----------------------------
    # RANKING FEATURES（初動特化 + signal quality）
    # -----------------------------
    atr_now = float(df4.iloc[-1]["ATR"])
    atrp = (atr_now / entry) * 100.0

    bars_ago = (len(df4) - 1) - best_i

    rebound_pct = (float(sig["Close"]) / float(sig["EMA20"]) - 1.0) * 100.0
    touch_depth_pct = (float(sig["EMA20"]) / float(sig["Low"]) - 1.0) * 100.0

    # スコア：押し目近さ + 直近の勢い(1h) + 出来高(1h) + 新しさ(控えめ) + 反発の質 - ボラのズレ - 深すぎる刺さり
    vol_boost = np.log1p(vol1h / vol1h_ma20) if vol1h_ma20 > 0 else 0.0

    score = (
        (2.0 - abs(dist_now * 100.0)) +
        (candle1h_pct * 1.4) +
        (vol_boost * 1.2) +
        (max(0, 4 - bars_ago) * 0.25) +
        (rebound_pct * 2.0) -
        (abs(atrp - 2.5) * 0.2) -
        (max(0, touch_depth_pct - 1.2) * 0.8)
    )

    # sig_bar JST表示（4hのシグナル足）
    ts = df4.index[best_i]
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    ts_jst = ts.tz_convert("Asia/Tokyo")

    # entry_bar（最新1h足）の時刻もJSTで残す（後場エントリー運用で便利）
    ts_e = df.index[-1]
    if ts_e.tzinfo is None:
        ts_e = ts_e.tz_localize("UTC")
    ts_e_jst = ts_e.tz_convert("Asia/Tokyo")

    candidates.append({
        "ticker": ticker,
        "entry": round(entry, 2),
        "sl": round(sl, 2),
        "tp": round(tp, 2),
        "shares": shares,
        "dist_to_ema20": round(dist_now * 100, 2),
        "rr": round((tp - entry) / (entry - sl), 2),
        "sig_bar": ts_jst.strftime("%Y-%m-%d %H:%M"),
        "entry_bar": ts_e_jst.strftime("%Y-%m-%d %H:%M"),
        "bars_ago": int(bars_ago),
        # 1h confirm
        "candle_1h_pct": round(candle1h_pct, 2),
        "vol_1h": int(vol1h),
        "vol_1h_ma20": int(vol1h_ma20),
        # quality
        "atrp": round(atrp, 2),
        "rebound_pct": round(rebound_pct, 2),
        "touch_depth_pct": round(touch_depth_pct, 2),
        "score": round(score, 3),
    })

    time.sleep(random.uniform(B_SLEEP_MIN, B_SLEEP_MAX))

print(f"[STAGE B DONE] processed={processed} failed={failed}", flush=True)

# scoreが高い順（上位が“良い形”になりやすい）
candidates = sorted(candidates, key=lambda x: x["score"], reverse=True)

print("\n=== TODAY CANDIDATES (top) ===")
if not candidates:
    print("シグナルなし（条件を満たすものなし）")
else:
    for c in candidates[:MAX_POSITIONS]:
        print(c)

    top20 = [c["ticker"] for c in candidates[:20]]
    print("\n[top20 tickers]", top20)

print("\n=== STATS ===")
print("universe_prime: loaded")
print("prefiltered:", len(TICKERS))
print("candidates_found:", len(candidates))
print("sig_hits_total:", sig_hits_total)

# --- save result ---
if candidates:
    pd.DataFrame(candidates).to_csv("candidates_today.csv", index=False, encoding="utf-8-sig")
    print("[SAVED] candidates_today.csv")

print("\n=== END ===")
input()
