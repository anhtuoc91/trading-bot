"""
╔══════════════════════════════════════════════════════════════════╗
║       DUAL ENGINE v5.0 - ALERT ONLY (Không Trade Thật)         ║
║                                                                  ║
║  Phiên bản này CHỈ gửi tín hiệu qua Discord để trade thủ công  ║
║  - KHÔNG đặt lệnh lên Binance                                   ║
║  - KHÔNG cần API key có quyền trade                             ║
║  - Vẫn đọc giá + tính indicators từ Binance                    ║
║  - Gửi Discord đầy đủ: Entry, SL, TP, ATR, Mode               ║
║  - Gửi thêm thông báo khi nên thoát lệnh (EXIT ALERT)          ║
╚══════════════════════════════════════════════════════════════════╝
"""

import time
import threading
import datetime as _dt
import urllib.request
import json
import ccxt

# ============================================================
# BINANCE API CREDENTIALS
# (Chỉ cần READ permission để lấy giá — không cần Trade)
# ============================================================
BINANCE_API_KEY    = "g1lOkGNiKui71zVMTD2fRoQZXVDE5lzU03EL7jdO8C373Fk0zf1vDZoPvhZvYVmw"
BINANCE_API_SECRET = "TgSNPLYeHTiLddleEwjheZmJj2KCWrGCw66YRXMgUtOOdp8akr84PGGU4kj1eXz3"

# ============================================================
# SYMBOLS
# ============================================================
SYMBOLS_CRYPTO = ["BTCUSDT", "ETHUSDT", "XRPUSDT", "DOTUSDT", "BNBUSDT"]
SYMBOLS_FULL   = ["BTCUSDT", "ETHUSDT", "XAUUSDT", "SOLUSDT", "XRPUSDT", "DOTUSDT", "BNBUSDT"]
ACTIVE_SYMBOLS = SYMBOLS_CRYPTO

# ============================================================
# CONFIG
# ============================================================
TIMEFRAME        = "4h"
SCAN_INTERVAL    = 300.0
MONITOR_INTERVAL = 60.0

EMA_FAST   = 5
EMA_SLOW   = 13
RSI_PERIOD = 14
ADX_PERIOD = 14
ADX_TREND  = 25

# ── BUY with-trend ──────────────────────────────────────────
BUY_SL_ATR          = 2.5
BUY_TP_ATR          = 0        # KHÔNG TP — thoát bằng 15m signal
BUY_MAX_HOLD_SEC    = 259200   # 3 ngày fallback
BUY_EXIT_ON_15M_REV = True

# ── Adaptive 15m exit ───────────────────────────────────────
EXIT_15M_LOOSE_BELOW    = 0.30
EXIT_15M_MID_BELOW      = 0.80
EXIT_15M_TIGHT_BELOW    = 1.50
EXIT_15M_RSI_OB         = 72
EXIT_15M_SLOPE_WEAK     = 0.01
EXIT_15M_SLOPE_TURN     = 0.00
EXIT_15M_TRAIL_SLOPE    = -0.02
EXIT_15M_MIN_HOLD_LOOSE = 900
EXIT_15M_MIN_HOLD_OTHER = 300

# ── BUY counter-trend ───────────────────────────────────────
BUY_COUNTER_SL_ATR         = 1.2
BUY_COUNTER_TP_ATR         = 1.5
BUY_COUNTER_TRAIL_ACTIVATE = 20.0
BUY_COUNTER_TRAIL_ATR      = 0.6
BUY_COUNTER_MAX_HOLD_SEC   = 28800

# ── SELL ────────────────────────────────────────────────────
SELL_SL_ATR         = 0.7
SELL_TP_ATR         = 2.2
SELL_TRAIL_ACTIVATE = 50.0
SELL_TRAIL_ATR      = 1.0
SELL_MAX_HOLD_SEC   = 86400

# ── SELL counter-trend ──────────────────────────────────────
SELL_COUNTER_SL_ATR         = 1.2
SELL_COUNTER_TP_ATR         = 1.5
SELL_COUNTER_TRAIL_ACTIVATE = 20.0
SELL_COUNTER_TRAIL_ATR      = 0.4
SELL_COUNTER_MAX_HOLD_SEC   = 14400

# ── LOT (chỉ để hiển thị gợi ý size trong Discord) ──────────
BASE_USDT = {
    "BTCUSDT": 10.0,
    "ETHUSDT": 10.0,
    "XAUUSDT": 10.0,
    "SOLUSDT": 10.0,
    "XRPUSDT": 10.0,
    "DOTUSDT": 10.0,
    "BNBUSDT": 10.0,
}
USDT_DEFAULT = 10.0
SELL_LOT_PCT = 0.5
LEVERAGE     = 10

# ── Cross dedup ──────────────────────────────────────────────
CROSS_COOLDOWN_SEC = 0

# ── Discord ──────────────────────────────────────────────────
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1454428772970270803/DD3f3qpwH2mLMC-vMiK7xxdaxqIccOHu_COIIxywoPCMQd6ywCY4hlbFXmgFfA93CzKO"

# ── Log ─────────────────────────────────────────────────────
LOG_FILE = "dual_alert_log.txt"

# ============================================================
# GLOBAL STATE
# (Dùng để theo dõi tín hiệu đang "ảo" — không phải lệnh thật)
# ============================================================
_lock       = threading.Lock()
_log_lock   = threading.Lock()
_state_lock = threading.Lock()

# _alerts: lưu các signal đã gửi để theo dõi EXIT
# {alert_id: {symbol, side, direction, entry_price, entry_time, sl, tp, atr, mode_tag, is_counter}}
_alerts    = {}
_last_cross = {}  # {(symbol, side): timestamp}

_15m_cache      = {}
_15m_cache_time = {}
_15m_cache_lock = threading.Lock()
_15M_CACHE_TTL  = 60

_exchange = None

# ============================================================
# BINANCE CLIENT (read-only)
# ============================================================
def get_exchange():
    global _exchange
    if _exchange is None:
        _exchange = ccxt.binance({
            "apiKey":    BINANCE_API_KEY,
            "secret":    BINANCE_API_SECRET,
            "options":   {"defaultType": "future"},
            "enableRateLimit": True,
        })
    return _exchange

def _safe_call(fn, *args, retries=3, **kwargs):
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except ccxt.NetworkError as e:
            _log(f"[NET] retry {i+1}: {e}")
            time.sleep(1.5 ** i)
        except ccxt.RateLimitExceeded:
            time.sleep(2)
        except Exception as e:
            _log(f"[ERR] {fn.__name__}: {e}")
            return None
    return None

# ============================================================
# LOGGING
# ============================================================
def _log(line):
    ts = _dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {line}")

def _log_alert(symbol, side, direction, entry, exit_price, reason, hold_sec):
    try:
        pnl    = ((exit_price - entry) if direction == "LONG" else (entry - exit_price)) / entry * 100
        result = "WIN" if pnl > 0 else "LOSS"
        now    = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line   = (f"{now} | {symbol:<8} | {side:<4} | {direction:<5} | "
                  f"entry={entry:.4f} | exit={exit_price:.4f} | pnl={pnl:+.2f}% | "
                  f"reason={reason:<30} | hold={hold_sec:.0f}s | {result}\n")
        with _log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line)
        _log(f"[ALERT-LOG] {line.strip()}")
    except Exception as e:
        _log(f"[LOG] error: {e}")

# ============================================================
# DATA
# ============================================================
TF_MAP = {
    "1m": "1m", "3m": "3m", "5m": "5m", "15m": "15m",
    "1h": "1h", "4h": "4h", "1d": "1d",
}

def fetch_klines(symbol, tf="4h", limit=150):
    ex = get_exchange()
    data = _safe_call(ex.fetch_ohlcv, symbol, TF_MAP.get(tf, tf), limit=limit)
    return data

def get_price(symbol):
    ex = get_exchange()
    ticker = _safe_call(ex.fetch_ticker, symbol)
    return ticker["last"] if ticker else None

def _tick_to_decimals(v, default=4):
    """Chuyển tick size (0.0001) hoặc số nguyên (4) -> số chữ số thập phân."""
    try:
        v = float(v)
        if v >= 1:
            return max(int(v), 0)
        s = f"{v:.10f}".rstrip("0")
        if "." in s:
            return max(len(s.split(".")[1]), 0)
    except:
        pass
    return default

def get_precision(symbol):
    ex = get_exchange()
    try:
        markets = ex.load_markets()
        mkt = markets.get(symbol) or markets.get(symbol.replace("USDT", "/USDT"))
        if mkt:
            price_prec = _tick_to_decimals(mkt.get("precision", {}).get("price", 4), default=4)
            qty_prec   = _tick_to_decimals(mkt.get("precision", {}).get("amount", 3), default=3)
            return price_prec, qty_prec
    except:
        pass
    return 4, 3

def usdt_to_qty(symbol, usdt_amount, price):
    _, qty_prec = get_precision(symbol)
    notional = usdt_amount * LEVERAGE
    qty = notional / price
    return round(qty, qty_prec)

# ============================================================
# INDICATORS
# ============================================================
def _ema(data, period):
    if len(data) < period:
        return None
    k   = 2.0 / (period + 1)
    ema = sum(data[:period]) / period
    for p in data[period:]:
        ema = p * k + ema * (1 - k)
    return ema

def calc_indicators(symbol, tf=None):
    try:
        klines = fetch_klines(symbol, tf or TIMEFRAME, 150)
        if not klines or len(klines) < 50:
            return None
        closes = [float(k[4]) for k in klines]
        highs  = [float(k[2]) for k in klines]
        lows   = [float(k[3]) for k in klines]

        ef_now  = _ema(closes,      EMA_FAST)
        es_now  = _ema(closes,      EMA_SLOW)
        ef_prev = _ema(closes[:-1], EMA_FAST)
        es_prev = _ema(closes[:-1], EMA_SLOW)
        if None in (ef_now, es_now, ef_prev, es_prev):
            return None

        gains  = [max(closes[i] - closes[i-1], 0) for i in range(1, len(closes))]
        losses = [max(closes[i-1] - closes[i], 0) for i in range(1, len(closes))]
        ag = sum(gains[-RSI_PERIOD:])  / RSI_PERIOD
        al = sum(losses[-RSI_PERIOD:]) / RSI_PERIOD
        rsi = 100 - (100 / (1 + ag / al)) if al > 0 else 100

        trs = [max(highs[i] - lows[i],
                   abs(highs[i] - closes[i-1]),
                   abs(lows[i]  - closes[i-1])) for i in range(1, len(closes))]
        atr = sum(trs[-14:]) / 14

        adx = 25.0
        try:
            n = ADX_PERIOD
            if len(highs) >= n * 2:
                pdm = [max(highs[i] - highs[i-1], 0) if (highs[i] - highs[i-1]) > (lows[i-1] - lows[i]) else 0
                       for i in range(1, len(highs))]
                mdm = [max(lows[i-1] - lows[i], 0) if (lows[i-1] - lows[i]) > (highs[i] - highs[i-1]) else 0
                       for i in range(1, len(lows))]
                def ws(d, p):
                    r = sum(d[:p])
                    for v in d[p:]: r = r - r / p + v
                    return r
                tr14 = ws(trs[-n*2:], n)
                p14  = ws(pdm[-n*2:], n)
                m14  = ws(mdm[-n*2:], n)
                pdi  = 100 * p14 / tr14 if tr14 > 0 else 0
                mdi  = 100 * m14 / tr14 if tr14 > 0 else 0
                adx  = 100 * abs(pdi - mdi) / (pdi + mdi) if (pdi + mdi) > 0 else 25.0
        except:
            adx = 25.0

        ef_2ago    = _ema(closes[:-2], EMA_FAST)
        slope_now  = (ef_now  - ef_prev) / ef_prev * 100 if ef_prev and ef_prev > 0 else 0
        slope_prev = (ef_prev - ef_2ago) / ef_2ago * 100 if ef_2ago and ef_2ago > 0 else 0

        return {
            "ef_now": ef_now, "es_now": es_now,
            "ef_prev": ef_prev, "es_prev": es_prev,
            "rsi": rsi, "atr": atr, "adx": adx,
            "close": closes[-1], "high": highs[-1], "low": lows[-1],
            "slope_now": slope_now, "slope_prev": slope_prev,
            "cross_up":   ef_prev <= es_prev and ef_now > es_now,
            "cross_down": ef_prev >= es_prev and ef_now < es_now,
            "is_trending": adx >= ADX_TREND,
        }
    except Exception as e:
        _log(f"[IND] {symbol}: {e}")
        return None

def get_15m_regime(symbol):
    now = time.time()
    with _15m_cache_lock:
        if symbol in _15m_cache_time and now - _15m_cache_time[symbol] < _15M_CACHE_TTL:
            return _15m_cache[symbol]

    try:
        ind = calc_indicators(symbol, "15m")
        if not ind:
            result = ("RANGING", 25.0, False)
        else:
            cross_down_15m     = ind["cross_down"]
            slope_now          = ind["slope_now"]
            slope_prev         = ind["slope_prev"]
            trend_exhausted    = cross_down_15m
            slope_turning_down = slope_prev > 0 and slope_now <= 0 and ind["ef_now"] > ind["es_now"]
            if slope_turning_down:
                trend_exhausted = True

            if ind["adx"] >= ADX_TREND:
                regime = "TRENDING_UP" if ind["ef_now"] > ind["es_now"] else "TRENDING_DOWN"
                result = (regime, ind["adx"], trend_exhausted)
            else:
                result = ("RANGING", ind["adx"], trend_exhausted)

        with _15m_cache_lock:
            _15m_cache[symbol]      = result
            _15m_cache_time[symbol] = now
        return result
    except:
        return "RANGING", 25.0, False

def should_exit_15m(pos, price, hold_sec):
    symbol  = pos["symbol"]
    entry   = pos["entry"]
    pnl_pct = (price - entry) / entry * 100

    ind15 = calc_indicators(symbol, "15m")
    if not ind15:
        return False, ""

    es15     = ind15["es_now"]
    rsi15    = ind15["rsi"]
    slope15  = ind15["slope_now"]
    cross_dn = ind15["cross_down"]

    if pnl_pct < EXIT_15M_LOOSE_BELOW:
        if hold_sec < EXIT_15M_MIN_HOLD_LOOSE:
            return False, ""
        if cross_dn:
            return True, f"15m_cross_dn_loose pnl={pnl_pct:+.2f}%"
        return False, ""

    if pnl_pct < EXIT_15M_MID_BELOW:
        if hold_sec < EXIT_15M_MIN_HOLD_OTHER:
            return False, ""
        if cross_dn:
            return True, f"15m_cross_dn_mid pnl={pnl_pct:+.2f}%"
        if rsi15 > EXIT_15M_RSI_OB and slope15 < EXIT_15M_SLOPE_WEAK:
            return True, f"15m_rsi_slope_mid pnl={pnl_pct:+.2f}% RSI={rsi15:.1f}"
        return False, ""

    if pnl_pct < EXIT_15M_TIGHT_BELOW:
        if cross_dn:
            return True, f"15m_cross_dn_tight pnl={pnl_pct:+.2f}%"
        if slope15 <= EXIT_15M_SLOPE_TURN:
            return True, f"15m_slope_turn pnl={pnl_pct:+.2f}%"
        return False, ""

    if price < es15:
        return True, f"15m_trail_ema pnl={pnl_pct:+.2f}%"
    if slope15 < EXIT_15M_TRAIL_SLOPE:
        return True, f"15m_trail_slope pnl={pnl_pct:+.2f}%"
    return False, ""

# ============================================================
# SIGNAL
# ============================================================
def detect_signal(ind, symbol, side):
    if not ind:
        return False

    direction  = "LONG" if side == "BUY" else "SHORT"
    rsi        = ind["rsi"]
    cross_up   = ind["cross_up"]
    cross_down = ind["cross_down"]
    slope_now  = ind["slope_now"]
    slope_prev = ind["slope_prev"]
    turn_up    = slope_prev <= 0 and slope_now > 0
    turn_down  = slope_prev >= 0 and slope_now < 0

    regime, adx, _ = get_15m_regime(symbol)
    htf_warn = ""
    if direction == "LONG"  and regime == "TRENDING_DOWN": htf_warn = "NGƯỢC 15m"
    if direction == "SHORT" and regime == "TRENDING_UP":   htf_warn = "NGƯỢC 15m"

    if direction == "LONG":
        if cross_up and rsi > 30:
            _log(f"[SIG] BUY  {symbol} CROSS_UP RSI={rsi:.1f} 15m:{regime} {htf_warn}")
            return True
        if turn_up and ind["ef_now"] > ind["es_now"] and rsi > 30:
            _log(f"[SIG] BUY  {symbol} SLOPE_UP RSI={rsi:.1f} 15m:{regime} {htf_warn}")
            return True

    if direction == "SHORT":
        if cross_down and rsi < 70:
            _log(f"[SIG] SELL {symbol} CROSS_DOWN RSI={rsi:.1f} 15m:{regime} {htf_warn}")
            return True
        if turn_down and ind["ef_now"] < ind["es_now"] and rsi < 70:
            _log(f"[SIG] SELL {symbol} SLOPE_DOWN RSI={rsi:.1f} 15m:{regime} {htf_warn}")
            return True

    return False

def get_signal_type(ind, side):
    """Trả về chuỗi mô tả loại signal để hiển thị trong Discord."""
    direction = "LONG" if side == "BUY" else "SHORT"
    if direction == "LONG":
        if ind.get("cross_up"):   return "EMA CROSS UP"
        if ind.get("slope_now", 0) > 0: return "SLOPE TURN UP"
    else:
        if ind.get("cross_down"): return "EMA CROSS DOWN"
        if ind.get("slope_now", 0) < 0: return "SLOPE TURN DOWN"
    return "SIGNAL"

# ============================================================
# LOT SUGGESTION
# ============================================================
def get_suggested_qty(symbol, side, price):
    usdt = BASE_USDT.get(symbol, USDT_DEFAULT)
    if side == "SELL":
        usdt = usdt * SELL_LOT_PCT
    return usdt_to_qty(symbol, usdt, price)

# ============================================================
# DISCORD — ENTRY ALERT
# ============================================================
def send_discord_entry(symbol, side, price, sl, tp, qty, mode_tag, atr, signal_type, rsi, adx, regime):
    """Gửi thông báo VÀO LỆNH qua Discord."""
    try:
        direction = "🟢 LONG (BUY)" if side == "BUY" else "🔴 SHORT (SELL)"
        tp_str    = f"{tp:.4f}" if tp else "none"
        sl_pct    = abs(price - sl) / price * 100 if sl else 0
        tp_pct    = abs(price - tp) / price * 100 if tp else 0
        color     = 3066993 if side == "BUY" else 15158332

        counter_warn = "⚠️ COUNTER-TREND" if mode_tag == "COUNTER" else "✅ WITH-TREND"

        # Tính RR
        if tp and sl:
            risk   = abs(price - sl)
            reward = abs(price - tp)
            rr     = f"1 : {reward/risk:.1f}" if risk > 0 else "—"
        else:
            rr = "—"

        embed = {
            "embeds": [{
                "title": f"📡 SIGNAL  {direction}  {symbol}",
                "description": (
                    f"**{counter_warn}  |  {signal_type}**\n"
                    f"Đây là tín hiệu thủ công — hãy tự vào lệnh nếu đồng ý!"
                ),
                "color": color,
                "fields": [
                    {"name": "🎯 Entry",      "value": f"`{price:.4f}`",                                    "inline": True},
                    {"name": "📦 Qty gợi ý", "value": f"`{qty}` (×{LEVERAGE}x lev)",                       "inline": True},
                    {"name": "💡 USDT",       "value": f"`{BASE_USDT.get(symbol, USDT_DEFAULT):.0f} USDT`", "inline": True},
                    {"name": "🛑 SL",         "value": f"`{sl:.4f}` ({sl_pct:.2f}%)",                       "inline": True},
                    {"name": "🎯 TP",         "value": f"`{tp_str}`" + (f" ({tp_pct:.2f}%)" if tp else ""), "inline": True},
                    {"name": "⚖️ R:R",        "value": f"`{rr}`",                                           "inline": True},
                    {"name": "📊 RSI",        "value": f"`{rsi:.1f}`",                                      "inline": True},
                    {"name": "📈 ADX",        "value": f"`{adx:.1f}`",                                      "inline": True},
                    {"name": "🔍 15m Regime", "value": f"`{regime}`",                                       "inline": True},
                    {"name": "📐 ATR",        "value": f"`{atr:.4f}`",                                      "inline": True},
                    {"name": "⏱️ Timeframe",  "value": f"`{TIMEFRAME}` + `15m`",                            "inline": True},
                    {"name": "🏷️ Mode",       "value": f"`{mode_tag}`",                                     "inline": True},
                ],
                "footer": {
                    "text": f"⚠️ Alert Only — Không tự trade | {_dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                }
            }]
        }
        _send_webhook(embed)
        _log(f"[DISCORD] Entry alert gửi: {side} {symbol} @{price:.4f}")
    except Exception as e:
        _log(f"[DISCORD] Lỗi entry alert: {e}")

# ============================================================
# DISCORD — EXIT ALERT
# ============================================================
def send_discord_exit(symbol, side, direction, entry, current_price, reason, hold_sec, atr, mode_tag):
    """Gửi thông báo THOÁT LỆNH qua Discord."""
    try:
        pnl_pct = ((current_price - entry) if direction == "LONG"
                   else (entry - current_price)) / entry * 100

        result_icon = "🟩 WIN" if pnl_pct > 0 else ("🟥 LOSS" if pnl_pct < 0 else "⬜ BREAK EVEN")
        color = 3066993 if pnl_pct > 0 else 15158332

        hold_str = f"{int(hold_sec // 3600)}h {int((hold_sec % 3600) // 60)}m"

        embed = {
            "embeds": [{
                "title": f"🚪 EXIT SIGNAL  {symbol}  [{side}]",
                "description": (
                    f"**{result_icon}**  |  Lý do: `{reason}`\n"
                    "Bot đề xuất đóng lệnh — hãy kiểm tra và thoát nếu đồng ý!"
                ),
                "color": color,
                "fields": [
                    {"name": "⬆️ Entry",       "value": f"`{entry:.4f}`",              "inline": True},
                    {"name": "📌 Price Now",   "value": f"`{current_price:.4f}`",       "inline": True},
                    {"name": "💹 PnL (ước)",   "value": f"`{pnl_pct:+.2f}%`",           "inline": True},
                    {"name": "⏱️ Giữ",         "value": f"`{hold_str}`",               "inline": True},
                    {"name": "🏷️ Mode",        "value": f"`{mode_tag}`",               "inline": True},
                    {"name": "📐 ATR",          "value": f"`{atr:.4f}`",               "inline": True},
                ],
                "footer": {
                    "text": f"⚠️ Alert Only | {_dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                }
            }]
        }
        _send_webhook(embed)
        _log(f"[DISCORD] Exit alert gửi: {side} {symbol} pnl={pnl_pct:+.2f}% | {reason}")
    except Exception as e:
        _log(f"[DISCORD] Lỗi exit alert: {e}")

# ============================================================
# DISCORD — TRAIL SL UPDATE
# ============================================================
def send_discord_trail_update(symbol, side, direction, new_sl, entry, current_price, pnl_pct):
    """Thông báo cập nhật trailing stop."""
    try:
        color = 16776960  # vàng
        embed = {
            "embeds": [{
                "title": f"🔄 TRAIL SL UPDATE  {symbol}  [{side}]",
                "color": color,
                "fields": [
                    {"name": "📌 Price Now", "value": f"`{current_price:.4f}`", "inline": True},
                    {"name": "🆕 New SL",    "value": f"`{new_sl:.4f}`",        "inline": True},
                    {"name": "💹 PnL",       "value": f"`{pnl_pct:+.2f}%`",    "inline": True},
                ],
                "footer": {
                    "text": f"⚠️ Alert Only | {_dt.datetime.now().strftime('%H:%M:%S')}"
                }
            }]
        }
        _send_webhook(embed)
    except Exception as e:
        _log(f"[DISCORD] Lỗi trail alert: {e}")

def _send_webhook(embed):
    data = json.dumps(embed).encode("utf-8")
    req  = urllib.request.Request(
        DISCORD_WEBHOOK, data=data,
        headers={"Content-Type": "application/json", "User-Agent": "DiscordBot (https://github.com/discord/discord-api-docs, 1.0)"}, method="POST"
    )
    urllib.request.urlopen(req, timeout=5)

# ============================================================
# OPEN ALERT (thay thế open_position)
# ============================================================
def open_alert(symbol, side):
    """Thay vì đặt lệnh, chỉ gửi thông báo Discord."""
    direction = "LONG" if side == "BUY" else "SHORT"

    ind = calc_indicators(symbol, TIMEFRAME)
    if not ind:
        _log(f"[ALERT] Không tính được indicator cho {symbol}")
        return

    price = ind["close"]
    atr   = ind["atr"]
    rsi   = ind["rsi"]
    adx   = ind["adx"]

    price_prec, _ = get_precision(symbol)

    regime, _, _ = get_15m_regime(symbol)
    is_counter = (side == "BUY"  and regime == "TRENDING_DOWN") or \
                 (side == "SELL" and regime == "TRENDING_UP")

    if side == "BUY":
        sl_mult = BUY_COUNTER_SL_ATR if is_counter else BUY_SL_ATR
        tp_mult = BUY_COUNTER_TP_ATR if is_counter else 0
    else:
        sl_mult = SELL_COUNTER_SL_ATR if is_counter else SELL_SL_ATR
        tp_mult = SELL_COUNTER_TP_ATR if is_counter else SELL_TP_ATR

    mode_tag = "COUNTER" if is_counter else "WITH_TREND"

    sl = round(price - atr * sl_mult, price_prec) if direction == "LONG" \
         else round(price + atr * sl_mult, price_prec)
    tp = 0 if tp_mult == 0 else (
         round(price + atr * tp_mult, price_prec) if direction == "LONG"
         else round(price - atr * tp_mult, price_prec))

    qty = get_suggested_qty(symbol, side, price)

    signal_type = get_signal_type(ind, side)

    _log(f"[ALERT] → {side} {symbol} @{price:.4f} SL={sl:.4f} TP={tp if tp else 'none'} [{mode_tag}]")

    # Gửi Discord
    send_discord_entry(symbol, side, price, sl, tp, qty, mode_tag, atr, signal_type, rsi, adx, regime)

    # Lưu vào _alerts để theo dõi EXIT
    alert_id = f"ALERT_{symbol}_{side}_{int(time.time())}"
    with _state_lock:
        _alerts[alert_id] = {
            "alert_id":   alert_id,
            "symbol":     symbol,
            "side":       side,
            "direction":  direction,
            "entry":      price,
            "entry_time": time.time(),
            "sl":         sl,
            "tp":         tp,
            "atr":        atr,
            "mode_tag":   mode_tag,
            "is_counter": is_counter,
            "trail_sl":   0 if direction == "LONG" else 999999,
        }

    # Bắt đầu theo dõi để gửi EXIT alert
    t = threading.Thread(target=monitor_alert, args=(alert_id,),
                         daemon=True, name=f"MonAlert-{alert_id}")
    t.start()

    # Log
    try:
        with _log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"[SIGNAL] {_dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
                        f"{symbol} {side} @{price:.4f} SL={sl:.4f} TP={tp if tp else 'none'} [{mode_tag}]\n")
    except:
        pass

# ============================================================
# MONITOR ALERT (theo dõi để gửi EXIT)
# ============================================================
def monitor_alert(alert_id):
    _log(f"[MON-ALERT] Start id={alert_id}")

    while True:
        with _state_lock:
            pos = _alerts.get(alert_id)
        if not pos:
            break

        try:
            symbol     = pos["symbol"]
            side       = pos["side"]
            direction  = pos["direction"]
            entry      = pos["entry"]
            atr        = pos["atr"]
            sl         = pos["sl"]
            tp         = pos["tp"]
            mode_tag   = pos["mode_tag"]
            is_counter = pos["is_counter"]
            hold_sec   = time.time() - pos["entry_time"]
            price_prec, _ = get_precision(symbol)

            price = get_price(symbol)
            if not price:
                time.sleep(1)
                continue

            pnl_pct = ((price - entry) if direction == "LONG"
                       else (entry - price)) / entry * 100

            # ── Kiểm tra SL bị hit ──────────────────────────────
            if direction == "LONG" and price <= sl:
                _send_exit_alert(pos, price, "SL_HIT", hold_sec)
                break
            if direction == "SHORT" and price >= sl:
                _send_exit_alert(pos, price, "SL_HIT", hold_sec)
                break

            # ── Kiểm tra TP bị hit ──────────────────────────────
            if tp > 0:
                if direction == "LONG" and price >= tp:
                    _send_exit_alert(pos, price, "TP_HIT", hold_sec)
                    break
                if direction == "SHORT" and price <= tp:
                    _send_exit_alert(pos, price, "TP_HIT", hold_sec)
                    break

            # ── Timeout ─────────────────────────────────────────
            if side == "SELL":
                timeout = SELL_COUNTER_MAX_HOLD_SEC if is_counter else SELL_MAX_HOLD_SEC
                if hold_sec > timeout:
                    _send_exit_alert(pos, price, f"TIMEOUT pnl={pnl_pct:+.2f}%", hold_sec)
                    break

            if side == "BUY":
                timeout = BUY_COUNTER_MAX_HOLD_SEC if is_counter else BUY_MAX_HOLD_SEC
                if hold_sec > timeout:
                    _send_exit_alert(pos, price, f"TIMEOUT pnl={pnl_pct:+.2f}%", hold_sec)
                    break

            # ── BUY with-trend: adaptive 15m exit ───────────────
            if side == "BUY" and not is_counter and BUY_EXIT_ON_15M_REV:
                should_exit, exit_reason = should_exit_15m(pos, price, hold_sec)
                if should_exit:
                    _send_exit_alert(pos, price, exit_reason, hold_sec)
                    break

            # ── BUY counter: thoát khi 15m đảo & đang lời ───────
            if side == "BUY" and is_counter and BUY_EXIT_ON_15M_REV:
                if pnl_pct > 0 and hold_sec >= 60:
                    _, _, trend_exhausted = get_15m_regime(symbol)
                    if trend_exhausted:
                        _send_exit_alert(pos, price, f"15m_reversal_counter pnl={pnl_pct:+.2f}%", hold_sec)
                        break

            # ── SELL counter: thoát khi 15m cross up ────────────
            if side == "SELL" and is_counter and pnl_pct > 0:
                ind15 = calc_indicators(symbol, "15m")
                if ind15 and ind15.get("cross_up"):
                    _send_exit_alert(pos, price, f"15m_cross_up_counter pnl={pnl_pct:+.2f}%", hold_sec)
                    break

            # ── Trailing Stop alert ──────────────────────────────
            if (pnl_pct > 0 and is_counter) or (side == "SELL" and pnl_pct > 0):
                if tp > 0:
                    tp_dist = abs(tp - entry)
                    moved   = abs(price - entry)

                    if side == "BUY":
                        trail_act = BUY_COUNTER_TRAIL_ACTIVATE
                        trail_atr = BUY_COUNTER_TRAIL_ATR
                    else:
                        trail_act = SELL_COUNTER_TRAIL_ACTIVATE if is_counter else SELL_TRAIL_ACTIVATE
                        trail_atr = SELL_COUNTER_TRAIL_ATR      if is_counter else SELL_TRAIL_ATR

                    activate = tp_dist > 0 and (moved / tp_dist * 100) >= trail_act

                    if activate:
                        if direction == "LONG":
                            new_sl  = round(price - atr * trail_atr, price_prec)
                            cur_sl  = pos.get("trail_sl", 0)
                            if new_sl > cur_sl:
                                with _state_lock:
                                    if alert_id in _alerts:
                                        _alerts[alert_id]["trail_sl"] = new_sl
                                        _alerts[alert_id]["sl"]       = new_sl
                                send_discord_trail_update(symbol, side, direction, new_sl, entry, price, pnl_pct)
                                _log(f"[TRAIL-ALERT] BUY {symbol} SL→{new_sl:.4f} ({pnl_pct:+.2f}%)")
                        else:
                            new_sl  = round(price + atr * trail_atr, price_prec)
                            cur_sl  = pos.get("trail_sl", 999999)
                            if new_sl < cur_sl:
                                with _state_lock:
                                    if alert_id in _alerts:
                                        _alerts[alert_id]["trail_sl"] = new_sl
                                        _alerts[alert_id]["sl"]       = new_sl
                                send_discord_trail_update(symbol, side, direction, new_sl, entry, price, pnl_pct)
                                _log(f"[TRAIL-ALERT] SELL {symbol} SL→{new_sl:.4f} ({pnl_pct:+.2f}%)")

            time.sleep(MONITOR_INTERVAL)

        except Exception as e:
            _log(f"[MON-ALERT] id={alert_id} error: {e}")
            time.sleep(2)

    _log(f"[MON-ALERT] Done id={alert_id}")

def _send_exit_alert(pos, current_price, reason, hold_sec):
    """Gửi exit alert và xoá khỏi _alerts."""
    alert_id  = pos["alert_id"]
    symbol    = pos["symbol"]
    side      = pos["side"]
    direction = pos["direction"]
    entry     = pos["entry"]
    atr       = pos["atr"]
    mode_tag  = pos["mode_tag"]

    send_discord_exit(symbol, side, direction, entry, current_price, reason, hold_sec, atr, mode_tag)
    _log_alert(symbol, side, direction, entry, current_price, reason, hold_sec)

    with _state_lock:
        _alerts.pop(alert_id, None)

# ============================================================
# SCAN
# ============================================================
def scan_symbol(symbol):
    ind = calc_indicators(symbol, TIMEFRAME)
    if not ind:
        return

    price  = ind["close"]
    rsi    = ind["rsi"]
    regime, adx, _ = get_15m_regime(symbol)

    with _state_lock:
        buy_count  = sum(1 for p in _alerts.values() if p["symbol"] == symbol and p["side"] == "BUY")
        sell_count = sum(1 for p in _alerts.values() if p["symbol"] == symbol and p["side"] == "SELL")

    _log(f"[SCAN] {symbol} P={price:.4f} RSI={rsi:.1f} 15m:{regime} "
         f"BUY×{buy_count} SELL×{sell_count}")

    # Scan tín hiệu thoát BUY đang theo dõi
    if buy_count > 0:
        cur_price = get_price(symbol)
        if cur_price:
            with _state_lock:
                wt_ids = [aid for aid, p in _alerts.items()
                          if p["symbol"] == symbol and p["side"] == "BUY"
                          and not p.get("is_counter", False)]
            for aid in wt_ids:
                with _state_lock:
                    p = _alerts.get(aid)
                if not p:
                    continue
                hold = time.time() - p["entry_time"]
                should_exit, exit_reason = should_exit_15m(p, cur_price, hold)
                if should_exit:
                    _log(f"[EXIT-SCAN-ALERT] {symbol} id={aid} | {exit_reason}")
                    _send_exit_alert(p, cur_price, exit_reason + "_scan", hold)
                    return

    for side in ("BUY", "SELL"):
        key     = (symbol, side)
        last_ts = _last_cross.get(key, 0)
        if time.time() - last_ts < CROSS_COOLDOWN_SEC:
            continue

        if not detect_signal(ind, symbol, side):
            continue

        _last_cross[key] = time.time()
        cnt = buy_count if side == "BUY" else sell_count
        _log(f"[SCAN] → ALERT {side} #{cnt+1} {symbol}")

        t = threading.Thread(target=open_alert, args=(symbol, side),
                             daemon=True, name=f"Alert-{symbol}-{side}")
        t.start()

def scan_loop(active_flag):
    global ACTIVE_SYMBOLS
    while active_flag[0]:
        try:
            dow = _dt.datetime.utcnow().weekday()
            ACTIVE_SYMBOLS = SYMBOLS_CRYPTO if dow >= 5 else SYMBOLS_FULL

            with _state_lock:
                buy_syms  = list({p["symbol"] for p in _alerts.values() if p["side"] == "BUY"})
                sell_syms = list({p["symbol"] for p in _alerts.values() if p["side"] == "SELL"})
            total = len(_alerts)
            _log(f"[SCAN] Đang theo dõi={total} | BUY:{buy_syms or '-'} SELL:{sell_syms or '-'}")

            for symbol in ACTIVE_SYMBOLS:
                scan_symbol(symbol)

        except Exception as e:
            _log(f"[SCAN] error: {e}")

        time.sleep(SCAN_INTERVAL)

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("=" * 65)
    print("  DUAL ENGINE v5.0 — ALERT ONLY (Không Trade Thật)")
    print("=" * 65)

    # Kiểm tra kết nối (chỉ cần read)
    ex = get_exchange()
    try:
        ticker = _safe_call(ex.fetch_ticker, "BTC/USDT")
        _log(f"[CCXT] Binance OK — BTC=${ticker['last']:,.0f}")
    except Exception as e:
        print(f"[ERROR] Không kết nối được Binance: {e}")
        exit(1)

    print(f"\n   Mode:         ⚠️  ALERT ONLY — Không đặt lệnh thật!")
    print(f"   Symbols:      {SYMBOLS_FULL}")
    print(f"   Timeframe:    {TIMEFRAME} | Scan: {SCAN_INTERVAL}s")
    print(f"   EMA:          fast={EMA_FAST} slow={EMA_SLOW}")
    print(f"   BUY  SL:      {BUY_SL_ATR} ATR | TP=none | Exit=15m exhausted")
    print(f"   BUY  COUNTER: SL={BUY_COUNTER_SL_ATR} TP={BUY_COUNTER_TP_ATR} ATR")
    print(f"   SELL SL/TP:   {SELL_SL_ATR}/{SELL_TP_ATR} ATR | Trail={SELL_TRAIL_ATR}ATR")
    print(f"   Discord:      {DISCORD_WEBHOOK[:50]}...")
    print("=" * 65)
    print("Bot đang chạy và gửi alert... Ctrl+C để dừng\n")

    # Gửi thông báo khởi động
    try:
        start_embed = {
            "embeds": [{
                "title": "🤖 DUAL ENGINE v5.0 — ALERT MODE đã khởi động",
                "description": (
                    "Bot đang chạy ở chế độ **Alert Only**.\n"
                    "Tất cả tín hiệu sẽ được gửi tại đây để bạn **trade thủ công**.\n"
                    f"Symbols: `{', '.join(SYMBOLS_FULL)}`\n"
                    f"Timeframe: `{TIMEFRAME}` | Scan mỗi `{int(SCAN_INTERVAL)}s`"
                ),
                "color": 3447003,
                "footer": {"text": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            }]
        }
        _send_webhook(start_embed)
    except:
        pass

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"\n{'='*100}\n")
        f.write(f"SESSION ALERT-ONLY: {_dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*100}\n")

    active_flag = [True]
    t = threading.Thread(target=scan_loop, args=(active_flag,),
                         daemon=True, name="DUAL-Scan-Alert")
    t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDừng bot...")
        active_flag[0] = False
        time.sleep(2)
        # Gửi thông báo tắt
        try:
            stop_embed = {
                "embeds": [{
                    "title": "🛑 DUAL ENGINE — Alert Bot đã dừng",
                    "color": 10197915,
                    "footer": {"text": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                }]
            }
            _send_webhook(stop_embed)
        except:
            pass
        print("Bot đã dừng.")
