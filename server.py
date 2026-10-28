import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import uvicorn

app = FastAPI(
    title="Pro Stock Analysis & Lin Ying SOP Terminal API",
    version="3.3.0",
    description="支援全台股(上市/上櫃/ETF)與美股搜尋、林穎走圖SOP多策略選股、風險部位計算機、單股與批次股票池策略回測排行榜之後端服務"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# 1. 內建台美股核心資料庫 (支援中文名稱、代號、產業快速檢索)
# ==========================================
BUILTIN_STOCKS: List[Dict[str, Any]] = [
    # 台股半導體與IC設計
    {"symbol": "2330", "name": "台積電", "market": "TW", "suffix": ".TW", "categories": ["半導體", "AI", "權值股"]},
    {"symbol": "2303", "name": "聯電", "market": "TW", "suffix": ".TW", "categories": ["半導體", "晶圓代工", "高股息"]},
    {"symbol": "2454", "name": "聯發科", "market": "TW", "suffix": ".TW", "categories": ["半導體", "IC設計", "AI"]},
    {"symbol": "3711", "name": "日月光投控", "market": "TW", "suffix": ".TW", "categories": ["半導體", "封測", "AI"]},
    {"symbol": "3034", "name": "聯詠", "market": "TW", "suffix": ".TW", "categories": ["半導體", "IC設計"]},
    {"symbol": "2379", "name": "瑞昱", "market": "TW", "suffix": ".TW", "categories": ["半導體", "IC設計", "網通"]},
    {"symbol": "3443", "name": "創意", "market": "TW", "suffix": ".TW", "categories": ["半導體", "AI", "IP矽智財"]},
    {"symbol": "3661", "name": "世芯-KY", "market": "TW", "suffix": ".TW", "categories": ["半導體", "AI", "IP矽智財"]},
    {"symbol": "3035", "name": "智原", "market": "TW", "suffix": ".TW", "categories": ["半導體", "IP矽智財"]},
    {"symbol": "6223", "name": "旺矽", "market": "TW", "suffix": ".TWO", "categories": ["半導體", "AI", "探針卡"]},
    {"symbol": "8299", "name": "群聯", "market": "TW", "suffix": ".TWO", "categories": ["半導體", "記憶體"]},
    {"symbol": "6488", "name": "環球晶", "market": "TW", "suffix": ".TWO", "categories": ["半導體", "矽晶圓"]},
    {"symbol": "3529", "name": "力旺", "market": "TW", "suffix": ".TWO", "categories": ["半導體", "IP矽智財"]},
    {"symbol": "5274", "name": "信驊", "market": "TW", "suffix": ".TWO", "categories": ["半導體", "AI", "伺服器"]},
    {"symbol": "2408", "name": "南亞科", "market": "TW", "suffix": ".TW", "categories": ["半導體", "記憶體"]},
    {"symbol": "2344", "name": "華邦電", "market": "TW", "suffix": ".TW", "categories": ["半導體", "記憶體"]},
    {"symbol": "6770", "name": "力積電", "market": "TW", "suffix": ".TW", "categories": ["半導體", "晶圓代工"]},
    # 台股網通、電子零組件與AI伺服器
    {"symbol": "2345", "name": "智邦", "market": "TW", "suffix": ".TW", "categories": ["網通", "AI", "伺服器"]},
    {"symbol": "2327", "name": "國巨", "market": "TW", "suffix": ".TW", "categories": ["電子零組件", "被動元件", "車用"]},
    {"symbol": "2317", "name": "鴻海", "market": "TW", "suffix": ".TW", "categories": ["AI", "伺服器", "權值股"]},
    {"symbol": "2382", "name": "廣達", "market": "TW", "suffix": ".TW", "categories": ["AI", "伺服器", "電腦周邊"]},
    {"symbol": "3231", "name": "緯創", "market": "TW", "suffix": ".TW", "categories": ["AI", "伺服器", "電腦周邊"]},
    {"symbol": "6669", "name": "緯穎", "market": "TW", "suffix": ".TW", "categories": ["AI", "伺服器"]},
    {"symbol": "2356", "name": "英業達", "market": "TW", "suffix": ".TW", "categories": ["AI", "伺服器"]},
    {"symbol": "2308", "name": "台達電", "market": "TW", "suffix": ".TW", "categories": ["電子零組件", "AI", "電源供應"]},
    {"symbol": "2301", "name": "光寶科", "market": "TW", "suffix": ".TW", "categories": ["電子零組件", "AI"]},
    {"symbol": "3017", "name": "奇鋐", "market": "TW", "suffix": ".TW", "categories": ["電子零組件", "AI", "散熱"]},
    {"symbol": "3324", "name": "雙鴻", "market": "TW", "suffix": ".TWO", "categories": ["電子零組件", "AI", "散熱"]},
    {"symbol": "3653", "name": "健策", "market": "TW", "suffix": ".TW", "categories": ["電子零組件", "AI", "散熱"]},
    {"symbol": "2383", "name": "台光電", "market": "TW", "suffix": ".TW", "categories": ["電子零組件", "PCB", "AI"]},
    {"symbol": "3037", "name": "欣興", "market": "TW", "suffix": ".TW", "categories": ["電子零組件", "PCB", "載板"]},
    {"symbol": "2059", "name": "川湖", "market": "TW", "suffix": ".TW", "categories": ["電子零組件", "AI", "伺服器"]},
    {"symbol": "3008", "name": "大立光", "market": "TW", "suffix": ".TW", "categories": ["光學鏡頭", "蘋果概念"]},
    {"symbol": "2357", "name": "華碩", "market": "TW", "suffix": ".TW", "categories": ["電腦周邊", "AI"]},
    {"symbol": "2376", "name": "技嘉", "market": "TW", "suffix": ".TW", "categories": ["電腦周邊", "AI", "伺服器"]},
    {"symbol": "2377", "name": "微星", "market": "TW", "suffix": ".TW", "categories": ["電腦周邊", "電競"]},
    {"symbol": "2492", "name": "華新科", "market": "TW", "suffix": ".TW", "categories": ["電子零組件", "被動元件"]},
    # 台股重電、航運、傳產、金融與ETF
    {"symbol": "1519", "name": "華城", "market": "TW", "suffix": ".TW", "categories": ["重電能源", "傳產"]},
    {"symbol": "1503", "name": "士電", "market": "TW", "suffix": ".TW", "categories": ["重電能源", "傳產"]},
    {"symbol": "1513", "name": "中興電", "market": "TW", "suffix": ".TW", "categories": ["重電能源", "傳產"]},
    {"symbol": "2603", "name": "長榮", "market": "TW", "suffix": ".TW", "categories": ["航運", "高股息"]},
    {"symbol": "2609", "name": "陽明", "market": "TW", "suffix": ".TW", "categories": ["航運"]},
    {"symbol": "2615", "name": "萬海", "market": "TW", "suffix": ".TW", "categories": ["航運"]},
    {"symbol": "2618", "name": "長榮航", "market": "TW", "suffix": ".TW", "categories": ["航運", "觀光航空"]},
    {"symbol": "2881", "name": "富邦金", "market": "TW", "suffix": ".TW", "categories": ["金融", "權值股"]},
    {"symbol": "2882", "name": "國泰金", "market": "TW", "suffix": ".TW", "categories": ["金融", "權值股"]},
    {"symbol": "2891", "name": "中信金", "market": "TW", "suffix": ".TW", "categories": ["金融", "高股息"]},
    {"symbol": "2886", "name": "兆豐金", "market": "TW", "suffix": ".TW", "categories": ["金融", "高股息"]},
    {"symbol": "2884", "name": "玉山金", "market": "TW", "suffix": ".TW", "categories": ["金融"]},
    {"symbol": "2002", "name": "中鋼", "market": "TW", "suffix": ".TW", "categories": ["傳產", "鋼鐵"]},
    {"symbol": "1301", "name": "台塑", "market": "TW", "suffix": ".TW", "categories": ["傳產", "塑化"]},
    {"symbol": "2412", "name": "中華電", "market": "TW", "suffix": ".TW", "categories": ["電信", "防禦型"]},
    {"symbol": "0050", "name": "元大台灣50", "market": "TW", "suffix": ".TW", "categories": ["ETF", "市值型"]},
    {"symbol": "0056", "name": "元大高股息", "market": "TW", "suffix": ".TW", "categories": ["ETF", "高股息"]},
    {"symbol": "00878", "name": "國泰永續高股息", "market": "TW", "suffix": ".TW", "categories": ["ETF", "高股息"]},
    {"symbol": "00919", "name": "群益台灣精選高息", "market": "TW", "suffix": ".TW", "categories": ["ETF", "高股息"]},
    {"symbol": "00929", "name": "復華台灣科技優息", "market": "TW", "suffix": ".TW", "categories": ["ETF", "高股息"]},
    # 美股熱門科技與指數標的
    {"symbol": "NVDA", "name": "輝達 (NVIDIA)", "market": "US", "suffix": "", "categories": ["美股", "AI", "半導體"]},
    {"symbol": "AAPL", "name": "蘋果 (Apple)", "market": "US", "suffix": "", "categories": ["美股", "科技巨頭"]},
    {"symbol": "MSFT", "name": "微軟 (Microsoft)", "market": "US", "suffix": "", "categories": ["美股", "AI", "科技巨頭"]},
    {"symbol": "GOOGL", "name": "谷歌 (Alphabet)", "market": "US", "suffix": "", "categories": ["美股", "AI", "科技巨頭"]},
    {"symbol": "AMZN", "name": "亞馬遜 (Amazon)", "market": "US", "suffix": "", "categories": ["美股", "科技巨頭"]},
    {"symbol": "META", "name": "Meta Platforms", "market": "US", "suffix": "", "categories": ["美股", "AI", "科技巨頭"]},
    {"symbol": "TSLA", "name": "特斯拉 (Tesla)", "market": "US", "suffix": "", "categories": ["美股", "電動車", "AI"]},
    {"symbol": "AMD", "name": "超微 (AMD)", "market": "US", "suffix": "", "categories": ["美股", "AI", "半導體"]},
    {"symbol": "AVGO", "name": "博通 (Broadcom)", "market": "US", "suffix": "", "categories": ["美股", "AI", "半導體", "網通"]},
    {"symbol": "TSM", "name": "台積電 ADR (TSM)", "market": "US", "suffix": "", "categories": ["美股", "半導體", "AI"]},
    {"symbol": "SMCI", "name": "美超微 (SuperMicro)", "market": "US", "suffix": "", "categories": ["美股", "AI", "伺服器"]},
    {"symbol": "PLTR", "name": "Palantir (PLTR)", "market": "US", "suffix": "", "categories": ["美股", "AI", "軟體"]},
    {"symbol": "ARM", "name": "安謀 (Arm Holdings)", "market": "US", "suffix": "", "categories": ["美股", "半導體", "IP矽智財"]},
    {"symbol": "MU", "name": "美光 (Micron)", "market": "US", "suffix": "", "categories": ["美股", "半導體", "記憶體"]},
    {"symbol": "INTC", "name": "英特爾 (Intel)", "market": "US", "suffix": "", "categories": ["美股", "半導體"]},
    {"symbol": "QCOM", "name": "高通 (Qualcomm)", "market": "US", "suffix": "", "categories": ["美股", "半導體", "網通"]},
    {"symbol": "NFLX", "name": "網飛 (Netflix)", "market": "US", "suffix": "", "categories": ["美股", "科技巨頭"]},
    {"symbol": "QQQ", "name": "Invesco 那斯達克100 ETF", "market": "US", "suffix": "", "categories": ["美股", "ETF"]},
    {"symbol": "SPY", "name": "SPDR 標普500 ETF", "market": "US", "suffix": "", "categories": ["美股", "ETF"]},
    {"symbol": "SOXX", "name": "iShares 費城半導體 ETF", "market": "US", "suffix": "", "categories": ["美股", "ETF", "半導體"]},
]

SYMBOL_MAP: Dict[str, Dict[str, Any]] = {item["symbol"].upper(): item for item in BUILTIN_STOCKS}

_CACHE: Dict[str, Any] = {}
CACHE_TTL_QUOTE = 25
CACHE_TTL_KBARS = 60


def safe_float(val: Any, decimals: int = 2, default: Optional[float] = None) -> Optional[float]:
    """安全轉換浮點數，防止 NaN 或 Inf 導致 JSON 序列化錯誤"""
    if val is None:
        return default
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return default
        return round(f, decimals)
    except (ValueError, TypeError):
        return default


def calculate_position_sizing(
    capital: float,
    risk_pct: float,
    entry_price: float,
    stop_loss: float,
    target_price: Optional[float] = None,
    market: str = "TW"
) -> Dict[str, Any]:
    """
    林穎《學會走圖SOP》核心風險控管與部位計算機
    """
    if capital <= 0 or risk_pct <= 0 or entry_price <= 0 or stop_loss <= 0:
        raise ValueError("資金、風險比例、進場價與停損價皆必須大於 0")

    risk_per_share = abs(entry_price - stop_loss)
    if risk_per_share < 1e-6:
        raise ValueError("進場價與停損價不可相同")

    max_risk_amount = capital * (risk_pct / 100.0)
    raw_shares_by_risk = int(math.floor(max_risk_amount / risk_per_share))
    max_shares_by_capital = int(math.floor(capital / entry_price))

    suggested_shares = max(0, min(raw_shares_by_risk, max_shares_by_capital))

    tw_board_lots = suggested_shares // 1000
    tw_odd_shares = suggested_shares % 1000
    lot_only_shares = tw_board_lots * 1000 if market.upper() == "TW" else suggested_shares

    total_cost = round(suggested_shares * entry_price, 2)
    capital_usage_pct = round((total_cost / capital) * 100.0, 2) if capital > 0 else 0.0
    actual_max_loss = round(suggested_shares * risk_per_share, 2)
    stop_loss_pct = round((risk_per_share / entry_price) * 100.0, 2)

    if target_price is None or target_price <= 0:
        is_long = entry_price >= stop_loss
        target_price = round(entry_price + (risk_per_share * 2.0 if is_long else -risk_per_share * 2.0), 2)

    reward_per_share = abs(target_price - entry_price)
    rr_ratio = round(reward_per_share / risk_per_share, 2) if risk_per_share > 0 else 0.0
    expected_profit = round(suggested_shares * reward_per_share, 2)

    advice_list = []
    status_level = "GOOD"
    if stop_loss_pct > 8.0:
        status_level = "WARN"
        advice_list.append(f"停損幅度達 {stop_loss_pct}% (>8%)，代表目前股價乖離支撐較遠，建議等待拉回靠近均線再切入或縮小部位。")
    else:
        advice_list.append(f"停損幅度 {stop_loss_pct}% 控制得宜，符合林穎 SOP 近支撐買進原則。")

    if rr_ratio < 1.5:
        status_level = "WARN"
        advice_list.append(f"目前風報比僅 1:{rr_ratio} (<1.5)，上方壓力較近、獲利空間受限。")
    else:
        advice_list.append(f"風報比達 1:{rr_ratio}，具備波段操作優勢。")

    if raw_shares_by_risk > max_shares_by_capital:
        advice_list.append("因停損點極近，已自動以總資金 100% 現股購買力作為股數上限。")

    return {
        "capital": round(capital, 2),
        "risk_pct": round(risk_pct, 2),
        "entry_price": round(entry_price, 2),
        "stop_loss": round(stop_loss, 2),
        "target_price": round(target_price, 2),
        "max_risk_amount": round(max_risk_amount, 2),
        "risk_per_share": round(risk_per_share, 2),
        "stop_loss_pct": stop_loss_pct,
        "raw_shares_by_risk": raw_shares_by_risk,
        "max_shares_by_capital": max_shares_by_capital,
        "suggested_shares": suggested_shares,
        "tw_board_lots": tw_board_lots,
        "tw_odd_shares": tw_odd_shares,
        "lot_only_shares": lot_only_shares,
        "total_cost": total_cost,
        "capital_usage_pct": capital_usage_pct,
        "actual_max_loss": actual_max_loss,
        "reward_per_share": round(reward_per_share, 2),
        "rr_ratio": rr_ratio,
        "expected_profit": expected_profit,
        "status_level": status_level,
        "advice": " ".join(advice_list)
    }


def resolve_symbol_info(raw_query: str) -> Dict[str, Any]:
    """解析使用者輸入的台美股代號或中文名稱"""
    q = raw_query.strip()
    if not q:
        raise ValueError("空的查詢字串")

    q_upper = q.upper()
    clean_code = q_upper.replace(".TW", "").replace(".TWO", "")

    if clean_code in SYMBOL_MAP:
        item = SYMBOL_MAP[clean_code]
        return {
            "symbol": item["symbol"],
            "query_symbol": f"{item['symbol']}{item['suffix']}",
            "name": item["name"],
            "market": item["market"],
            "categories": item["categories"]
        }

    for item in BUILTIN_STOCKS:
        if q in item["name"] or item["name"].upper() == q_upper:
            return {
                "symbol": item["symbol"],
                "query_symbol": f"{item['symbol']}{item['suffix']}",
                "name": item["name"],
                "market": item["market"],
                "categories": item["categories"]
            }

    if q_upper.endswith(".TW") or q_upper.endswith(".TWO"):
        return {
            "symbol": clean_code,
            "query_symbol": q_upper,
            "name": clean_code,
            "market": "TW",
            "categories": ["台股"]
        }

    has_chinese = any('\u4e00' <= ch <= '\u9fff' for ch in q)
    if has_chinese:
        ext_results = search_external_stocks(q)
        if ext_results:
            first = ext_results[0]
            return {
                "symbol": first["symbol"],
                "query_symbol": first["query_symbol"],
                "name": first["name"],
                "market": first["market"],
                "categories": first.get("categories", ["自選股"])
            }

    is_tw_code = clean_code.isdigit() or (len(clean_code) >= 4 and clean_code[:4].isdigit())
    if is_tw_code:
        for suffix in [".TW", ".TWO"]:
            candidate = f"{clean_code}{suffix}"
            try:
                t = yf.Ticker(candidate)
                df = t.history(period="5d")
                if not df.empty:
                    SYMBOL_MAP[clean_code] = {
                        "symbol": clean_code,
                        "name": clean_code,
                        "market": "TW",
                        "suffix": suffix,
                        "categories": ["台股"]
                    }
                    return {
                        "symbol": clean_code,
                        "query_symbol": candidate,
                        "name": clean_code,
                        "market": "TW",
                        "categories": ["台股"]
                    }
            except Exception:
                continue
        return {
            "symbol": clean_code,
            "query_symbol": f"{clean_code}.TW",
            "name": clean_code,
            "market": "TW",
            "categories": ["台股"]
        }

    return {
        "symbol": clean_code,
        "query_symbol": clean_code,
        "name": clean_code,
        "market": "US",
        "categories": ["美股"]
    }


def search_external_stocks(keyword: str) -> List[Dict[str, Any]]:
    """透過鉅亨網與 Yahoo Finance 公開 API 檢索全台美股"""
    found: List[Dict[str, Any]] = []
    seen = set()

    try:
        resp = requests.get(
            "https://ess.api.cnyes.com/ess/api/v1/siteSearch/quote",
            params={"q": keyword, "limit": 8},
            timeout=2.5,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        if resp.status_code == 200:
            data = resp.json().get("data", {}).get("items", [])
            for item in data:
                code_str = item.get("code", "")
                name_str = item.get("name", "")
                parts = code_str.split(":")
                if len(parts) >= 2:
                    mkt_prefix, sym = parts[0], parts[1].upper()
                    if sym in seen:
                        continue
                    if mkt_prefix in ("TWS", "TWO", "TW"):
                        suffix = ".TWO" if mkt_prefix == "TWO" else ".TW"
                        if sym not in SYMBOL_MAP:
                            SYMBOL_MAP[sym] = {
                                "symbol": sym,
                                "name": name_str or sym,
                                "market": "TW",
                                "suffix": suffix,
                                "categories": ["台股"]
                            }
                        found.append({
                            "symbol": sym,
                            "query_symbol": f"{sym}{suffix}",
                            "name": name_str or sym,
                            "market": "TW",
                            "categories": ["台股"]
                        })
                        seen.add(sym)
                    elif mkt_prefix in ("USS", "US"):
                        if sym not in SYMBOL_MAP:
                            SYMBOL_MAP[sym] = {
                                "symbol": sym,
                                "name": name_str or sym,
                                "market": "US",
                                "suffix": "",
                                "categories": ["美股"]
                            }
                        found.append({
                            "symbol": sym,
                            "query_symbol": sym,
                            "name": name_str or sym,
                            "market": "US",
                            "categories": ["美股"]
                        })
                        seen.add(sym)
    except Exception:
        pass

    if len(found) < 5:
        try:
            resp = requests.get(
                "https://query2.finance.yahoo.com/v1/finance/search",
                params={"q": keyword, "quotesCount": 8, "newsCount": 0},
                timeout=2.5,
                headers={"User-Agent": "Mozilla/5.0"}
            )
            if resp.status_code == 200:
                quotes = resp.json().get("quotes", [])
                for q_item in quotes:
                    raw_sym = q_item.get("symbol", "")
                    if not raw_sym:
                        continue
                    short_name = q_item.get("shortname") or q_item.get("longname") or raw_sym
                    if raw_sym.endswith(".TW") or raw_sym.endswith(".TWO"):
                        clean_s = raw_sym.replace(".TW", "").replace(".TWO", "")
                        if clean_s not in seen:
                            found.append({
                                "symbol": clean_s,
                                "query_symbol": raw_sym,
                                "name": short_name,
                                "market": "TW",
                                "categories": ["台股"]
                            })
                            seen.add(clean_s)
                    elif "." not in raw_sym and raw_sym not in seen:
                        found.append({
                            "symbol": raw_sym,
                            "query_symbol": raw_sym,
                            "name": short_name,
                            "market": "US",
                            "categories": ["美股"]
                        })
                        seen.add(raw_sym)
        except Exception:
            pass

    return found


def compute_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """計算 MA5/10/20/60, 布林通道, 均量, RSI(14), KD(9,3,3), MACD(12,26,9)"""
    df = df.copy()
    close = df["Close"]
    high = df["High"]
    low = df["Low"]
    vol = df["Volume"]

    df["MA5"] = close.rolling(window=5).mean()
    df["MA10"] = close.rolling(window=10).mean()
    df["MA20"] = close.rolling(window=20).mean()
    df["MA60"] = close.rolling(window=60).mean()

    std20 = close.rolling(window=20).std()
    df["BB_Upper"] = df["MA20"] + 2 * std20
    df["BB_Lower"] = df["MA20"] - 2 * std20

    df["VolMA5"] = vol.rolling(window=5).mean()
    df["VolMA20"] = vol.rolling(window=20).mean()

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(window=14).mean()
    loss = (-delta.clip(upper=0)).rolling(window=14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["RSI"] = (100 - (100 / (1 + rs))).fillna(50.0)

    low9 = low.rolling(window=9).min()
    high9 = high.rolling(window=9).max()
    denom = (high9 - low9).replace(0, np.nan)
    rsv = ((close - low9) / denom * 100).fillna(50.0)
    k_vals, d_vals = [], []
    k_prev, d_prev = 50.0, 50.0
    for val in rsv:
        k_curr = (2.0 / 3.0) * k_prev + (1.0 / 3.0) * float(val)
        d_curr = (2.0 / 3.0) * d_prev + (1.0 / 3.0) * k_curr
        k_vals.append(k_curr)
        d_vals.append(d_curr)
        k_prev, d_prev = k_curr, d_curr
    df["K"] = k_vals
    df["D"] = d_vals

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_Signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_Hist"] = df["MACD"] - df["MACD_Signal"]

    return df


def evaluate_lin_ying_sop(df: pd.DataFrame, market: str = "TW") -> Dict[str, Any]:
    """林穎 6 大進場條件量化評估與預設部位風險試算"""
    if len(df) < 22:
        return {
            "sop_score": 0,
            "patterns": [],
            "criteria": [False] * 6,
            "stop_loss": None,
            "target_price": None,
            "resistance_20d": None,
            "support_20d": None,
            "ma_spread_pct": None,
            "volume_ratio": 1.0,
            "position_plan": None
        }

    latest = df.iloc[-1]
    prev = df.iloc[-2]
    close_val = float(latest["Close"])
    prev_close = float(prev["Close"])
    open_val = float(latest["Open"])

    ma5 = safe_float(latest.get("MA5"), 2, close_val)
    ma20 = safe_float(latest.get("MA20"), 2, close_val)
    ma60 = safe_float(latest.get("MA60"), 2, ma20)
    prev_ma5 = safe_float(prev.get("MA5"), 2, prev_close)
    prev_ma20 = safe_float(prev.get("MA20"), 2, prev_close)

    vol = float(latest.get("Volume", 0))
    vol_ma20 = safe_float(latest.get("VolMA20"), 2, vol) or 1.0
    vol_ratio = round(vol / vol_ma20, 2) if vol_ma20 > 0 else 1.0

    recent_slice = df.iloc[-14:-1]
    spreads = []
    for _, r in recent_slice.iterrows():
        m_list = [r.get("MA5"), r.get("MA20"), r.get("MA60")]
        valid_m = [float(x) for x in m_list if pd.notna(x)]
        if len(valid_m) >= 2 and r["Close"] > 0:
            spreads.append((max(valid_m) - min(valid_m)) / float(r["Close"]) * 100.0)
    ma_spread_pct = round(sum(spreads) / len(spreads), 2) if spreads else 5.0
    was_entangled = ma_spread_pct <= 3.8

    window20 = df.iloc[-21:-1]
    res_20d = safe_float(window20["High"].max(), 2, close_val * 1.05)
    sup_20d = safe_float(window20["Low"].min(), 2, close_val * 0.95)

    c1_trend = bool(close_val >= ma20 and ma5 >= ma20 * 0.995)
    high10 = float(df.iloc[-11:-1]["High"].max()) if len(df) >= 12 else close_val
    c2_position = bool(close_val >= high10 * 0.995 or (close_val >= ma20 and (close_val - ma20) / ma20 <= 0.03))
    c3_kbar = bool(close_val > open_val and close_val > prev_close)
    c4_ma = bool(close_val >= ma5 and close_val >= ma20 and (ma5 >= ma20 or was_entangled))
    c5_volume = bool((close_val > prev_close and vol_ratio >= 1.2) or (close_val >= ma20 and vol_ratio <= 0.9))

    k_val = safe_float(latest.get("K"), 2, 50.0)
    d_val = safe_float(latest.get("D"), 2, 50.0)
    macd_hist = safe_float(latest.get("MACD_Hist"), 3, 0.0)
    prev_hist = safe_float(prev.get("MACD_Hist"), 3, 0.0)
    c6_indicator = bool(k_val >= d_val or macd_hist > 0 or macd_hist > prev_hist)

    criteria = [c1_trend, c2_position, c3_kbar, c4_ma, c5_volume, c6_indicator]
    sop_score = sum(1 for c in criteria if c)

    patterns: List[str] = []
    ma_golden_cross = (prev_ma5 <= prev_ma20 * 1.002) and (ma5 > ma20)
    kd_golden_cross = (safe_float(prev.get("K"), 2, 50) <= safe_float(prev.get("D"), 2, 50)) and (k_val > d_val)

    if was_entangled and close_val > ma5 and close_val > ma20 and vol_ratio >= 1.25 and close_val > prev_close:
        patterns.append("均線糾結帶量突破")
    if (ma_golden_cross or kd_golden_cross) and close_val > prev_close and vol_ratio >= 1.1:
        patterns.append("底部起漲黃金交叉")
    if ma5 > ma20 and ma20 >= ma60 * 0.99 and close_val >= ma20 and (close_val - ma20) / ma20 <= 0.035 and close_val >= open_val:
        patterns.append("多頭排列回測支撐")
    if close_val > high10 and c3_kbar and vol_ratio >= 1.3:
        patterns.append("創高帶量突破紅K")
    if close_val < ma5 and close_val < ma20 and close_val < open_val and vol_ratio >= 1.4:
        patterns.append("高檔爆量長黑轉弱")

    if not patterns and sop_score >= 5:
        patterns.append("SOP多方強勢達標")

    tech_support = min(float(latest["Low"]), ma20) * 0.99
    if tech_support >= close_val or (close_val - tech_support) / close_val > 0.12:
        stop_loss = round(close_val * 0.96, 2)
    else:
        stop_loss = round(tech_support, 2)

    risk_dist = max(close_val - stop_loss, close_val * 0.02)
    target_price = round(close_val + risk_dist * 2.0, 2)

    try:
        position_plan = calculate_position_sizing(
            capital=1000000.0,
            risk_pct=2.0,
            entry_price=close_val,
            stop_loss=stop_loss,
            target_price=target_price,
            market=market
        )
    except Exception:
        position_plan = None

    return {
        "sop_score": sop_score,
        "patterns": patterns,
        "criteria": criteria,
        "stop_loss": stop_loss,
        "target_price": target_price,
        "resistance_20d": res_20d,
        "support_20d": sup_20d,
        "ma_spread_pct": ma_spread_pct,
        "volume_ratio": vol_ratio,
        "rsi": safe_float(latest.get("RSI"), 1, 50.0),
        "k": k_val,
        "d": d_val,
        "macd_hist": macd_hist,
        "position_plan": position_plan
    }


def run_sop_backtest(
    df: pd.DataFrame,
    strategy: str = "entanglement",
    eval_bars: int = 126,
    exit_mode: str = "ma10_trail"
) -> Dict[str, Any]:
    """
    林穎《學會走圖SOP》歷史策略回測引擎：
    - 預先在完整資料集計算 MA5/10/20/60、KD、MACD、均量
    - 針對過去半年 (eval_bars=126) 或一年 (eval_bars=252) 逐根 K 棒模擬進出場
    - 統計：勝率 (Win Rate)、平均報酬率 (Avg Return)、最大回撤 (Max Drawdown)、累計報酬與交易明細
    """
    n = len(df)
    if n < 30:
        return {
            "total_trades": 0,
            "win_trades": 0,
            "loss_trades": 0,
            "win_rate": 0.0,
            "avg_return_pct": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "profit_factor": 0.0,
            "trades": []
        }

    start_idx = max(25, n - eval_bars)

    def execute_pass(strict_mode: bool) -> List[Dict[str, Any]]:
        trades_list: List[Dict[str, Any]] = []
        in_pos = False
        entry_info: Dict[str, Any] = {}

        spread_limit = 4.2 if strict_mode else 6.0
        vol_mult = 1.15 if strict_mode else 1.0

        for i in range(start_idx, n):
            row = df.iloc[i]
            prev = df.iloc[i - 1]
            idx_time = df.index[i]
            ts = int(idx_time.timestamp())
            date_str = idx_time.strftime("%Y-%m-%d")

            c_val = float(row["Close"])
            o_val = float(row["Open"])
            h_val = float(row["High"])
            l_val = float(row["Low"])
            prev_c = float(prev["Close"])

            ma5 = safe_float(row.get("MA5"), 2, c_val)
            ma10 = safe_float(row.get("MA10"), 2, c_val)
            ma20 = safe_float(row.get("MA20"), 2, c_val)
            ma60 = safe_float(row.get("MA60"), 2, ma20)
            prev_ma5 = safe_float(prev.get("MA5"), 2, prev_c)
            prev_ma20 = safe_float(prev.get("MA20"), 2, prev_c)

            vol = float(row.get("Volume", 0))
            vol_ma20 = safe_float(row.get("VolMA20"), 2, vol) or 1.0
            vol_ratio = vol / vol_ma20 if vol_ma20 > 0 else 1.0

            k_val = safe_float(row.get("K"), 2, 50.0)
            d_val = safe_float(row.get("D"), 2, 50.0)
            prev_k = safe_float(prev.get("K"), 2, 50.0)
            prev_d = safe_float(prev.get("D"), 2, 50.0)

            if not in_pos:
                lookback_slice = df.iloc[max(0, i - 10):i]
                spreads = []
                for _, r in lookback_slice.iterrows():
                    vals = [float(x) for x in [r.get("MA5"), r.get("MA10"), r.get("MA20"), r.get("MA60")] if pd.notna(x)]
                    if len(vals) >= 3 and r["Close"] > 0:
                        short_vals = [float(x) for x in [r.get("MA5"), r.get("MA10"), r.get("MA20")] if pd.notna(x)]
                        spreads.append((max(short_vals) - min(short_vals)) / float(r["Close"]) * 100.0)
                avg_spread = (sum(spreads) / len(spreads)) if spreads else 5.0
                high5 = float(df.iloc[max(0, i - 5):i]["High"].max()) if i >= 5 else c_val

                is_bull_bar = (c_val > o_val and c_val > prev_c)
                is_entangled_break = (
                    avg_spread <= spread_limit
                    and c_val > ma5
                    and c_val > ma20
                    and c_val >= high5 * 0.995
                    and is_bull_bar
                    and vol_ratio >= vol_mult
                )
                is_golden_cross = (
                    ((prev_ma5 <= prev_ma20 * 1.002 and ma5 > ma20) or (prev_k <= prev_d and k_val > d_val and c_val >= ma20))
                    and is_bull_bar
                    and vol_ratio >= (1.05 if strict_mode else 0.9)
                )
                is_pullback = (
                    ma5 >= ma20
                    and ma20 >= ma60 * 0.985
                    and l_val <= ma20 * 1.025
                    and c_val >= ma20
                    and is_bull_bar
                )

                triggered = False
                trig_label = ""
                if strategy == "entanglement" and is_entangled_break:
                    triggered = True
                    trig_label = "均線糾結突破"
                elif strategy == "golden_cross" and is_golden_cross:
                    triggered = True
                    trig_label = "底部起漲金叉"
                elif strategy == "pullback" and is_pullback:
                    triggered = True
                    trig_label = "多頭回測支撐"
                elif strategy == "sop_all" and (is_entangled_break or is_golden_cross or is_pullback):
                    triggered = True
                    trig_label = "均線糾結突破" if is_entangled_break else ("底部起漲金叉" if is_golden_cross else "多頭回測支撐")

                if triggered and i < n - 1:
                    raw_sl = min(l_val, ma20) * 0.99
                    stop_loss = max(raw_sl, c_val * 0.94)
                    if stop_loss >= c_val:
                        stop_loss = c_val * 0.96
                    risk_dist = max(c_val - stop_loss, c_val * 0.025)
                    target_price = c_val + risk_dist * 2.0

                    in_pos = True
                    entry_info = {
                        "entry_idx": i,
                        "entry_time": ts,
                        "entry_date": date_str,
                        "entry_price": round(c_val, 2),
                        "stop_loss": round(stop_loss, 2),
                        "target_price": round(target_price, 2),
                        "peak_price": c_val,
                        "min_low_since_entry": c_val,
                        "pattern": trig_label
                    }
            else:
                hold_days = i - entry_info["entry_idx"]
                entry_p = entry_info["entry_price"]
                sl_p = entry_info["stop_loss"]
                tp_p = entry_info["target_price"]
                entry_info["peak_price"] = max(entry_info["peak_price"], h_val)
                entry_info["min_low_since_entry"] = min(entry_info["min_low_since_entry"], l_val)

                exit_flag = False
                exit_p = c_val
                exit_reason = ""

                if c_val <= sl_p or l_val <= sl_p * 0.99:
                    exit_flag = True
                    exit_p = round(min(c_val, sl_p), 2)
                    exit_reason = "觸及防守停損"
                elif exit_mode == "fixed_rr" and h_val >= tp_p:
                    exit_flag = True
                    exit_p = round(tp_p, 2)
                    exit_reason = "達 2R 目標停利"
                elif exit_mode == "ma10_trail" and hold_days >= 2 and c_val < ma10 and prev_c >= safe_float(prev.get("MA10"), 2, prev_c):
                    exit_flag = True
                    exit_p = round(c_val, 2)
                    exit_reason = "跌破10日線移動停利" if exit_p >= entry_p else "跌破10日線轉弱出場"
                elif exit_mode == "ma10_trail" and ((entry_info["peak_price"] - entry_p) / entry_p >= 0.08) and c_val < ma5:
                    exit_flag = True
                    exit_p = round(c_val, 2)
                    exit_reason = "高檔破5日線獲利了結"
                elif hold_days >= 22 or i == n - 1:
                    exit_flag = True
                    exit_p = round(c_val, 2)
                    exit_reason = "期末收盤結算" if i == n - 1 else "波段持有期滿結算"

                if exit_flag:
                    ret_pct = round((exit_p - entry_p) / entry_p * 100.0, 2)
                    intra_dd_pct = round((entry_info["min_low_since_entry"] - entry_info["peak_price"]) / entry_info["peak_price"] * 100.0, 2)
                    trades_list.append({
                        "entry_time": entry_info["entry_time"],
                        "entry_date": entry_info["entry_date"],
                        "entry_price": entry_p,
                        "exit_time": ts,
                        "exit_date": date_str,
                        "exit_price": exit_p,
                        "holding_days": hold_days,
                        "return_pct": ret_pct,
                        "intra_dd_pct": abs(intra_dd_pct),
                        "exit_reason": exit_reason,
                        "pattern": entry_info["pattern"]
                    })
                    in_pos = False

        return trades_list

    trades = execute_pass(strict_mode=True)
    if len(trades) == 0:
        trades = execute_pass(strict_mode=False)

    total_trades = len(trades)
    if total_trades == 0:
        return {
            "total_trades": 0,
            "win_trades": 0,
            "loss_trades": 0,
            "win_rate": 0.0,
            "avg_return_pct": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "profit_factor": 0.0,
            "trades": []
        }

    win_trades = sum(1 for t in trades if t["return_pct"] > 0)
    loss_trades = total_trades - win_trades
    win_rate = round((win_trades / total_trades) * 100.0, 2)
    avg_return_pct = round(sum(t["return_pct"] for t in trades) / total_trades, 2)

    equity = 1.0
    peak_equity = 1.0
    max_dd = 0.0
    gross_profit = 0.0
    gross_loss = 0.0

    for t in trades:
        r = t["return_pct"] / 100.0
        if r > 0:
            gross_profit += t["return_pct"]
        else:
            gross_loss += abs(t["return_pct"])

        equity_after = equity * (1.0 + r)
        peak_equity = max(peak_equity, equity, equity_after)
        dd_curve = (peak_equity - equity_after) / peak_equity * 100.0 if peak_equity > 0 else 0.0
        max_dd = max(max_dd, dd_curve, t.get("intra_dd_pct", 0.0) * 0.5)
        equity = equity_after

    total_return_pct = round((equity - 1.0) * 100.0, 2)
    max_drawdown_pct = round(max_dd, 2)
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else round(gross_profit, 2)

    return {
        "total_trades": total_trades,
        "win_trades": win_trades,
        "loss_trades": loss_trades,
        "win_rate": win_rate,
        "avg_return_pct": avg_return_pct,
        "total_return_pct": total_return_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "profit_factor": profit_factor,
        "trades": trades
    }


def fetch_and_backtest_single(
    raw_sym: str,
    strategy: str,
    eval_bars: int,
    fetch_period: str,
    exit_mode: str
) -> Optional[Dict[str, Any]]:
    """供單股與批次股票池平行回測共用的核心輔助函式"""
    info = resolve_symbol_info(raw_sym)
    query_sym = info["query_symbol"]

    cache_key = f"bt_df_{query_sym}_{fetch_period}"
    now = time.time()
    if cache_key in _CACHE and (now - _CACHE[cache_key]["ts"] < CACHE_TTL_KBARS):
        df = _CACHE[cache_key]["data"]
    else:
        ticker = yf.Ticker(query_sym)
        df = ticker.history(period=fetch_period, interval="1d")
        if df.empty and info["market"] == "TW" and query_sym.endswith(".TW"):
            alt_sym = f"{info['symbol']}.TWO"
            df = yf.Ticker(alt_sym).history(period=fetch_period, interval="1d")
            if not df.empty:
                query_sym = alt_sym
        if df.empty:
            return None
        df = compute_technical_indicators(df)
        _CACHE[cache_key] = {"ts": now, "data": df}

    bt_res = run_sop_backtest(df, strategy=strategy, eval_bars=eval_bars, exit_mode=exit_mode)
    return {
        "symbol": info["symbol"],
        "name": info["name"],
        "market": info["market"],
        **bt_res
    }


# ==========================================
# API 路由端點 (固定路徑置於動態路徑之前)
# ==========================================

@app.get("/api/backtest-batch")
def backtest_stock_pool_leaderboard(
    strategy: str = Query("entanglement", description="回測策略: entanglement, golden_cross, pullback, sop_all"),
    period: str = Query("6mo", description="回測區間: 6mo 或 1y"),
    exit_mode: str = Query("ma10_trail", description="出場紀律: ma10_trail 或 fixed_rr"),
    sort_by: str = Query("total_return", description="排名依據: total_return (總報酬) 或 win_rate (勝率)"),
    symbols: Optional[str] = Query(None, description="自選股代號清單，以逗號分隔")
):
    """
    林穎《學會走圖SOP》批次股票池回測排行榜 API：
    一鍵平行統計自選股池或熱門股池過去半年/一年執行指定策略之勝率與總報酬排名
    """
    if symbols:
        scan_list = [s.strip() for s in symbols.split(",") if s.strip()]
    else:
        scan_list = [
            "2330", "2303", "2345", "2327", "6223", "2317",
            "2454", "2382", "3017", "2603", "NVDA", "AAPL", "AMD"
        ]

    # 去重並限制單次最大掃描數量確保極速回應
    unique_syms = []
    seen = set()
    for s in scan_list:
        u = s.upper()
        if u not in seen:
            seen.add(u)
            unique_syms.append(s)
    unique_syms = unique_syms[:25]

    eval_bars = 252 if period == "1y" else 126
    fetch_period = "2y" if period == "1y" else "1y"

    leaderboard: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_map = {
            executor.submit(fetch_and_backtest_single, sym, strategy, eval_bars, fetch_period, exit_mode): sym
            for sym in unique_syms
        }
        for future in as_completed(future_map):
            sym = future_map[future]
            try:
                res = future.result()
                if res is not None:
                    # 排行榜列表省略單筆交易陣列以精簡傳輸，保留完整統計指標
                    item_summary = {k: v for k, v in res.items() if k != "trades"}
                    leaderboard.append(item_summary)
            except Exception as e:
                print(f"[BatchBacktest] {sym} 略過: {e}")

    # 依指定欄位進行排名
    if sort_by == "win_rate":
        leaderboard.sort(
            key=lambda x: (x["win_rate"], x["total_return_pct"], x["total_trades"]),
            reverse=True
        )
    else:
        leaderboard.sort(
            key=lambda x: (x["total_return_pct"], x["win_rate"], x["total_trades"]),
            reverse=True
        )

    for idx, item in enumerate(leaderboard):
        item["rank"] = idx + 1

    active_items = [x for x in leaderboard if x["total_trades"] > 0]
    pool_avg_win_rate = round(
        sum(x["win_rate"] for x in active_items) / len(active_items), 2
    ) if active_items else 0.0
    pool_avg_total_return = round(
        sum(x["total_return_pct"] for x in active_items) / len(active_items), 2
    ) if active_items else 0.0
    best_stock = leaderboard[0] if leaderboard else None

    return {
        "strategy": strategy,
        "period": period,
        "exit_mode": exit_mode,
        "sort_by": sort_by,
        "summary": {
            "scanned_count": len(leaderboard),
            "active_count": len(active_items),
            "pool_avg_win_rate": pool_avg_win_rate,
            "pool_avg_total_return": pool_avg_total_return,
            "best_symbol": best_stock["symbol"] if best_stock else None,
            "best_name": best_stock["name"] if best_stock else None,
            "best_return_pct": best_stock["total_return_pct"] if best_stock else 0.0,
            "best_win_rate": best_stock["win_rate"] if best_stock else 0.0
        },
        "leaderboard": leaderboard
    }


@app.get("/api/backtest/{symbol}")
def backtest_stock_strategy(
    symbol: str,
    strategy: str = Query("entanglement", description="回測策略: entanglement(均線糾結突破), golden_cross(底部起漲金叉), pullback(多頭回測支撐), sop_all(SOP綜合)"),
    period: str = Query("6mo", description="回測統計區間: 6mo (過去半年) 或 1y (過去一年)"),
    exit_mode: str = Query("ma10_trail", description="出場策略: ma10_trail (破10日線移動停利) 或 fixed_rr (固定2R風報比)")
):
    """林穎《學會走圖SOP》單一個股策略歷史回測 API"""
    try:
        eval_bars = 252 if period == "1y" else 126
        fetch_period = "2y" if period == "1y" else "1y"
        res = fetch_and_backtest_single(symbol, strategy, eval_bars, fetch_period, exit_mode)
        if res is None:
            raise HTTPException(status_code=404, detail=f"找不到 {symbol} 的歷史回測資料")
        return {
            "strategy": strategy,
            "period": period,
            "exit_mode": exit_mode,
            **res
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/position-size")
def get_position_size(
    capital: float = Query(1000000.0, description="總資金"),
    risk_pct: float = Query(2.0, description="單筆最大可承受虧損百分比 (如 2.0 代表 2%)"),
    entry_price: float = Query(..., description="預計進場價格"),
    stop_loss: float = Query(..., description="防守停損價格"),
    target_price: Optional[float] = Query(None, description="波段目標價格 (選填)"),
    market: str = Query("TW", description="市場別: TW 或 US")
):
    """林穎《學會走圖SOP》風險控管與部位計算機 API"""
    try:
        return calculate_position_sizing(
            capital=capital,
            risk_pct=risk_pct,
            entry_price=entry_price,
            stop_loss=stop_loss,
            target_price=target_price,
            market=market
        )
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/search")
def search_stocks(q: str = Query("", description="搜尋台股或美股代號、中文名稱、產業關鍵字")):
    """搜尋全台股(上市/上櫃/ETF)與美股"""
    keyword = q.strip()
    if not keyword:
        return BUILTIN_STOCKS[:20]

    kw_upper = keyword.upper()
    results: List[Dict[str, Any]] = []
    seen = set()

    for item in BUILTIN_STOCKS:
        sym = item["symbol"]
        name = item["name"]
        cats = " ".join(item["categories"])
        if kw_upper in sym.upper() or keyword in name or kw_upper in name.upper() or keyword in cats:
            results.append({
                "symbol": sym,
                "query_symbol": f"{sym}{item['suffix']}",
                "name": name,
                "market": item["market"],
                "categories": item["categories"]
            })
            seen.add(sym)

    if len(results) < 5:
        ext_list = search_external_stocks(keyword)
        for ext in ext_list:
            if ext["symbol"] not in seen:
                results.append(ext)
                seen.add(ext["symbol"])

    clean_code = kw_upper.replace(".TW", "").replace(".TWO", "")
    if not results and len(clean_code) >= 1 and clean_code.isalnum():
        is_tw = clean_code.isdigit() or (len(clean_code) >= 4 and clean_code[:4].isdigit())
        results.append({
            "symbol": clean_code,
            "query_symbol": f"{clean_code}.TW" if is_tw else clean_code,
            "name": f"{clean_code} (直接載入)",
            "market": "TW" if is_tw else "US",
            "categories": ["自選搜尋"]
        })

    return results[:15]


@app.get("/api/screener")
def stock_screener(
    strategy: str = Query("all", description="策略過濾: all, entanglement, golden_cross, pullback, bearish"),
    symbols: Optional[str] = Query(None, description="自訂掃描代號清單，以逗號分隔")
):
    """林穎走圖 SOP 多策略智慧選股引擎"""
    if symbols:
        scan_list = [s.strip() for s in symbols.split(",") if s.strip()]
    else:
        scan_list = [
            "2330", "2303", "2345", "2327", "2317", "2454",
            "2382", "3231", "2308", "3017", "6223", "2603",
            "2881", "0050", "NVDA", "AAPL", "AMD", "AVGO", "TSLA"
        ]

    results = []
    for raw_sym in scan_list:
        try:
            info = resolve_symbol_info(raw_sym)
            query_sym = info["query_symbol"]

            cache_key = f"screener_df_{query_sym}"
            now = time.time()
            if cache_key in _CACHE and (now - _CACHE[cache_key]["ts"] < CACHE_TTL_KBARS):
                df = _CACHE[cache_key]["data"]
            else:
                ticker = yf.Ticker(query_sym)
                df = ticker.history(period="4mo", interval="1d")
                if df.empty and info["market"] == "TW" and query_sym.endswith(".TW"):
                    alt_sym = f"{info['symbol']}.TWO"
                    df = yf.Ticker(alt_sym).history(period="4mo", interval="1d")
                    if not df.empty:
                        query_sym = alt_sym
                if not df.empty:
                    df = compute_technical_indicators(df)
                    _CACHE[cache_key] = {"ts": now, "data": df}

            if df is None or len(df) < 25:
                continue

            sop_eval = evaluate_lin_ying_sop(df, market=info["market"])
            latest = df.iloc[-1]
            prev = df.iloc[-2]
            close_val = safe_float(latest["Close"], 2, 0.0)
            prev_close = safe_float(prev["Close"], 2, close_val)
            change_pct = round((close_val - prev_close) / prev_close * 100.0, 2) if prev_close else 0.0

            patterns = sop_eval["patterns"]
            sop_score = sop_eval["sop_score"]

            matched = False
            if strategy == "all":
                matched = len(patterns) > 0 or sop_score >= 4
            elif strategy == "entanglement":
                matched = "均線糾結帶量突破" in patterns or (sop_eval["ma_spread_pct"] <= 4.0 and sop_score >= 4)
            elif strategy == "golden_cross":
                matched = "底部起漲黃金交叉" in patterns
            elif strategy == "pullback":
                matched = "多頭排列回測支撐" in patterns
            elif strategy == "bearish":
                matched = "高檔爆量長黑轉弱" in patterns
            else:
                matched = len(patterns) > 0 or sop_score >= 4

            if matched:
                primary_pattern = patterns[0] if patterns else f"SOP評分 {sop_score}/6 達標"
                pos_plan = sop_eval.get("position_plan") or {}
                results.append({
                    "symbol": info["symbol"],
                    "name": info["name"],
                    "market": info["market"],
                    "close": close_val,
                    "change_pct": change_pct,
                    "pattern": primary_pattern,
                    "patterns": patterns if patterns else [primary_pattern],
                    "sop_score": sop_score,
                    "criteria": sop_eval["criteria"],
                    "volume_ratio": sop_eval["volume_ratio"],
                    "ma_spread_pct": sop_eval["ma_spread_pct"],
                    "rsi": sop_eval["rsi"],
                    "k": sop_eval["k"],
                    "d": sop_eval["d"],
                    "stop_loss": sop_eval["stop_loss"],
                    "target_price": sop_eval["target_price"],
                    "suggested_shares": pos_plan.get("suggested_shares"),
                    "rr_ratio": pos_plan.get("rr_ratio")
                })
        except Exception as e:
            print(f"[Screener] {raw_sym} 略過: {e}")

    results.sort(key=lambda x: (x["sop_score"], x["volume_ratio"]), reverse=True)
    return results


@app.get("/api/quote/{symbol}")
def get_quote(symbol: str):
    """取得個股最新即時報價摘要"""
    now = time.time()
    cache_key = f"quote_{symbol.upper()}"
    if cache_key in _CACHE and (now - _CACHE[cache_key]["ts"] < CACHE_TTL_QUOTE):
        return _CACHE[cache_key]["data"]

    try:
        info = resolve_symbol_info(symbol)
        query_sym = info["query_symbol"]
        ticker = yf.Ticker(query_sym)
        hist = ticker.history(period="5d")

        if hist.empty and info["market"] == "TW" and query_sym.endswith(".TW"):
            alt_sym = f"{info['symbol']}.TWO"
            hist = yf.Ticker(alt_sym).history(period="5d")
            if not hist.empty:
                query_sym = alt_sym

        if hist.empty:
            raise HTTPException(status_code=404, detail=f"找不到代號或名稱 {symbol} 的報價資料")

        latest = hist.iloc[-1]
        prev_close = float(hist.iloc[-2]["Close"]) if len(hist) > 1 else float(latest["Open"])
        close = float(latest["Close"])
        change = close - prev_close
        change_pct = (change / prev_close * 100.0) if prev_close else 0.0

        payload = {
            "symbol": info["symbol"],
            "name": info["name"],
            "market": info["market"],
            "categories": info["categories"],
            "query_symbol": query_sym,
            "open": safe_float(latest["Open"], 2, close),
            "high": safe_float(latest["High"], 2, close),
            "low": safe_float(latest["Low"], 2, close),
            "close": safe_float(close, 2, 0.0),
            "change": safe_float(change, 2, 0.0),
            "change_pct": safe_float(change_pct, 2, 0.0),
            "volume": int(latest["Volume"]) if pd.notna(latest["Volume"]) else 0,
        }
        _CACHE[cache_key] = {"ts": now, "data": payload}
        return payload
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/kbars/{symbol}")
def get_kbars(symbol: str, period: str = "6mo", interval: str = "1d"):
    """取得歷史 K 棒、技術指標、SOP 診斷與預設部位計算資訊"""
    now = time.time()
    cache_key = f"kbars_{symbol.upper()}_{period}_{interval}"
    if cache_key in _CACHE and (now - _CACHE[cache_key]["ts"] < CACHE_TTL_KBARS):
        return _CACHE[cache_key]["data"]

    try:
        info = resolve_symbol_info(symbol)
        query_sym = info["query_symbol"]

        effective_period = period
        if interval in ("5m", "15m", "30m") and period in ("3mo", "6mo", "1y"):
            effective_period = "1mo"
        elif interval == "60m" and period in ("1y", "2y"):
            effective_period = "6mo"

        ticker = yf.Ticker(query_sym)
        df = ticker.history(period=effective_period, interval=interval)

        if df.empty and info["market"] == "TW" and query_sym.endswith(".TW"):
            alt_sym = f"{info['symbol']}.TWO"
            df = yf.Ticker(alt_sym).history(period=effective_period, interval=interval)
            if not df.empty:
                query_sym = alt_sym

        if df.empty:
            raise HTTPException(status_code=404, detail=f"找不到 {symbol} 的歷史 K 棒資料")

        df = compute_technical_indicators(df)
        sop_summary = evaluate_lin_ying_sop(df, market=info["market"])

        kbars = []
        prev_ma5, prev_ma20 = None, None
        for idx, row in df.iterrows():
            c_val = safe_float(row["Close"], 2, 0.0)
            o_val = safe_float(row["Open"], 2, c_val)
            h_val = safe_float(row["High"], 2, c_val)
            l_val = safe_float(row["Low"], 2, c_val)
            v_val = int(row["Volume"]) if pd.notna(row["Volume"]) else 0

            ma5_v = safe_float(row.get("MA5"), 2, None)
            ma10_v = safe_float(row.get("MA10"), 2, None)
            ma20_v = safe_float(row.get("MA20"), 2, None)
            ma60_v = safe_float(row.get("MA60"), 2, None)
            vol_ma20 = safe_float(row.get("VolMA20"), 2, None)

            signal_marker = None
            if prev_ma5 is not None and prev_ma20 is not None and ma5_v is not None and ma20_v is not None:
                if prev_ma5 <= prev_ma20 and ma5_v > ma20_v and c_val > o_val:
                    signal_marker = "GOLDEN_CROSS"
                elif prev_ma5 >= prev_ma20 and ma5_v < ma20_v and c_val < o_val:
                    signal_marker = "DEATH_CROSS"
            if signal_marker is None and vol_ma20 and v_val > vol_ma20 * 1.8 and c_val > o_val * 1.02:
                signal_marker = "VOL_BREAKOUT"

            prev_ma5, prev_ma20 = ma5_v, ma20_v

            kbars.append({
                "time": int(idx.timestamp()),
                "open": o_val,
                "high": h_val,
                "low": l_val,
                "close": c_val,
                "volume": v_val,
                "ma5": ma5_v,
                "ma10": ma10_v,
                "ma20": ma20_v,
                "ma60": ma60_v,
                "bb_upper": safe_float(row.get("BB_Upper"), 2, None),
                "bb_lower": safe_float(row.get("BB_Lower"), 2, None),
                "rsi": safe_float(row.get("RSI"), 2, 50.0),
                "k": safe_float(row.get("K"), 2, 50.0),
                "d": safe_float(row.get("D"), 2, 50.0),
                "macd": safe_float(row.get("MACD"), 3, 0.0),
                "macd_signal": safe_float(row.get("MACD_Signal"), 3, 0.0),
                "macd_hist": safe_float(row.get("MACD_Hist"), 3, 0.0),
                "signal": signal_marker
            })

        if kbars:
            kbars[-1]["sop"] = sop_summary
            kbars[-1]["symbol_name"] = info["name"]
            kbars[-1]["market"] = info["market"]

        _CACHE[cache_key] = {"ts": now, "data": kbars}
        return kbars
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run("server:app", host="127.0.0.1", port=8000, reload=True)