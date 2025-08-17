# fetch_polygon_options.py
import os
import requests
import pandas as pd

API_KEY = os.getenv("POLYGON_API_KEY")
UNDERLYING = os.getenv("TICKER", "SPY")

BASE = "https://api.polygon.io"


def get_prev_close(ticker: str) -> float | None:
    url = f"{BASE}/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={API_KEY}"
    r = requests.get(url); r.raise_for_status()
    data = r.json()
    res = data.get("results") or []
    return float(res[0]["c"]) if res else None


def fetch_options_snapshot(ticker: str) -> pd.DataFrame:
    url = f"{BASE}/v3/snapshot/options/{ticker}?apiKey={API_KEY}"
    r = requests.get(url); r.raise_for_status()
    data = r.json()
    rows = []

    for item in data.get("results", []) or []:
        det = (item or {}).get("details", {}) or {}
        rows.append({
            "ticker": ticker,
            "contract_symbol": det.get("ticker"),
            "expiration_date": det.get("expiration_date"),
            "strike_price": det.get("strike_price"),
            "option_type_raw": det.get("type"),  # often None on snapshot
            "underlying_price": item.get("underlying_price"),  # will backfill
            "last_quote_bid": (item.get("last_quote") or {}).get("bid"),
            "last_quote_ask": (item.get("last_quote") or {}).get("ask"),
            "volume": det.get("volume"),
            "open_interest": det.get("open_interest"),
        })
    return pd.DataFrame(rows)


# 2) Parse option_type from OCC symbol if missing
def parse_option_type_from_occ(sym: str) -> str | None:
    try:
        core = sym.split(":")[-1]
        cp = core[9]  # 10th char
        return "call" if cp == "C" else "put"
    except Exception:
        return None


if __name__ == "__main__":
    df = fetch_options_snapshot(UNDERLYING)
    print(df)

    # Backfill underlying_price with previous close when missing (weekends/holidays)
    prev = get_prev_close(UNDERLYING)
    print(prev)
 
    # force numeric dtypes before filling
    numeric_cols = ["underlying_price", "last_quote_bid", "last_quote_ask",
                    "strike_price", "volume", "open_interest"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # now fill missing safely
    if prev is not None:
        df.loc[df["underlying_price"].isna(), "underlying_price"] = float(prev)

    df["volume"] = df["volume"].fillna(0).astype("int64", errors="ignore")
    df["open_interest"] = df["open_interest"].fillna(0).astype("int64", errors="ignore")
    print(df)


    df["option_type"] = df["option_type_raw"]
    mask_missing = df["option_type"].isna()
    df.loc[mask_missing, "option_type"] = df.loc[mask_missing, "contract_symbol"].apply(parse_option_type_from_occ)
    print(df)

    # sanity print
    print(df.head(10)[["ticker","contract_symbol","strike_price","option_type","underlying_price","volume","open_interest"]])

    df.to_csv("raw_options_data.csv", index=False)
    print(f"Saved {len(df)} rows to raw_options_data.csv (fallback underlying_price={prev})")