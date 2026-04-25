import yfinance as yf
from datetime import datetime
from collections import defaultdict
import time

tickers = [

"NVDA","AAPL","WBD","AMZN","META","TSLA","INTC","MSFT","GOOGL","BAC",
"RIVN","MRK","AMD","TSM","WMT","META","GM","WFC","JD","AVGO",
"MDT","OXY","T","ORCL","PYPL","NKE","MU","DIS","MTCH","KO",
"JPM","PANW","FCX","PDD","IBM","XOM","PG","CMCSA","VZ","SNOW",
"CMG","JNJ","V","CRM","WMB","NOW","LCID","QCOM","ADBE","MRVL",
"CVX","AMAT","CSCO","GE","NEE","SLB","TTWO","CRWD","MNST","BSX",
"CSX","TXN","ABBV","ATVI","SCHW","KR","MO","MCD","COST","WDAY",
"HD","LLY","FDX","CAT","LOW","AZN","SO","TJX","KMB","MS",
"AXP","LRCX","RTX","SBUX","DDOG","PEP","BIDU","FANG","AIG","USB",
"VLO","GS","DOW","GEHC","ZS","UPS","EA","COF","MA","AMGN"
]

start = datetime(2023, 1, 1)
end   = datetime(2026, 2, 28)

results = defaultdict(list)

for ticker in tickers:
    print(f"Fetching {ticker}...", end=" ", flush=True)
    try:
        t = yf.Ticker(ticker)
        # limit=20 gives ~5 years of quarterly earnings (4 per year)
        df = t.get_earnings_dates(limit=20)

        if df is not None and not df.empty:
            for dt_idx in df.index:
                d = dt_idx.to_pydatetime().replace(tzinfo=None)
                # Only keep the date part (ignore time)
                d = datetime(d.year, d.month, d.day)
                if start <= d <= end:
                    results[ticker].append(d)

        results[ticker] = sorted(set(results[ticker]))
        print(f"{len(results[ticker])} dates found")
    except Exception as e:
        print(f"ERROR: {e}")

    time.sleep(0.5)   # be polite to Yahoo's servers

# ── Output in Python dict format ─────────────────────────────────────────────

print("\n\nMANUAL_EARNINGS_DATES = {")

for t in tickers:
    dates = results[t]
    if not dates:
        print(f'    "{t}": [],')
        continue
    print(f'    "{t}": [')
    for d in dates:
        print(f"        datetime({d.year}, {d.month}, {d.day}),")
    print("    ],")

print("}")
