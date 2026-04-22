import yfinance as yf
from datetime import datetime
from collections import defaultdict
import time

tickers = [
"NIO", "F", "BAC", "GE", "MDT", "PLUG", "WMB", "V", "AAPL", "MSFT", "FCX", "META", "MU", "SIRI", "SLB", "TSLA", "EMR", "KO", "ABBV", "JNJ", "CVX", "NVDA", "EBAY", "MCD", "MRVL", "CSCO", "JPM", "DAL", "AMD", "ELV", "PDD", "MRK", "WFC", "QCOM", "INTC", "XOM", "ACN", "HUYA", "MO", "ADM", "AXP", "FDX", "CRM", "TGT", "PTON", "WMT", "OXY", "ADI", "UAL", "KR", "PYPL", "SBUX", "IBM", "KHC", "NFLX", "ABT", "CRWD", "DE", "PNC", "HD", "WDAY", "ORCL", "SQ", "TEAM", "CAT", "CME", "GLW", "NXPI", "TTD", "BRK.B", "CL", "NSC", "CMCSA", "AIG", "COST", "UPS", "ROKU", "PM", "LULU", "JD", "GILD", "PG", "USB", "LOW", "AMZN", "TXN", "MPC", "DHI", "VLO", "UNH", "MDLZ", "ADP", "CSX", "DD", "YUM", "IQ", "MET", "HES", "LLY", "GS", "CTSH", "LMT", "ADBE", "HON", "OKTA", "SWKS", "AVGO", "PANW", "MA", "BIIB", "BYND", "PEP", "AMGN", "GOOGL", "EOG", "MNST", "RIOT", "AMAT", "CI", "SHOP", "SO", "UNP", "ULTA", "EA", "ED", "ZM", "ETSY", "STZ", "CHWY", "AFL", "VRTX", "ILMN"
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
