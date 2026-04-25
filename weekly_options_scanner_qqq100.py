# region imports
from AlgorithmImports import *
from datetime import datetime, timedelta
# endregion

# ─── Config ───────────────────────────────────────────────────────────────────
START_YEAR = 2023        # first calendar year to scan (inclusive)
END_YEAR   = 2026        # last  calendar year to scan (inclusive)
DTE_TARGET = 28          # target days-to-expiry for the chosen weekly each month

# Set to True to re-enable in-run diagnostic + per-snapshot progress logs
# (useful when debugging). When False, only the final ranking table prints.
VERBOSE = False

TICKERS = [
   "NVDA","GOOGL","AAPL","MSFT","AMZN","AVGO","TSM","META","TSLA",
"WMT","JPM","LLY","XOM","V","JNJ","ASML","MU","ORCL","MA",
"COST","BAC","ABBV","HD","PG","CVX","MRK","KO","PEP","ADBE",
"CSCO","CRM","AMD","ACN","MCD","NKE","TMO","LIN","DHR","ABT",
"WFC","DIS","TXN","VZ","INTC","PM","NEE","RTX","UPS","UNP",
"LOW","HON","IBM","SPGI","CAT","GS","AMGN","INTU","PLD","ISRG",
"BLK","MDT","GE","AXP","SCHW","NOW","BKNG","DE","TJX","ADP",
"SYK","CI","ELV","MO","MMC","VRTX","CB","SO","LMT","DUK",
"REGN","ZTS","GILD","BDX","T","CSX","CME","PGR","ITW","USB",
"BSX","FDX","APD","EQIX","CL","NSC","HCA","ICE","EOG","AON",
"SHW","EMR","MCK","GD","FIS","ETN","WM","ROP","COF","FCX",
"PSA","SLB","OXY","MAR","AIG","TRV","AZO","MS","ORLY","NXPI",
"KMB","ADM","MET","PH","SRE","NOC","TT","DOW","PAYX","F",
"GM","HLT","PCAR","AEP","ALL","AMP","DFS","PRU","CMG","ROST",
"MNST","KDP","KR","CTAS","IDXX","YUM","GIS","HSY","EXC","VLO",
"DLR","WMB","OKE","PEG","FAST","BIIB","OTIS","AME","EA","RMD",
"TMUS","QCOM","INTU","AMAT","SBUX","ADI","LRCX","MDLZ","PANW",
"SNPS","KLAC","MELI","CDNS","FTNT","ABNB","WDAY","CPRT","XEL",
"ODFL","DLTR","VRSK","CSGP","TEAM","ZS","DDOG","ANSS","GEHC",
"BKR","GFS","ON","MRVL","MCHP","AZN","SIRI","WBD","CCEP",
"TTWO","ILMN","LCID","RIVN","JD","PDD","BIDU","NTES","TCOM",
"CHTR","CMCSA","ATVI","PYPL","EBAY","FANG","DXCM","CRWD",
"OKTA","ALGN","MTCH","DOCU","SNOW","MDB","HUBS"
]

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _is_monthly(dt):
    """True if dt is the 3rd Friday of its month (standard monthly expiry)."""
    if dt.weekday() != 4:
        return False
    return 15 <= dt.day <= 21


class QQQOpenInterestScanner(QCAlgorithm):

    def Initialize(self):
        self.SetStartDate(START_YEAR, 1, 1)
        self.SetEndDate(END_YEAR, 12, 31)
        self.SetCash(100_000)

        # Dedupe TICKERS while preserving first-seen order
        seen = set()
        self._tickers = []
        for t in TICKERS:
            if t not in seen:
                seen.add(t)
                self._tickers.append(t)

        # ── Equity-only subscriptions (no AddOption) ────────────────────
        # We deliberately do NOT subscribe to any option chains here.
        # Persistent option subscriptions (AddOption + SetFilter) accumulate
        # entries in QC's process-global SymbolCache for every contract that
        # ever passes the filter. Over a 4-year x 200-ticker run that pushed
        # SymbolCache past its memory ceiling and produced
        # System.OutOfMemoryException at SymbolCache.cs:45.
        #
        # Instead, _scan() uses self.OptionChainProvider.GetOptionContractList
        # to enumerate the chain on-demand (no SymbolCache growth), then
        # self.History(OpenInterest, [put_sym, call_sym], ...) to fetch the
        # OI values for just the two ATM contracts we actually need. No
        # option contract is ever subscribed; SymbolCache stays bounded by
        # the equity universe size; memory footprint is essentially flat
        # regardless of how many tickers or how many years are scanned.
        #
        # Equity uses Resolution.Hour: with Daily resolution, equity bars
        # arrive at end-of-day (~16:00 ET), so at the 10:30 AM scheduled
        # scan time self.Securities[ticker].Price is still 0 (no bar has
        # been delivered yet for the current day). Hour resolution gives us
        # the 10:00 bar before scan fires, so Price is populated. Equity
        # data is tiny (no chain explosion, negligible SymbolCache impact),
        # so the memory concern doesn't apply here.
        self._equity_symbols = {}
        for ticker in self._tickers:
            eq = self.AddEquity(ticker, Resolution.Hour)
            self._equity_symbols[ticker] = eq.Symbol

        # ── Accumulators ────────────────────────────────────────────────
        # ticker -> list of per-snapshot (put_oi + call_oi) / 2 values
        self._snapshots = {t: [] for t in self._tickers}
        self._latest_mcap = {}    # ticker -> most recent market cap seen
        self._latest_price = {}   # ticker -> most recent price seen
        self._snapshot_count = 0  # total monthly snapshots fired

        # Run on the 1st trading day of each month at 10:30 AM ET
        # (half-day-safe; well before any half-day close)
        self.Schedule.On(
            self.DateRules.MonthStart("AAPL"),
            self.TimeRules.AfterMarketOpen("AAPL", 60),
            self._scan,
        )

    def OnData(self, data):
        pass

    def _scan(self):
        # ── Stage 1: pick the ATM put + call symbol for each ticker ─────
        # Uses self.OptionChainProvider to enumerate the chain on-demand
        # without ever subscribing to a contract. Symbol metadata
        # (expiry, strike, right) is read directly from Symbol.ID, so no
        # data fetch is needed in this stage at all.
        today = self.Time.date()
        target_date = today + timedelta(days=DTE_TARGET)

        # One-shot diagnostic: emit detailed Debug lines for the very first
        # snapshot only so we can see exactly where Stage 1 falls off if no
        # tickers come back with weeklies. Gated on VERBOSE so production
        # runs don't pay the log-volume cost.
        diag = VERBOSE and (self._snapshot_count == 0)

        # ticker -> (put_sym, call_sym) pairs we want OI for
        ticker_to_pair = {}
        # Flat list of every symbol we'll batch-fetch OI for in stage 2
        all_oi_symbols = []

        for ticker in self._tickers:
            equity_sym = self._equity_symbols.get(ticker)
            if equity_sym is None:
                if diag:
                    self.Debug(f"[OI-DIAG] {ticker}: no equity_sym registered")
                continue

            equity = self.Securities[ticker]
            s_price = equity.Price
            if s_price <= 0:
                if diag:
                    self.Debug(f"[OI-DIAG] {ticker}: price=0 (Daily bar not yet ready?)")
                continue

            # Track latest price + market cap (used in the final ranking)
            try:
                if equity.Fundamentals is not None:
                    mcap = equity.Fundamentals.MarketCap or 0.0
                    if mcap > 0:
                        self._latest_mcap[ticker] = mcap
            except Exception:
                pass
            self._latest_price[ticker] = s_price

            # Enumerate the chain WITHOUT subscribing to anything
            chain_syms = self.OptionChainProvider.GetOptionContractList(
                equity_sym, self.Time
            )
            if chain_syms is None:
                if diag:
                    self.Debug(f"[OI-DIAG] {ticker}: GetOptionContractList returned None")
                continue

            # Materialise so we can count and re-iterate
            chain_list = list(chain_syms)
            if diag:
                self.Debug(
                    f"[OI-DIAG] {ticker}: chain returned {len(chain_list)} contracts; "
                    f"equity_sym={equity_sym} price={s_price:.2f}"
                )

            # Filter to weekly expiries within the DTE window
            candidates = []
            for sym in chain_list:
                exp = sym.ID.Date.date()
                dte = (exp - today).days
                if dte < 21 or dte > 35:
                    continue
                if _is_monthly(exp):
                    continue
                candidates.append(sym)

            if diag:
                # Show a few sample contract DTEs so we can see if the
                # chain has the right shape but our filter is too tight.
                sample_dtes = sorted({(s.ID.Date.date() - today).days
                                      for s in chain_list})[:15]
                self.Debug(
                    f"[OI-DIAG] {ticker}: {len(candidates)} candidates passed "
                    f"weekly+DTE filter (21..35d); sample DTEs in chain: {sample_dtes}"
                )

            if not candidates:
                continue

            # Pick the expiry closest to today + DTE_TARGET
            expiries = sorted(set(s.ID.Date.date() for s in candidates))
            chosen_expiry = min(
                expiries, key=lambda e: abs((e - target_date).days)
            )
            at_expiry = [
                s for s in candidates if s.ID.Date.date() == chosen_expiry
            ]
            if not at_expiry:
                continue

            # ATM strike at the chosen expiry
            strikes = sorted(set(float(s.ID.StrikePrice) for s in at_expiry))
            atm_strike = min(strikes, key=lambda k: abs(k - s_price))

            put_sym = None
            call_sym = None
            for s in at_expiry:
                if float(s.ID.StrikePrice) != atm_strike:
                    continue
                if s.ID.OptionRight == OptionRight.Put:
                    put_sym = s
                elif s.ID.OptionRight == OptionRight.Call:
                    call_sym = s

            if put_sym is None or call_sym is None:
                if diag:
                    self.Debug(
                        f"[OI-DIAG] {ticker}: missing leg at ATM strike "
                        f"{atm_strike} for expiry {chosen_expiry}: "
                        f"put={put_sym} call={call_sym}"
                    )
                continue

            if diag:
                self.Debug(
                    f"[OI-DIAG] {ticker}: chosen expiry={chosen_expiry} "
                    f"ATM strike={atm_strike} put={put_sym} call={call_sym}"
                )

            ticker_to_pair[ticker] = (put_sym, call_sym)
            all_oi_symbols.append(put_sym)
            all_oi_symbols.append(call_sym)

        # ── Stage 2: one batched History call for all ATM OI values ─────
        # Single History call avoids per-ticker overhead and lets QC's data
        # provider amortize the lookup cost across the whole snapshot.
        # Look back 5 trading days so we tolerate light/missing days and
        # still get the most recent OI print.
        oi_lookup = {}  # symbol -> latest OI value (int)
        if all_oi_symbols:
            try:
                oi_df = self.History(
                    OpenInterest, all_oi_symbols, 5, Resolution.Daily
                )
            except Exception as e:
                if VERBOSE:
                    self.Log(f"[OI-DIAG] History(OpenInterest) failed: {e}")
                oi_df = None

            if diag:
                if oi_df is None:
                    self.Log("[OI-DIAG] History(OpenInterest) returned None")
                else:
                    try:
                        self.Log(
                            f"[OI-DIAG] OI dataframe shape={oi_df.shape} "
                            f"columns={list(oi_df.columns)} "
                            f"empty={getattr(oi_df, 'empty', None)}"
                        )
                    except Exception as e:
                        self.Log(f"[OI-DIAG] OI dataframe inspect failed: {e}")

            if oi_df is not None and not getattr(oi_df, "empty", True):
                # NOTE on indexing: QC returns a multi-index DataFrame
                # (symbol, time) -> openinterest, but using `.loc[sym]` on
                # the QC-wrapped frame can raise InvalidIndexError that
                # bypasses normal try/except (the PandasMapper wrapper
                # re-raises into the runtime). So we never use .loc[sym].
                # Instead, iterate the frame and bucket by str(symbol).
                #
                # Also: column casing has been seen as both 'openinterest'
                # and 'OpenInterest' across QC versions, so we resolve the
                # column name once at the top.
                col_name = None
                for c in ("openinterest", "OpenInterest"):
                    if c in oi_df.columns:
                        col_name = c
                        break

                if col_name is None:
                    if VERBOSE:
                        self.Log(
                            f"[OI-DIAG] OI column not found. "
                            f"Columns present: {list(oi_df.columns)}"
                        )
                else:
                    # Stringify all requested symbols once for fast lookup.
                    wanted = {str(s) for s in all_oi_symbols}

                    # Find which level of the multi-index holds the Symbol.
                    # QC's OpenInterest history returns a 5-level index
                    # ['expiry', 'strike', 'type', 'symbol', 'time']
                    # rather than the typical (symbol, time) layout used for
                    # equity / quote history. Resolve by name so we work
                    # correctly across both index shapes.
                    sym_level = 0
                    try:
                        idx_names = list(oi_df.index.names or [])
                        if "symbol" in idx_names:
                            sym_level = idx_names.index("symbol")
                    except Exception:
                        sym_level = 0

                    if diag:
                        try:
                            self.Log(
                                f"[OI-DIAG] OI df index names={oi_df.index.names} "
                                f"sym_level={sym_level} "
                                f"first_row={oi_df.iloc[0].to_dict()}"
                            )
                        except Exception as e:
                            self.Log(f"[OI-DIAG] df introspection failed: {e}")

                    # Walk the frame; keep the latest value per symbol.
                    matched = 0
                    unmatched = 0
                    try:
                        for idx, row in oi_df.iterrows():
                            if isinstance(idx, tuple):
                                sym_obj = idx[sym_level] if sym_level < len(idx) else idx[-1]
                            else:
                                sym_obj = idx
                            sym_key = str(sym_obj)
                            if sym_key not in wanted:
                                unmatched += 1
                                continue
                            matched += 1
                            try:
                                val = int(row[col_name])
                            except Exception:
                                continue
                            # iterrows is in chronological order, so a later
                            # write naturally overwrites with the latest OI.
                            oi_lookup[sym_key] = val
                    except Exception as e:
                        if VERBOSE:
                            self.Log(
                                f"[OI-DIAG] OI iteration failed: {e}"
                            )

                    if diag:
                        self.Log(
                            f"[OI-DIAG] iteration: matched={matched} "
                            f"unmatched={unmatched} "
                            f"oi_lookup_size={len(oi_lookup)}"
                        )

        # ── Stage 3: assemble per-ticker average OI from the batched fetch ──
        n_with_weeklies = 0
        for ticker, (put_sym, call_sym) in ticker_to_pair.items():
            put_oi = oi_lookup.get(str(put_sym), 0)
            call_oi = oi_lookup.get(str(call_sym), 0)
            # Skip tickers where we got no OI data at all (avoid polluting
            # the mean with zeros from missing-data symbols).
            if put_oi == 0 and call_oi == 0:
                continue
            avg_oi = (put_oi + call_oi) / 2.0
            self._snapshots[ticker].append(avg_oi)
            n_with_weeklies += 1

        self._snapshot_count += 1
        if VERBOSE:
            self.Log(f"[OI-SCAN] Snapshot {self._snapshot_count} at {today}: "
                     f"{n_with_weeklies} of {len(self._tickers)} tickers had weeklies "
                     f"(target DTE={DTE_TARGET}d)")

    def OnEndOfAlgorithm(self):
        # Build per-ticker stats for tickers that produced at least one snapshot
        results = []
        for ticker, snaps in self._snapshots.items():
            if not snaps:
                continue
            n = len(snaps)
            mean_oi = sum(snaps) / n
            min_oi  = min(snaps)
            max_oi  = max(snaps)
            results.append({
                "ticker":  ticker,
                "mcap":    self._latest_mcap.get(ticker, 0.0),
                "price":   self._latest_price.get(ticker, 0.0),
                "mean_oi": mean_oi,
                "min_oi":  min_oi,
                "max_oi":  max_oi,
                "n":       n,
            })

        # Sort by mean OI descending
        results.sort(key=lambda r: r["mean_oi"], reverse=True)

        lines = []
        lines.append(f"OI History Scanner — Period: {START_YEAR}..{END_YEAR}  "
                     f"(target DTE={DTE_TARGET}d)")
        lines.append(f"Total monthly snapshots fired: {self._snapshot_count}  |  "
                     f"Tickers with at least one weekly snapshot: {len(results)}  |  "
                     f"Tickers scanned: {len(self._tickers)}")
        lines.append("")

        hdr = (f"{'#':>3} | {'Ticker':<6} | {'MktCap':>8} | {'Price':>9} | "
               f"{'Mean OI':>10} | {'Min OI':>10} | {'Max OI':>10} | {'n':>4}")
        lines.append(hdr)
        lines.append("-" * len(hdr))

        for i, r in enumerate(results, 1):
            mcap_b = round(r['mcap'] / 1_000_000_000) if r['mcap'] > 0 else 0
            mcap_s = f"${mcap_b:>7,}" if mcap_b > 0 else f"{'n/a':>8}"
            lines.append(
                f"{i:>3} | {r['ticker']:<6} | {mcap_s} | ${r['price']:>8.2f} | "
                f"{r['mean_oi']:>10,.0f} | {r['min_oi']:>10,.0f} | "
                f"{r['max_oi']:>10,.0f} | {r['n']:>4}"
            )

        lines.append("-" * len(hdr))
        lines.append(f"Tickers without any weekly snapshot in period: "
                     f"{len(self._tickers) - len(results)}")

        for line in lines:
            self.Log(line)

        self.ObjectStore.Save("oi_history_scan", "\n".join(lines))
        self.Log(f"\nSaved to ObjectStore key 'oi_history_scan' ({len(lines)} lines)")
