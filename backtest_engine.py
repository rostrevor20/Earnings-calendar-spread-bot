import backtest_historical_calendar as h_cal
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, date
import config
from scanner import yang_zhang, build_term_structure
from polygon import RESTClient
import time
import numpy as np

def get_historical_spread_price(client: RESTClient, ticker: str, trade_datetime: datetime, strike: float, short_expiry: date, long_expiry: date):
    """
    Gets the historical price of a calendar spread at a specific minute in time.
    """
    try:
        def format_option_ticker(underlying, expiry, right, strike):
            return f"O:{underlying.upper()}{expiry.strftime('%y%m%d')}{right[0].upper()}{str(int(strike * 1000)).zfill(8)}"

        short_ticker = format_option_ticker(ticker, short_expiry, config.OPTION_TYPE, strike)
        long_ticker = format_option_ticker(ticker, long_expiry, config.OPTION_TYPE, strike)
        trade_timestamp_ms = int(trade_datetime.timestamp() * 1000)
        
        short_bar = client.get_aggs(short_ticker, 1, "minute", trade_timestamp_ms, trade_timestamp_ms, limit=1)
        time.sleep(4)
        long_bar = client.get_aggs(long_ticker, 1, "minute", trade_timestamp_ms, trade_timestamp_ms, limit=1)
        time.sleep(4)

        if short_bar and long_bar and short_bar[0].close is not None and long_bar[0].close is not None:
            return round(long_bar[0].close - short_bar[0].close, 2)
        return None
    except Exception:
        return None

def run_scanner_with_historical_data(ticker, scan_date, client):
    """
    Runs the full scanner logic using the efficient unique expiration date method.
    """
    print(f"  - Scanning on {scan_date}...")
    try:
        start_of_history = scan_date - timedelta(days=400)
        aggs = client.get_aggs(ticker, 1, "day", start_of_history.strftime("%Y-%m-%d"), scan_date.strftime("%Y-%m-%d"))
        time.sleep(4)
        price_history = pd.DataFrame(aggs)
        if price_history.empty: return "Avoid", None
        price_history['datetime'] = pd.to_datetime(price_history['timestamp'], unit='ms')
        price_history = price_history.set_index('datetime')
        price_history.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
        if 'Adj Close' not in price_history.columns: price_history['Adj Close'] = price_history['Close']
        if len(price_history) < 30: return "Avoid", None
        
        avg_volume = price_history['Volume'].rolling(30).mean().iloc[-1]
        realized_vol_30d = yang_zhang(price_history)
        avg_volume_passed = avg_volume >= config.AVG_VOLUME_THRESHOLD
        if not avg_volume_passed: print(f"  - FAIL: Avg Volume"); return "Avoid", None
        
        dtes, ivs = [], []
        min_exp = scan_date + timedelta(days=5)
        max_exp = scan_date + timedelta(days=90)
        contracts = client.list_options_contracts(underlying_ticker=ticker, as_of=scan_date.strftime("%Y-%m-%d"), limit=1000)
        time.sleep(4)
        
        processed_expirations = set()
        max_unique_expirations = 6

        for contract in contracts:
            if len(processed_expirations) >= max_unique_expirations:
                break

            exp_date_str = contract.expiration_date
            if exp_date_str not in processed_expirations:
                time.sleep(4)
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
                if min_exp <= exp_date <= max_exp:
                    try:
                        bar = client.get_daily_open_close_agg(contract.ticker, scan_date.strftime("%Y-%m-%d"))
                        if bar and hasattr(bar, 'greeks') and bar.greeks.implied_volatility is not None:
                            dtes.append((exp_date - scan_date).days)
                            ivs.append(bar.greeks.implied_volatility)
                            processed_expirations.add(exp_date_str)
                    except Exception:
                        continue
        
        if len(dtes) < 2: print("  - FAIL: Not enough options data."); return "Avoid", None

        term_spline = build_term_structure(dtes, ivs)
        iv30 = float(term_spline(30))
        iv_rv_ratio = iv30 / realized_vol_30d if realized_vol_30d > 0 else float('inf')
        dte_start = min(dtes)
        ts_slope = (float(term_spline(45)) - float(term_spline(dte_start))) / (45 - dte_start) if (45 - dte_start) != 0 else 0
        
        iv_rv_passed = iv_rv_ratio >= config.IV_RV_RATIO_THRESHOLD
        slope_passed = ts_slope <= config.TERM_STRUCTURE_SLOPE_THRESHOLD
        
        if not iv_rv_passed: print(f"  - FAIL: IV/RV Ratio"); return "Avoid", None
        if not slope_passed: print(f"  - FAIL: Term Structure Slope"); return "Avoid", None
        
        print(f"    - Scanner Checks: PASS")
        return "Recommended", price_history
        
    except Exception as e:
        print(f"  - Scanner failed: {e}"); return "Avoid", None

def get_precise_trade_times(event_date, ticker):
    try:
        stock = yf.Ticker(ticker)
        calendar_data = stock.calendar
        if not isinstance(calendar_data, pd.DataFrame) or calendar_data.empty or 'Earnings Date' not in calendar_data.index: return None, None
        earnings_timestamp = calendar_data.loc['Earnings Date'][0]
        is_amc = earnings_timestamp.time() != datetime.min.time()
        price_reaction_day = earnings_timestamp.date() if not is_amc else earnings_timestamp.date() + timedelta(days=1)
        entry_day = price_reaction_day - timedelta(days=1)
        exit_day = price_reaction_day
        if entry_day.weekday() >= 5: entry_day -= timedelta(days=entry_day.weekday() - 4)
        if exit_day.weekday() >= 5: exit_day += timedelta(days=7 - exit_day.weekday())
        entry_datetime = config.MARKET_TIMEZONE.localize(datetime.combine(entry_day, datetime.min.time()) + timedelta(hours=15, minutes=45))
        exit_datetime = config.MARKET_TIMEZONE.localize(datetime.combine(exit_day, datetime.min.time()) + timedelta(hours=9, minutes=45))
        return entry_datetime, exit_datetime
    except Exception: return None, None

def run_backtest(start_date, end_date):
    client = RESTClient(api_key=config.POLYGON_API_KEY)
    tickers = h_cal.get_combined_universe_tickers()
    events = h_cal.get_historical_earnings_calendar(tickers, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    results = []
    
    initial_capital = 100000.00
    current_capital = initial_capital
    print(f"\n--- Starting Backtest with Initial Capital: ${initial_capital:,.2f} ---")

    for i, (event_date, ticker) in enumerate(events):
        print(f"\nProcessing event {i+1}/{len(events)}: {ticker} on {event_date}")
        scan_date = event_date - timedelta(days=1)
        if scan_date.weekday() >= 5: scan_date -= timedelta(days=scan_date.weekday() - 4)
        
        scan_result, price_history = run_scanner_with_historical_data(ticker, scan_date, client)

        if scan_result == "Recommended":
            print(f"  - Scanner Recommended. Simulating trade for {ticker}.")
            entry_datetime, exit_datetime = get_precise_trade_times(event_date, ticker)
            
            if entry_datetime and exit_datetime:
                atm_strike = round(price_history['Close'].iloc[-1])
                short_expiry = entry_datetime.date() + timedelta(days=20)
                long_expiry = entry_datetime.date() + timedelta(days=20 + config.EXPIRY_GAP_DAYS)
                entry_price = get_historical_spread_price(client, ticker, entry_datetime, atm_strike, short_expiry, long_expiry)
                
                if entry_price is not None and entry_price > 0:
                    risk_amount = current_capital * config.RISK_ALLOCATION_PERCENT
                    cost_per_spread = entry_price * 100
                    num_contracts = int(risk_amount // cost_per_spread) if cost_per_spread > 0 else 0
                    
                    if num_contracts > 0:
                        print(f"  - Sizing: Allocating ${risk_amount:,.2f} -> Trading {num_contracts} contracts.")
                        exit_price = get_historical_spread_price(client, ticker, exit_datetime, atm_strike, short_expiry, long_expiry)
                        
                        if exit_price is not None:
                            pnl_per_contract = (exit_price - entry_price) * 100
                            total_trade_pnl = pnl_per_contract * num_contracts
                            current_capital += total_trade_pnl
                            result = {"ticker": ticker, "exit_date": exit_datetime.date(), "trade_pnl": total_trade_pnl, "portfolio_end_balance": current_capital}
                            results.append(result)
                            print(f"  - TRADE RESULT: P&L = ${total_trade_pnl:,.2f}. New Capital: ${current_capital:,.2f}")
                        else: print("  - TRADE SKIPPED: Could not retrieve price for exit.")
                    else: print("  - TRADE SKIPPED: Not enough capital to size position.")
                else: print("  - TRADE SKIPPED: Could not retrieve price for entry.")
    return results, initial_capital

def calculate_performance_metrics(results_df, initial_capital, backtest_days):
    if results_df.empty:
        return 0, 0, 0
    
    portfolio_values = pd.Series([initial_capital] * len(backtest_days), index=backtest_days)
    results_df_no_tz = results_df.copy()
    if pd.api.types.is_datetime64_any_dtype(results_df_no_tz.index) and results_df_no_tz.index.tz is not None:
        results_df_no_tz.index = results_df_no_tz.index.tz_localize(None)

    for idx, trade in results_df_no_tz.iterrows():
        trade_date_naive = pd.to_datetime(idx).normalize()
        if trade_date_naive in portfolio_values.index:
            portfolio_values.loc[trade_date_naive:] += trade['trade_pnl']

    daily_returns = portfolio_values.pct_change().dropna()
    
    sharpe_ratio = daily_returns.mean() / daily_returns.std() if daily_returns.std() != 0 else 0
    annualized_sharpe = sharpe_ratio * np.sqrt(252)
    
    cumulative_max = portfolio_values.cummax()
    drawdown = (portfolio_values - cumulative_max) / cumulative_max
    max_drawdown_pct = drawdown.min()
    
    if max_drawdown_pct == 0:
        return annualized_sharpe, 0, 0

    trough_date = drawdown.idxmin()
    peak_date = portfolio_values.loc[:trough_date].idxmax()
    
    recovery_df = portfolio_values.loc[trough_date:]
    try:
        recovery_date = recovery_df[recovery_df >= portfolio_values[peak_date]].index[0]
        max_drawdown_duration = (recovery_date - peak_date).days
    except IndexError:
        max_drawdown_duration = "N/A (Did not recover)"
        
    return annualized_sharpe, max_drawdown_pct, max_drawdown_duration

if __name__ == "__main__":
    end_date = datetime.now().date()
    # --- THIS IS THE CORRECTED LINE ---
    start_date = end_date - timedelta(days=365)
    
    trade_results, starting_capital = run_backtest(start_date=start_date, end_date=end_date)
    
    print("\n--- Backtest Complete ---")
    if trade_results:
        results_df = pd.DataFrame(trade_results)
        results_df['exit_date'] = pd.to_datetime(results_df['exit_date'])
        results_df = results_df.set_index('exit_date').sort_index()

        backtest_days = pd.to_datetime(pd.bdate_range(start=start_date, end=end_date))
        sharpe, max_dd, dd_duration = calculate_performance_metrics(results_df, starting_capital, backtest_days)

        print("Backtest Results:")
        print(results_df)
        
        ending_capital = results_df['portfolio_end_balance'].iloc[-1] if not results_df.empty else starting_capital
        total_return_pct = ((ending_capital - starting_capital) / starting_capital) * 100
        
        print("\n--- Performance Summary ---")
        print(f"Period: {start_date} to {end_date}")
        print(f"Starting Capital:       ${starting_capital:,.2f}")
        print(f"Ending Capital:         ${ending_capital:,.2f}")
        print(f"Total Return:           {total_return_pct:.2f}%")
        print(f"Total Trades:           {len(results_df)}")
        print(f"Win Rate:               {(results_df['trade_pnl'] > 0).mean():.2%}")
        print(f"Average P&L per Trade:  ${results_df['trade_pnl'].mean():,.2f}")
        print("-" * 30)
        print(f"Annualized Sharpe Ratio:{sharpe:.2f}")
        print(f"Max Drawdown:           {max_dd:.2%}")
        print(f"Max Drawdown Duration:  {dd_duration} days")