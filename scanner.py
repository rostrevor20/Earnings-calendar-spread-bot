import yfinance as yf
from datetime import datetime, timedelta
from scipy.interpolate import interp1d
import numpy as np
import pandas as pd
import requests
import csv
import io
import config # Import the new config file

def filter_dates(dates):
    """Finds expiration dates between today and 45 days out."""
    today = datetime.today().date()
    cutoff_date = today + timedelta(days=45)
    
    valid_dates = []
    for date_str in dates:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
        if today < date_obj <= cutoff_date:
            valid_dates.append(date_str)
            
    if not valid_dates:
        raise ValueError("No suitable option expiration dates found within the next 45 days.")
    return sorted(valid_dates)

def yang_zhang(price_data, window=30, trading_periods=252):
    """Calculates the Yang-Zhang volatility."""
    log_ho = (price_data['High'] / price_data['Open']).apply(np.log)
    log_lo = (price_data['Low'] / price_data['Open']).apply(np.log)
    log_co = (price_data['Close'] / price_data['Open']).apply(np.log)
    log_oc = (price_data['Open'] / price_data['Close'].shift(1)).apply(np.log)
    log_oc_sq = log_oc**2
    log_cc = (price_data['Close'] / price_data['Close'].shift(1)).apply(np.log)
    log_cc_sq = log_cc**2
    rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)
    close_vol = log_cc_sq.rolling(window=window).sum() * (1.0 / (window - 1.0))
    open_vol = log_oc_sq.rolling(window=window).sum() * (1.0 / (window - 1.0))
    window_rs = rs.rolling(window=window).sum() * (1.0 / (window - 1.0))
    k = 0.34 / (1.34 + ((window + 1) / (window - 1)))
    result = (open_vol + k * close_vol + (1 - k) * window_rs).apply(np.sqrt) * np.sqrt(trading_periods)
    return result.iloc[-1]

def build_term_structure(days, ivs):
    """Builds a spline for the IV term structure."""
    days = np.array(days)
    ivs = np.array(ivs)
    sort_idx = days.argsort()
    days, ivs = days[sort_idx], ivs[sort_idx]
    if len(days) < 2:
        return lambda dte: ivs[0] if len(ivs) > 0 else 0
    spline = interp1d(days, ivs, kind='linear', fill_value="extrapolate")
    return spline

def get_average_historical_earnings_move(stock, price_history):
    """Calculates the average absolute price move on the day following the last 8 earnings announcements."""
    try:
        earnings_dates = stock.earnings_dates
        if earnings_dates is None or earnings_dates.empty: return None
        earnings_dates.index = pd.to_datetime(earnings_dates.index, utc=True)
        price_history.index = pd.to_datetime(price_history.index, utc=True)
        recent_earnings_dates = earnings_dates.index.dropna().sort_values(ascending=False)[:8]
        moves = []
        for date in recent_earnings_dates:
            try:
                loc_t0 = price_history.index.get_loc(date, method='ffill')
                if loc_t0 + 1 < len(price_history):
                    price_t0 = price_history.iloc[loc_t0]['Close']
                    price_t1 = price_history.iloc[loc_t0 + 1]['Close']
                    if price_t0 > 0: moves.append(abs((price_t1 - price_t0) / price_t0))
            except KeyError: continue
        return np.mean(moves) if moves else None
    except Exception: return None

def check_for_macro_events():
    """Checks for major upcoming economic events using the Alpha Vantage API."""
    api_key = config.ALPHA_VANTAGE_API_KEY
    days_away_threshold = config.MACRO_EVENT_DAYS_AWAY
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        return True, "API key not set for macro check"
    IMPORTANT_EVENTS = ["FOMC", "CPI", "Retail Sales", "Non-Farm Payrolls", "GDP", "Unemployment Rate"]
    url = f'https://www.alphavantage.co/query?function=ECONOMIC_CALENDAR&horizon=3month&apikey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        csv_file = io.StringIO(response.text)
        reader = csv.DictReader(csv_file)
        today = datetime.now().date()
        for row in reader:
            event_name = row.get('event')
            if any(important_event in event_name for important_event in IMPORTANT_EVENTS):
                event_date_str = row.get('date')
                if not event_date_str: continue
                event_date = datetime.strptime(event_date_str, '%Y-%m-%d').date()
                delta_days = (event_date - today).days
                if 0 <= delta_days <= days_away_threshold:
                    return False, f"Upcoming Event: {event_name} on {event_date_str}"
        return True, "No major macro events found"
    except Exception as e:
        return True, f"Macro check failed: {e}"

def scan_stock(ticker):
    """Runs the full scan for a single stock ticker."""
    ticker = ticker.strip().upper()
    try:
        stock = yf.Ticker(ticker)
        exp_dates = filter_dates(stock.options)
        if not exp_dates: return {'error': f"No suitable options found for {ticker}."}
        price_history_3y = stock.history(period='3y')
        underlying_price = price_history_3y['Close'].iloc[-1]
        
        avg_volume = price_history_3y['Volume'].rolling(30).mean().iloc[-1]
        rv30 = yang_zhang(price_history_3y, window=30)
        
        dtes, ivs = [], []
        today = datetime.today().date()
        for exp_date in exp_dates:
            chain = stock.option_chain(exp_date)
            if chain.calls.empty or chain.puts.empty: continue
            atm_strike_idx = (chain.calls['strike'] - underlying_price).abs().idxmin()
            call_iv = chain.calls.loc[atm_strike_idx, 'impliedVolatility']
            put_iv = chain.puts.loc[(chain.puts['strike'] - underlying_price).abs().idxmin(), 'impliedVolatility']
            dtes.append((datetime.strptime(exp_date, "%Y-%m-%d").date() - today).days)
            ivs.append((call_iv + put_iv) / 2.0)
        
        if not dtes: return {'error': f"Could not calculate ATM IV for {ticker}."}
        term_spline = build_term_structure(dtes, ivs)
        iv30 = float(term_spline(30))
        iv30_rv30_ratio = iv30 / rv30 if rv30 > 0 else float('inf')
        
        dte_start = dtes[0]
        ts_slope = (float(term_spline(45)) - float(term_spline(dte_start))) / (45 - dte_start) if (45 - dte_start) != 0 else 0

        macro_event_passed, macro_event_reason = check_for_macro_events()

        results = {
            'core': {
                'avg_volume': {'value': f"{avg_volume:,.0f}", 'passed': avg_volume >= config.AVG_VOLUME_THRESHOLD},
                'iv_rv_ratio': {'value': round(iv30_rv30_ratio, 2), 'passed': iv30_rv30_ratio >= config.IV_RV_RATIO_THRESHOLD},
                'term_structure_slope': {'value': round(ts_slope, 5), 'passed': ts_slope <= config.TERM_STRUCTURE_SLOPE_THRESHOLD},
            },
            'enhanced': {
                'macro_event': {'value': macro_event_reason, 'passed': macro_event_passed},
            }
        }
        
        core_passed = all(check['passed'] for check in results['core'].values())
        enhanced_passed = all(check['passed'] for check in results['enhanced'].values())
        
        recommendation = "Avoid"
        if core_passed and enhanced_passed: recommendation = "Recommended"
        elif core_passed: recommendation = "Consider (Core Passed)"

        return {
            'ticker': ticker, 'recommendation': recommendation,
            'checks': {**results['core'], **results['enhanced']},
            'details': {
                'underlying_price': round(underlying_price, 2),
                'iv30': round(iv30, 4),
                'rv30': round(rv30, 4),
            }, 'error': None
        }
    except Exception as e:
        return {'ticker': ticker, 'error': str(e)}