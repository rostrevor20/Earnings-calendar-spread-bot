import yfinance as yf
import pandas as pd
from datetime import datetime

def get_sp500_tickers():
    """Gets the list of S&P 500 tickers from Wikipedia by finding the correct table."""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        all_tables = pd.read_html(url)
        for table in all_tables:
            if 'Symbol' in table.columns:
                return table['Symbol'].tolist()
        raise ValueError("Could not find the components table with a 'Symbol' column.")
    except Exception as e:
        print(f"Could not fetch S&P 500 tickers: {e}")
        return []

def get_sp400_tickers():
    """Gets the list of S&P 400 tickers from Wikipedia by finding the correct table."""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_400_companies'
        all_tables = pd.read_html(url)
        for table in all_tables:
            if 'Symbol' in table.columns:
                return table['Symbol'].tolist()
        raise ValueError("Could not find the components table with a 'Symbol' column.")
    except Exception as e:
        print(f"Could not fetch S&P 400 tickers: {e}")
        return []

def get_sp600_tickers():
    """Gets the list of S&P 600 tickers from Wikipedia by finding the correct table."""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies'
        all_tables = pd.read_html(url)
        for table in all_tables:
            if 'Symbol' in table.columns:
                return table['Symbol'].tolist()
        raise ValueError("Could not find the components table with a 'Symbol' column.")
    except Exception as e:
        print(f"Could not fetch S&P 600 tickers: {e}")
        return []

def get_combined_universe_tickers():
    """Fetches S&P 500, 400, and 600 tickers and combines them."""
    print("Fetching tickers from S&P 500, 400, and 600...")
    sp500 = get_sp500_tickers()
    sp400 = get_sp400_tickers()
    sp600 = get_sp600_tickers()
    
    # Combine the lists and remove duplicates
    combined_list = sorted(list(set(sp500 + sp400 + sp600)))
    print(f"Created a combined universe of {len(combined_list)} unique tickers.")
    return combined_list

def get_historical_earnings_calendar(tickers, start_date, end_date):
    """
    Fetches historical earnings announcement dates for a list of tickers.
    """
    print(f"Fetching historical earnings dates for {len(tickers)} tickers...")
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    all_earnings_events = []
    
    for i, ticker_symbol in enumerate(tickers):
        ticker_symbol = ticker_symbol.strip().replace('.', '-')
        try:
            stock = yf.Ticker(ticker_symbol)
            earnings_dates = stock.earnings_dates
            
            if earnings_dates is None or earnings_dates.empty:
                continue

            for date in earnings_dates.index:
                event_date = date.date()
                if start_date_dt <= event_date <= end_date_dt:
                    all_earnings_events.append((event_date, ticker_symbol))
            
            if (i + 1) % 100 == 0:
                print(f"Processed {i+1}/{len(tickers)}: {ticker_symbol}")
        except Exception as e:
            pass
            
    all_earnings_events.sort()
    print(f"\nFound {len(all_earnings_events)} earnings events between {start_date} and {end_date}.")
    return all_earnings_events

if __name__ == "__main__":
    
    # 1. Get the combined universe of tickers from the S&P index family
    combined_tickers = get_combined_universe_tickers()
    
    # 2. Define the backtest period
    backtest_start = "2023-01-01"
    backtest_end = "2023-12-31"
    
    # 3. Get the historical events
    historical_events = get_historical_earnings_calendar(combined_tickers, backtest_start, backtest_end)
    
    # 4. Display a sample of the results
    if historical_events:
        print("\nSample of scheduled events:")
        for event in historical_events[:15]:
            print(f"  Date: {event[0]}, Ticker: {event[1]}")