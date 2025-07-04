import requests
import csv
import io
from datetime import datetime, timedelta
import config  # Import the new config file

def get_upcoming_earnings(days_ahead=7):
    """
    Fetches a list of tickers with earnings announcements in the next X days.

    Args:
        days_ahead (int): The number of days to look ahead for earnings.

    Returns:
        list: A list of unique stock tickers, or an empty list if an error occurs.
    """
    api_key = config.ALPHA_VANTAGE_API_KEY
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        print("ERROR: API key for Alpha Vantage is not set in config.py.")
        return []

    # Alpha Vantage provides earnings for the next 3 months with this call
    url = f'https://www.alphavantage.co/query?function=EARNINGS_CALENDAR&horizon=3month&apikey={api_key}'

    upcoming_earnings_tickers = []
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for HTTP errors

        # The data is returned as a CSV string
        csv_file = io.StringIO(response.text)
        reader = csv.DictReader(csv_file)

        today = datetime.now().date()
        end_date = today + timedelta(days=days_ahead)

        for row in reader:
            report_date_str = row.get('reportDate')
            if not report_date_str:
                continue

            report_date = datetime.strptime(report_date_str, '%Y-%m-%d').date()

            # Check if the earnings report date is within our desired window
            if today <= report_date <= end_date:
                ticker = row.get('symbol')
                if ticker:
                    upcoming_earnings_tickers.append(ticker)

        # Return a list of unique tickers
        return sorted(list(set(upcoming_earnings_tickers)))

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Could not connect to Alpha Vantage. {e}")
        return []
    except Exception as e:
        # This can happen if the API returns an error message instead of CSV data
        # (e.g., "Thank you for using Alpha Vantage! Our standard API call frequency is 25 requests per day...")
        print(f"ERROR: Could not parse earnings data. The API may have a call limit. Details: {e}")
        return []


# --- Example of how to use this module ---
if __name__ == "__main__":
    print("Fetching earnings for the next 7 days...")
    
    # The function no longer needs the API key passed to it
    tickers = get_upcoming_earnings(days_ahead=7)
    
    if tickers:
        print(f"Found {len(tickers)} companies reporting earnings soon:")
        # Print in columns for readability
        col_width = 10
        cols = 5
        for i in range(0, len(tickers), cols):
            line = "".join(f"{ticker:<{col_width}}" for ticker in tickers[i:i+cols])
            print(line)
    else:
        print("No upcoming earnings found or an error occurred.")