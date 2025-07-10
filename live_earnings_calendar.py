from datetime import datetime, timedelta
import config
from polygon import RESTClient

def get_upcoming_earnings(days_ahead=7):
    """
    Fetches a list of tickers and their announcement times for upcoming earnings.

    Args:
        days_ahead (int): The number of days to look ahead for earnings.

    Returns:
        list: A list of dictionaries, each containing the ticker, report date, and time.
    """
    api_key = config.POLYGON_API_KEY
    if not api_key or api_key == "YOUR_POLYGON_API_KEY_HERE":
        print("ERROR: Polygon API key not set in config.py.")
        return []

    client = RESTClient(api_key)
    upcoming_events = []
    
    try:
        # Define the date range for the calendar lookup
        today = datetime.now().date()
        end_date = today + timedelta(days=days_ahead)
        
        # Fetch the calendar data from Polygon
        resp = client.get_earnings_calendar(from_=today, to=end_date)
        
        for event in resp:
            # The time (bmo, amc, etc.) is included in the response
            upcoming_events.append({
                "ticker": event.ticker,
                "report_date": datetime.strptime(event.report_date, "%Y-%m-%d").date(),
                "timing": event.time
            })
            
    except Exception as e:
        print(f"ERROR: Could not fetch upcoming earnings from Polygon.io: {e}")

    print(f"Found {len(upcoming_events)} upcoming earnings events from Polygon.io.")
    return upcoming_events

# --- Example of how to use this module ---
if __name__ == "__main__":
    events = get_upcoming_earnings()
    if events:
        print("\nSample of upcoming events:")
        for event in events[:15]:
            print(f"  Ticker: {event['ticker']}, Date: {event['report_date']}, Time: {event['timing']}")