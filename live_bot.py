import threading
import time
from datetime import datetime, timedelta
import pytz
import yfinance as yf
import pandas as pd
import config 

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.ticktype import TickTypeEnum

from scanner import scan_stock
from live_earnings_calendar import get_upcoming_earnings

# Global trade schedule
trade_schedule = []

class IBKRBot(EWrapper, EClient):
    # ... (The IBKRBot class remains exactly the same as before) ...
    """ The main class for handling the connection and trading logic with Interactive Brokers. """
    def __init__(self):
        EClient.__init__(self, self)
        self.next_order_id = None
        self.account_value = 0
        self.market_data = {}
        self.market_data_events = {}
        self.next_order_id_event = threading.Event()
        self.account_value_event = threading.Event()

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.next_order_id = orderId
        self.next_order_id_event.set()
        print(f"Received next valid order ID: {orderId}")

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        if tag == "NetLiquidation" and currency == "USD":
            self.account_value = float(value)
            print(f"Account Net Liquidation Value: ${self.account_value:,.2f}")
            self.account_value_event.set()

    def orderStatus(self, orderId: int, status: str, filled: float, remaining: float,
                    avgFillPrice: float, permId: int, parentId: int, lastFillPrice: float,
                    clientId: int, whyHeld: str, mktCapPrice: float):
        super().orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId,
                          parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        print(f"Order Status - ID: {orderId}, Status: {status}, Filled: {filled}, Avg Price: {avgFillPrice}")
        for trade in trade_schedule:
            if orderId == trade.get('entry_order_id') and status == 'Filled' and trade['status'] != 'open':
                print(f"Entry order for {trade['ticker']} filled. Placing stop-loss.")
                trade['status'] = 'open'
                trade['position'] = filled
                self.place_stop_loss_order(trade, avgFillPrice)
                break
            elif orderId == trade.get('stop_loss_order_id') and status == 'Filled':
                print(f"Stop-loss for {trade['ticker']} filled. Position is closed.")
                trade['status'] = 'closed_by_stop'
                break

    def error(self, reqId, errorCode, errorString):
        if errorCode < 2100 or errorCode > 2170:
             print(f"Error - Code: {errorCode}, Message: {errorString}")
            
    def tickPrice(self, reqId, tickType, price, attrib):
        super().tickPrice(self, reqId, tickType, price, attrib)
        if tickType in [1, 2]:
            if reqId not in self.market_data: self.market_data[reqId] = {}
            if tickType == 1: self.market_data[reqId]['bid'] = price
            elif tickType == 2: self.market_data[reqId]['ask'] = price
            if 'bid' in self.market_data[reqId] and 'ask' in self.market_data[reqId]:
                if reqId in self.market_data_events: self.market_data_events[reqId].set()

    def get_next_order_id(self):
        if self.next_order_id is not None:
            current_id = self.next_order_id
            self.next_order_id += 1
            return current_id
        else:
            raise ConnectionError("Could not get a valid order ID from IBKR.")

    def create_option_contract(self, symbol, expiry, strike, right):
        contract = Contract(); contract.symbol = symbol; contract.secType = "OPT";
        contract.exchange = "SMART"; contract.currency = "USD";
        contract.lastTradeDateOrContractMonth = expiry; contract.strike = strike;
        contract.right = right; contract.multiplier = "100"
        return contract

    def request_spread_price(self, short_leg_contract, long_leg_contract):
        short_req_id = self.get_next_order_id(); long_req_id = self.get_next_order_id()
        self.market_data_events[short_req_id] = threading.Event()
        self.market_data_events[long_req_id] = threading.Event()
        self.reqMktData(short_req_id, short_leg_contract, "", False, False, [])
        self.reqMktData(long_req_id, long_leg_contract, "", False, False, [])
        short_received = self.market_data_events[short_req_id].wait(timeout=10)
        long_received = self.market_data_events[long_req_id].wait(timeout=10)
        self.cancelMktData(short_req_id); self.cancelMktData(long_req_id)
        natural_price = None
        if short_received and long_received:
            short_mid = (self.market_data[short_req_id]['bid'] + self.market_data[short_req_id]['ask']) / 2
            long_mid = (self.market_data[long_req_id]['bid'] + self.market_data[long_req_id]['ask']) / 2
            natural_price = round(long_mid - short_mid, 2)
            print(f"Calculated natural price for spread: ${natural_price:.2f}")
        else: print("Failed to receive market data for one or both legs within timeout.")
        del self.market_data_events[short_req_id]; del self.market_data_events[long_req_id]
        if short_req_id in self.market_data: del self.market_data[short_req_id]
        if long_req_id in self.market_data: del self.market_data[long_req_id]
        return natural_price

    def place_order(self, contract, direction, quantity, order_type="MKT", limit_price=0, transmit=True):
        order = Order(); order.action = direction; order.orderType = order_type
        order.totalQuantity = quantity; order.transmit = transmit
        if order_type == "LMT": order.lmtPrice = limit_price
        order_id = self.get_next_order_id()
        self.placeOrder(order_id, contract, order)
        print(f"Placed {direction} {order_type} order for {quantity} {contract.symbol} contracts. Order ID: {order_id}")
        return order_id

    def place_stop_loss_order(self, trade, fill_price):
        stop_price = round(fill_price * (1 + config.STOP_LOSS_PERCENTAGE), 2)
        order = Order(); order.action = "SELL"; order.orderType = "STP"
        order.totalQuantity = trade['position']; order.auxPrice = stop_price
        order.transmit = True
        stop_loss_id = self.get_next_order_id()
        trade['stop_loss_order_id'] = stop_loss_id
        self.placeOrder(stop_loss_id, trade['contract'], order)
        print(f"Placed stop-loss order for {trade['ticker']} at STP ${stop_price:.2f}. Order ID: {stop_loss_id}")

def create_bag_contract(ticker):
    contract = Contract(); contract.symbol = ticker; contract.secType = "BAG"
    contract.currency = "USD"; contract.exchange = "SMART"
    leg1 = {"conId": 0, "ratio": 1, "action": "SELL", "exchange": "SMART"}
    leg2 = {"conId": 0, "ratio": 1, "action": "BUY", "exchange": "SMART"}
    contract.comboLegs = [leg1, leg2]
    return contract

def populate_trade_schedule(bot):
    print("Populating trade schedule for the week...")
    events = get_upcoming_earnings()
    if not events:
        print("No upcoming earnings found from Polygon.io.")
        return

    for event in events:
        ticker = event['ticker']
        print(f"\n--- Scanning {ticker} for schedule ---")
        scan_result = scan_stock(ticker)
        if scan_result.get('error') or scan_result['recommendation'] != 'Recommended':
            print(f"Skipping {ticker}: {scan_result.get('error', 'Not Recommended')}")
            continue
        try:
            # The earnings date and time now come directly from our calendar function
            earnings_day = event['report_date']
            # Timing: 'amc' (After Market Close), 'bmo' (Before Market Open), 'dmh' (During Market Hours)
            is_amc = event['timing'] == 'amc'

            price_reaction_day = earnings_day + timedelta(days=1) if is_amc else earnings_day
            entry_day = earnings_day - timedelta(days=1)
            exit_day = price_reaction_day
            
            if entry_day.weekday() >= 5: entry_day -= timedelta(days=entry_day.weekday() - 4)
            if exit_day.weekday() >= 5: exit_day += timedelta(days=7 - exit_day.weekday())

            entry_time = config.MARKET_TIMEZONE.localize(datetime.combine(entry_day, datetime.min.time()) + timedelta(hours=15, minutes=45))
            exit_time = config.MARKET_TIMEZONE.localize(datetime.combine(exit_day, datetime.min.time()) + timedelta(hours=9, minutes=45))
            
            if entry_time < datetime.now(config.MARKET_TIMEZONE):
                print(f"Skipping {ticker}: Entry time {entry_time.strftime('%Y-%m-%d %H:%M')} is in the past.")
                continue
                
            trade = {
                'ticker': ticker, 'status': 'pending_entry', 'entry_time': entry_time,
                'exit_time': exit_time, 'position': 0, 'entry_order_id': None,
                'stop_loss_order_id': None, 'contract': create_bag_contract(ticker),
                'underlying_price': scan_result['details']['underlying_price']
            }
            trade_schedule.append(trade)
            print(f"Scheduled trade for {ticker}: Entry at {entry_time.strftime('%Y-%m-%d %H:%M')}, Exit at {exit_time.strftime('%Y-%m-%d %H:%M')}")
        except Exception as e:
            print(f"Could not schedule trade for {ticker}: {e}")

def main():
    # ... (The main function remains exactly the same as before) ...
    print("--- Starting Earnings Calendar Spread Bot ---")
    bot = IBKRBot()
    try:
        print(f"Connecting to IBKR on {config.IBKR_HOST}:{config.IBKR_PORT}...")
        bot.connect(config.IBKR_HOST, config.IBKR_PORT, clientId=config.IBKR_CLIENT_ID)
        api_thread = threading.Thread(target=bot.run, daemon=True)
        api_thread.start()
        if not bot.next_order_id_event.wait(timeout=10): raise ConnectionError("Failed to get next order ID from IBKR.")
        bot.reqAccountSummary(9001, "All", "NetLiquidation")
        if not bot.account_value_event.wait(timeout=10): raise ConnectionError("Failed to get account value from IBKR.")
        populate_trade_schedule(bot)
        print("\n--- Starting Persistent Trading Loop (Checks every 30s) ---")
        while True:
            current_time = datetime.now(config.MARKET_TIMEZONE)
            for trade in trade_schedule:
                if trade['status'] == 'pending_entry' and current_time >= trade['entry_time']:
                    print(f"\n>>> Time to enter trade for {trade['ticker']} <<<")
                    trade['status'] = 'processing_entry'
                    atm_strike = round(trade['underlying_price'])
                    short_expiry = (datetime.now() + timedelta(days=20)).strftime('%Y%m%d')
                    long_expiry = (datetime.now() + timedelta(days=20 + config.EXPIRY_GAP_DAYS)).strftime('%Y%m%d')
                    short_leg = bot.create_option_contract(trade['ticker'], short_expiry, atm_strike, config.OPTION_TYPE)
                    long_leg = bot.create_option_contract(trade['ticker'], long_expiry, atm_strike, config.OPTION_TYPE)
                    natural_price = bot.request_spread_price(short_leg, long_leg)
                    if natural_price and natural_price > 0:
                        risk_amount = bot.account_value * config.RISK_ALLOCATION_PERCENT
                        cost_per_spread = natural_price * 100
                        num_contracts = int(risk_amount // cost_per_spread) if cost_per_spread > 0 else 0
                        if num_contracts > 0:
                            print(f"  - Sizing: Allocating ${risk_amount:,.2f} -> Trading {num_contracts} contracts.")
                            entry_order_id = bot.place_order(trade['contract'], "BUY", num_contracts, config.ORDER_TYPE, natural_price)
                            trade['entry_order_id'] = entry_order_id
                        else:
                            print(f"Not enough capital for {trade['ticker']}. Skipping entry."); trade['status'] = 'skipped'
                    else:
                        print(f"Could not get price for {trade['ticker']}. Skipping entry."); trade['status'] = 'skipped'
                if trade['status'] == 'open' and current_time >= trade['exit_time']:
                    print(f"\n>>> Time to exit trade for {trade['ticker']} <<<")
                    trade['status'] = 'processing_exit'
                    print(f"Cancelling existing stop-loss order {trade['stop_loss_order_id']} for {trade['ticker']}.")
                    bot.cancelOrder(trade['stop_loss_order_id'])
                    bot.place_order(trade['contract'], "SELL", trade['position'], "MKT")
                    trade['status'] = 'closed_by_time'
            time.sleep(30)
    except KeyboardInterrupt: print("Bot shutdown requested by user.")
    except Exception as e:
        print(f"An critical error occurred: {e}")
    finally:
        print("Disconnecting from IBKR...")
        bot.disconnect()
        print("Bot has shut down.")

if __name__ == "__main__":
    main()