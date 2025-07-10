import pytz

# === API KEYS & CONNECTION SETTINGS ===
# --- IBKR Connection ---
IBKR_HOST = '127.0.0.1'
IBKR_PORT = 7497
IBKR_CLIENT_ID = 1

# --- API Keys ---
# IMPORTANT: Replace with your actual key.
ALPHA_VANTAGE_API_KEY = 'Your_Alpha_Vantage_API_Key_Here'
POLYGON_API_KEY = 'Your_Polygon_API_Key_Here'


# === TRADING STRATEGY PARAMETERS ===
# --- Strategy & Trade Definition ---
OPTION_TYPE = 'CALL'
EXPIRY_GAP_DAYS = 30
MARKET_TIMEZONE = pytz.timezone("US/Eastern")

# --- Risk Management & Position Sizing ---
RISK_ALLOCATION_PERCENT = 0.15
STOP_LOSS_PERCENTAGE = 0.40

# --- Order Execution ---
ORDER_TYPE = 'LMT'


# === SCANNER PARAMETER THRESHOLDS ===
# --- Core Scanner Parameters ---
AVG_VOLUME_THRESHOLD = 1500000
IV_RV_RATIO_THRESHOLD = 1.25
TERM_STRUCTURE_SLOPE_THRESHOLD = -0.00406

# --- Enhanced Scanner Parameters ---
MACRO_EVENT_DAYS_AWAY = 1