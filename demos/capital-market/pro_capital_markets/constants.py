
from pathlib import Path


DATA_DIR = Path(__file__).parent.parent / "data"
if not DATA_DIR.exists():
    DATA_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS_DIR = DATA_DIR / "symbols"
if not SYMBOLS_DIR.exists():
    SYMBOLS_DIR.mkdir(parents=True, exist_ok=True)


MARKET_FILE = DATA_DIR / "historical_stock_daily.parquet"
BLOTTER_FILE = DATA_DIR / "blotter_daily.parquet"


# Metadata for major stocks, sectors, and industries

STOCK_STORIES = {
    # ---------- Information Technology ----------
    "AAPL": {
        "name": "Apple Inc.",
        "sector": "Information Technology",
        "industry": "Consumer Electronics",
        "events": [
            {"date": "2007-01-09", "description": "First iPhone unveiled"},
            {"date": "2011-10-05", "description": "Steve Jobs passes away"},
            {"date": "2020-08-31", "description": "4-for-1 stock split at pandemic highs"},
            {"date": "2023-06-30", "description": "Closes above $3 T market value"}
        ],
        "competitors": ["MSFT", "GOOGL", "SSNLF", "DELL", "HPQ"],
        "index": ["NASDAQ 100", "S&P 500"]
    },
    "MSFT": {
        "name": "Microsoft Corporation",
        "sector": "Information Technology",
        "industry": "Software & Cloud",
        "events": [
            {"date": "2000-04-03", "description": "Antitrust verdict sinks shares"},
            {"date": "2014-02-04", "description": "Satya Nadella named CEO; cloud pivot"},
            {"date": "2023-10-13", "description": "Closes $69 B Activision deal"}
        ],
        "competitors": ["GOOGL", "AMZN", "IBM", "ORCL", "CRM"],
        "index": ["NASDAQ 100", "S&P 500"]
    },
    "NVDA": {
        "name": "NVIDIA Corporation",
        "sector": "Information Technology",
        "industry": "Semiconductors",
        "events": [
            {"date": "1999-01-22", "description": "IPO during dot-com boom"},
            {"date": "2016-09-13", "description": "Pascal GPUs + AI breakout"},
            {"date": "2023-05-30", "description": "Briefly tops $1 T valuation"}
        ],
        "competitors": ["AMD", "INTC", "QCOM", "AVGO", "TSM"],
        "index": ["NASDAQ 100", "S&P 500"]
    },
    "AMD": {
        "name": "Advanced Micro Devices Inc.",
        "sector": "Information Technology",
        "industry": "Semiconductors",
        "events": [
            {"date": "2006-07-24", "description": "Buys ATI for $5.4 B"},
            {"date": "2014-10-08", "description": "Lisa Su becomes CEO; turnaround begins"},
            {"date": "2022-02-14", "description": "Completes $35 B Xilinx acquisition"}
        ],
        "competitors": ["NVDA", "INTC", "QCOM", "ARM", "AVGO"],
        "index": ["NASDAQ 100", "S&P 500"]
    },
    "INTC": {
        "name": "Intel Corporation",
        "sector": "Information Technology",
        "industry": "Semiconductors",
        "events": [
            {"date": "2000-03-10", "description": "Dot-com peak then 80% crash"},
            {"date": "2018-01-04", "description": "Spectre/Meltdown flaws revealed"},
            {"date": "2020-07-23", "description": "7 nm delay triggers 15% drop"}
        ],
        "competitors": ["AMD", "NVDA", "QCOM", "TSM", "IBM"],
        "index": ["NASDAQ 100", "S&P 500"]
    },
    "CSCO": {
        "name": "Cisco Systems Inc.",
        "sector": "Information Technology",
        "industry": "Networking Hardware",
        "events": [
            {"date": "2000-03-27", "description": "Loses ~90% in dot-com bust"},
            {"date": "2023-09-21", "description": "Announces $28 B Splunk acquisition"},
            {"date": "2025-06-20", "description": "Shares near 25-year high on AI demand"}
        ],
        "competitors": ["JNPR", "HPE", "ANET", "NOK", "ERIC"],
        "index": ["NASDAQ 100", "S&P 500"]
    },

    # ---------- Consumer Discretionary ----------
    "AMZN": {
        "name": "Amazon.com Inc.",
        "sector": "Consumer Discretionary",
        "industry": "E-commerce & Cloud",
        "events": [
            {"date": "1997-05-15", "description": "IPO at $18 (split-adj. $1.44)"},
            {"date": "2000-12-01", "description": "Dot-com crash; down 95 %"},
            {"date": "2005-02-02", "description": "Amazon Prime launches"},
            {"date": "2020-04-30", "description": "Pandemic demand sends shares to ATH"}
        ],
        "competitors": ["BABA", "WMT", "TGT", "SHOP", "EBAY"],
        "index": ["S&P 500", "Dow Jones Industrial Average"]
    },
    "TSLA": {
        "name": "Tesla Inc.",
        "sector": "Consumer Discretionary",
        "industry": "Automobiles",
        "events": [
            {"date": "2010-06-29", "description": "IPO raises $226 M"},
            {"date": "2013-05-08", "description": "First quarterly profit; shares triple"},
            {"date": "2018-08-07", "description": "'Funding secured' tweet → SEC case"},
            {"date": "2020-12-21", "description": "Joins S&P 500 after 5-for-1 split"}
        ],
        "competitors": ["GM", "F", "RIVN", "NIO", "BYDDY"],
        "index": ["S&P 500", "Dow Jones Industrial Average"]
    },
    "GME": {
        "name": "GameStop Corp.",
        "sector": "Consumer Discretionary",
        "industry": "Specialty Retail",
        "events": [
            {"date": "2021-01-28", "description": "Reddit short squeeze peaks at $483"},
            {"date": "2024-05-14", "description": "'Roaring Kitty' livestream sparks rally 2.0"}
        ],
        "competitors": ["BBY", "WMT", "AMZN", "COST", "EBAY"],
        "index": ["S&P 500", "Dow Jones Industrial Average"]
    },
    "GM": {
        "name": "General Motors Company",
        "sector": "Consumer Discretionary",
        "industry": "Automobiles",
        "events": [
            {"date": "2009-06-01", "description": "Files Chapter 11 bankruptcy"},
            {"date": "2010-11-18", "description": "Returns with record $20 B IPO"},
            {"date": "2021-01-28", "description": "Pledges all-EV line-up by 2035"},
            {"date": "2023-10-24", "description": "UAW strike dents guidance"}
        ],
        "competitors": ["F", "TSLA", "STLA", "TM", "RIVN"],
        "index": ["S&P 500", "Dow Jones Industrial Average"]
    },
    "F": {
        "name": "Ford Motor Company",
        "sector": "Consumer Discretionary",
        "industry": "Automobiles",
        "events": [
            {"date": "2006-11-20", "description": "Takes $23 B loan—avoids bailout"},
            {"date": "2020-03-18", "description": "Shuts plants for Covid-19"},
            {"date": "2021-05-19", "description": "Reveals all-electric F-150 Lightning"}
        ],
        "competitors": ["GM", "TSLA", "RIVN", "STLA", "TM"],
        "index": ["S&P 500", "Dow Jones Industrial Average"]
    },
    "NKE": {
        "name": "Nike Inc.",
        "sector": "Consumer Discretionary",
        "industry": "Athletic Apparel",
        "events": [
            {"date": "2012-01-19", "description": "Launches Nike+ FuelBand (digital pivot)"},
            {"date": "2018-09-03", "description": "Kaepernick 'Dream Crazy' ad sparks volatility"},
            {"date": "2020-06-26", "description": "E-commerce boom lifts pandemic quarter"}
        ],
        "competitors": ["ADDYY", "PUMSY", "LULU", "UA", "SKE"],
        "index": ["S&P 500", "Dow Jones Industrial Average"]
    },

    # ---------- Communication Services ----------
    "GOOGL": {
        "name": "Alphabet Inc.",
        "sector": "Communication Services",
        "industry": "Internet & Search",
        "events": [
            {"date": "2004-08-19", "description": "Google IPO at $85"},
            {"date": "2015-10-02", "description": "Alphabet restructuring complete"},
            {"date": "2023-02-06", "description": "Bard AI demo stumble—$100 B hit"}
        ],
        "competitors": ["MSFT", "META", "AMZN", "BIDU", "SNAP"],
        "index": ["S&P 500", "NASDAQ 100"]
    },
    "META": {
        "name": "Meta Platforms Inc.",
        "sector": "Communication Services",
        "industry": "Social Media",
        "events": [
            {"date": "2012-05-18", "description": "Largest tech IPO of its time"},
            {"date": "2018-03-17", "description": "Cambridge Analytica scandal"},
            {"date": "2023-02-02", "description": "'Year of Efficiency' adds $196 B in a day"}
        ],
        "competitors": ["GOOGL", "SNAP", "PINS", "TCEHY", "MSFT"],
        "index": ["S&P 500", "NASDAQ 100"]
    },
    "NFLX": {
        "name": "Netflix Inc.",
        "sector": "Communication Services",
        "industry": "Streaming Media",
        "events": [
            {"date": "2002-05-23", "description": "IPO (DVD-by-mail)"},
            {"date": "2007-01-16", "description": "Launches streaming"},
            {"date": "2011-09-18", "description": "Qwikster fiasco; shares -75 %"},
            {"date": "2022-04-19", "description": "First subscriber loss → 35 % plunge"}
        ],
        "competitors": ["DIS", "AMZN", "WBD", "PARA", "AAPL"],
        "index": ["S&P 500", "NASDAQ 100"]
    },
    "DIS": {
        "name": "The Walt Disney Company",
        "sector": "Communication Services",
        "industry": "Media & Entertainment",
        "events": [
            {"date": "2006-01-24", "description": "Buys Pixar for $7.4 B"},
            {"date": "2019-03-20", "description": "Closes $71 B 21st Century Fox deal"},
            {"date": "2019-11-12", "description": "Launches Disney+ streaming"},
            {"date": "2020-03-15", "description": "Closes theme parks worldwide for Covid-19"}
        ],
        "competitors": ["NFLX", "WBD", "PARA", "CMCSA", "AMZN"],
        "index": ["S&P 500", "NASDAQ 100"]
    },
    "SNAP": {
        "name": "Snap Inc.",
        "sector": "Communication Services",
        "industry": "Social Media",
        "events": [
            {"date": "2017-03-02", "description": "IPO; shares +44 % first day"},
            {"date": "2021-10-21", "description": "Apple iOS privacy change cuts ads; -25 %"},
            {"date": "2022-05-23", "description": "Guidance shock; shares -40 %"}
        ],
        "competitors": ["META", "PINS", "GOOGL", "TCEHY", "BIDU"],
        "index": ["S&P 500", "NASDAQ 100"]
    },

    # ---------- Industrials ----------
    "BA": {
        "name": "The Boeing Company",
        "sector": "Industrials",
        "industry": "Aerospace & Defense",
        "events": [
            {"date": "1997-08-01", "description": "Merges with McDonnell Douglas"},
            {"date": "2019-03-13", "description": "Global 737 MAX grounding"},
            {"date": "2024-01-08", "description": "MAX-9 door-plug incident; shares slide"}
        ],
        "competitors": ["EADSY", "LMT", "NOC", "GE", "RTX"],
        "index": ["S&P 500", "Dow Jones Industrial Average"]
    },
    "GE": {
        "name": "General Electric Company",
        "sector": "Industrials",
        "industry": "Conglomerate",
        "events": [
            {"date": "2008-10-10", "description": "Buffett injects $3 B amid crisis"},
            {"date": "2018-10-30", "description": "Cuts dividend to 1 ¢"},
            {"date": "2021-11-09", "description": "Announces three-way break-up"},
            {"date": "2024-04-02", "description": "Split completes; GE Vernova/Aerospace debut"}
        ],
        "competitors": ["BA", "HON", "RTX", "SIEGY", "MMM"],
        "index": ["S&P 500", "Dow Jones Industrial Average"]
    },

    # ---------- Energy ----------
    "BP": {
        "name": "BP p.l.c.",
        "sector": "Energy",
        "industry": "Integrated Oil & Gas",
        "events": [
            {"date": "2010-04-20", "description": "Deepwater Horizon spill"},
            {"date": "2020-02-04", "description": "Books first loss in a decade"},
            {"date": "2022-02-27", "description": "Exits Russia; $25 B write-down"}
        ],
        "competitors": ["XOM", "CVX", "SHEL", "TOT", "COP"],
        "index": ["S&P 500", "Russell 1000"]
    },
    "XOM": {
        "name": "Exxon Mobil Corporation",
        "sector": "Energy",
        "industry": "Integrated Oil & Gas",
        "events": [
            {"date": "1999-11-30", "description": "Completes Exxon-Mobil mega-merger"},
            {"date": "2020-02-02", "description": "Posts first annual loss as public company"},
            {"date": "2022-02-01", "description": "Reports record $55 B profit"}
        ],
        "competitors": ["CVX", "BP", "COP", "SHEL", "TOT"],
        "index": ["S&P 500", "Russell 1000"]
    },
    "CVX": {
        "name": "Chevron Corporation",
        "sector": "Energy",
        "industry": "Integrated Oil & Gas",
        "events": [
            {"date": "2001-10-09", "description": "Merger with Texaco closes"},
            {"date": "2022-01-27", "description": "Record profit; $75 B buyback"},
            {"date": "2023-10-23", "description": "Agrees to buy Hess for $53 B"}
        ],
        "competitors": ["XOM", "BP", "COP", "OXY", "SHEL"],
        "index": ["S&P 500", "Russell 1000"]
    }
}


CLIENTS = [
    "BlackRock", "Vanguard", "State Street",
    "Fidelity", "Goldman Sachs", "Morgan Stanley",
    "Citadel Securities", "Bridgewater", "Berkshire Hathaway",
]



def get_sector_symbols():
    sector_symbols = {}
    for symbol, info in STOCK_STORIES.items():
        sector = info.get("sector")
        sector_symbols.setdefault(sector, set()).add(symbol)
    return {sector: list(symbols) for sector, symbols in sector_symbols.items()}


SYMBOLS_BY_SECTOR   = get_sector_symbols()
UNIQUE_SYMBOLS = sorted(list(set(symbol for sector in SYMBOLS_BY_SECTOR.values() for symbol in sector)))


EVENTS = [
    {"date": event["date"], "symbol": symbol, "description": event["description"]}
    for symbol, info in STOCK_STORIES.items()
    for event in info.get("events", [])
]



# ------------------------------------------------------------

# Traders: mix of human dealers and algorithmic books
TRADERS = [
    # Human traders
    "Sarah Kim",        "Michael Johnson",   "Priya Shah",       "Carlos Garcia",
    "Wei Zhang",        "Alex Ivanov",       "Emma Davies",      "Isabella Rossi",
    "Liam Wilson",      "Sofia Hernandez",   "Jack Nguyen",      "Anna Petrova",
    "Lucas Silva",      "Maya Patel",
    # Algorithmic traders / smart-order routers
    "ALGO_VWAP",        "ALGO_IS",           "ALGO_Momentum",    "ALGO_LiquiditySweep",
    "ALGO_SmartRouter"
]

# Desks / books (keep it small for demo filters)
DESKS = [
    "US Cash Equities",
    "EU Cash Equities",
    "Asia Cash Equities",
    "Program Trading",
    "Options & Volatility",
    "Delta One",
    "ETF Market Making",
    "Algorithmic Execution"
]

# Execution venues (MIC codes & common names)
EXEC_VENUES = [
    "XNAS",  # Nasdaq
    "XNYS",  # NYSE
    "ARCX",  # NYSE Arca
    "ISE",   # International Securities Exchange
    "XCHI",  # Chicago Stock Exchange
    "XBOS",  # Boston Stock Exchange
    "XLON",  # London Stock Exchange
    "XETR"   # Deutsche Börse Xetra
]

# Funds under management
FUNDS = [
    "Prospective Growth Fund",
    "Prospective Global Alpha Fund",
    "Prospective ESG Equity Fund",
    "Prospective Systematic Strategies",
    "Prospective Market Neutral Fund",
    "Prospective Technology Select Fund"
]

# Benchmarks / indices
BENCHMARK_INDICES = [
    "S&P 500",
    "Russell 1000",
    "NASDAQ 100",
    "Dow Jones",
    "MSCI EM",
    "FTSE 100",
]

ORDER_TYPES = [
    "LIMIT",        # Limit order: price cap/floor
    "MARKET",       # Market order: execute at best available
    "ICEBERG",      # Partial display, hidden size
    "FOK",          # Fill or Kill
    "IOC",          # Immediate or Cancel
]

ORDER_STATUSES = [
    "NEW",          # Order accepted, not yet filled
    "PARTIALLY_FILLED", # Some shares filled, order remains open
    "FILLED",       # Order fully filled
    "CANCELLED",    # Cancelled by trader or system
]

