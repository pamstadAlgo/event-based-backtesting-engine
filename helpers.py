

SUFFIX_TO_EXCHANGES = {
    "PA": ["Paris"],
    "L": ["London"],
    "HM": ["Hamburg"],
    "BATS": ["BATS"],
    "OTC": ["OTC"],
    "ARCA": ["NYSEArca"],
    "VI": ["Vienna"],
    "AT": ["Athens"],
    "DE": ["XETRA"],
    "AX": ["ASX"],
    "V": ["TSX Venture", "TSXVenture", "Toronto"],
    "AS": ["Euronext Amsterdam"],
    "SG": ["Stuttgart"],
    "ST": ["Stockholm"],
    "MI": ["Borsa Italiana"],
    "BR": ["Euronext Brussels"],
    "MC": ["Madrid"],
    "AM": ["NYSEAMERICAN"],
    "WA": ["Warsaw"],
    "OL": ["Oslo Bors"],
    "LS": ["Euronext Lisbon"],
    "TO": ["Toronto"],
    "HE": ["Helsinki"],
    "CN": ["CSE"],
    "F": ["Frankfurt"],
    "IT": ["Borsa Italiana"],
    "SW": ["SIX Swiss Exchange"],
    "" : ["NYSE", "NASDAQ", "OTC", "NYSEAMERICAN"]
}


def parse_ticker(ticker: str):
    """
    Parse ticker and optional exchange suffix.
    SPN.V   -> ('SPN', 'V')
    AAPL    -> ('AAPL', None)
    BMW.DE  -> ('BMW', 'DE')
    """
    ticker = ticker.strip().upper()
    if "." in ticker:
        symbol, suffix = ticker.split(".", 1)
        return symbol, suffix.upper()
    return ticker, None