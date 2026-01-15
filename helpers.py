

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

#this maps qfs country codes + exchange to EODHD symbols
EXCHANGE_MAPPING = {
    "FR$Paris" : ["PA"],
    "LN$London" : ["LSE", "IL"],
    "DE$Hamburg" : ["HM"],
    "DE$XETRA" : ["XETRA"],
    "DE$Stuttgart" : ["STU"],
    "DE$Frankfurt" : ["F"],
    "US$BATS" : ["US"],
    "US$OTC" : ["US"],
    "US$NYSEArca" : ["US"],
    "US$NYSEAMERICAN" : ["US"],
    "US$NYSE" : ["US"],
    "US$NASDAQ" : ["US"],
    "AT$Vienna" : ["VI"],
    "GR$Athens" : ["AT"], #Greece, athens exchange
    "AU$ASX" : ["AU"],
    "CA$TSX Venture" : ["V"],
    "CA$TSXVenture" : ["V"],
    "CA$Toronto" : ["TO"],
    "NL$Euronext Amsterdam" : ["AS"], #Amsterdam exchange
    "SE$Stockholm" : ["ST"], #Stockholm
    "BE$Euronext Brussels" : ["BR"], #euro next brussels
    "ES$Madrid" : ["MC"], #madrid exchange
    "PL$Warsaw" : ["WAR"], #poland warsam
    "NO$Oslo Bors" : ["OL"], #oslo
    "PT$Euronext Lisbon" : ["LS"], #lisbon exchange
    "FI$Helsinki" : ["HE"],
    "CH$SIX Swiss Exchange" : ["SW"], #six swiss exchange
    "DK$CSE" : ["CO"] #denmark
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