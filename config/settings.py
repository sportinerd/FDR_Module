# config/settings.py
import os

# API Keys
SPORTMONKS_API_KEY = os.getenv("SPORTMONKS_API_KEY", "GH7YT0CYNgDWcl99LLPkYRR6cWnVZchVhz1IL6QpTBy5ciYSyyPs5mEv8n82")

# API URLs
SPORTMONKS_BASE_URL = "https://api.sportmonks.com/v3/football"

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./fdr.db")  # SQLite for local testing

# Update frequencies
ODDS_UPDATE_FREQUENCY = 30  # seconds
PLAYER_AVAILABILITY_UPDATE_FREQUENCY = 86400  # 24 hours in seconds

# FDR color coding based on 0-10 scale
FDR_COLORS = {
    "EASIEST": "#00E563",  # 0-2 (bright green)
    "EASIER": "#80FFB7",   # 3-4 (light green)
    "AVERAGE": "#E1EBE5",  # 5-6 (neutral)
    "TOUGH": "#FFA0A0",    # 7-8 (light red)
    "TOUGHEST": "#FF6060"  # 9-10 (dark red)
}

# FDR interpretation
FDR_CATEGORIES = {
    (0, 2): "EASIEST",
    (3, 4): "EASIER", 
    (5, 6): "AVERAGE",
    (7, 8): "TOUGH",
    (9, 10): "TOUGHEST"
}
