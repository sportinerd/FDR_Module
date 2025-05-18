# config/leagues_classifier.py
import logging
from typing import Dict, Set, List

logger = logging.getLogger(__name__)

# Top-tier leagues by country - these will be considered major leagues
MAJOR_LEAGUES_BY_COUNTRY = {
    "England": ["Premier League"],
    "Spain": ["La Liga", "LaLiga", "Primera Division"],
    "Germany": ["Bundesliga"],
    "Italy": ["Serie A"],
    "France": ["Ligue 1"],
    "Netherlands": ["Eredivisie"],
    "Portugal": ["Primeira Liga"],
    "Belgium": ["Pro League", "Jupiler Pro League"],
    "Turkey": ["Super Lig"],
    "Scotland": ["Premiership"],
    # Add more as needed
}

# International major competitions
MAJOR_INTERNATIONAL_COMPETITIONS = [
    "UEFA Champions League",
    "UEFA Europa League", 
    "UEFA Conference League",
    "FIFA World Cup",
    "UEFA European Championship",
    "Copa America",
    "Africa Cup of Nations"
]

# Popularity metrics (this would ideally be pulled from a database)
LEAGUE_POPULARITY_METRICS = {
    # Example structure: "league name": {views, betting_volume, social_media_mentions}
    "Premier League": {"views": 1000000, "betting_volume": 5000000, "social_media": 2000000},
    "La Liga": {"views": 800000, "betting_volume": 3000000, "social_media": 1500000},
    # Add more as needed
}

# Popularity threshold for automatically considering a league as major
POPULARITY_THRESHOLD = {
    "views": 500000,
    "betting_volume": 1000000,
    "social_media": 500000
}

def classify_league_importance(league_name: str, country_name: str, is_cup: bool = False) -> bool:
    """
    Dynamically determine if a league is major based on several factors:
    1. Known top leagues (explicit list)
    2. Popularity metrics (if available)
    3. Heuristics (country importance, competition type)
    """
    # Check explicit lists first
    if country_name in MAJOR_LEAGUES_BY_COUNTRY:
        if league_name in MAJOR_LEAGUES_BY_COUNTRY[country_name]:
            logger.info(f"Classified {league_name} as major league (top-tier in {country_name})")
            return True
    
    # Check international competitions
    if league_name in MAJOR_INTERNATIONAL_COMPETITIONS:
        logger.info(f"Classified {league_name} as major league (international competition)")
        return True
    
    # Check popularity metrics if available
    if league_name in LEAGUE_POPULARITY_METRICS:
        metrics = LEAGUE_POPULARITY_METRICS[league_name]
        if (metrics.get("views", 0) > POPULARITY_THRESHOLD["views"] or
            metrics.get("betting_volume", 0) > POPULARITY_THRESHOLD["betting_volume"] or
            metrics.get("social_media", 0) > POPULARITY_THRESHOLD["social_media"]):
            logger.info(f"Classified {league_name} as major league (popularity metrics)")
            return True
    
    # Heuristic checks
    # 1. Is it a top-tier league in a major footballing nation?
    top_football_nations = ["England", "Spain", "Germany", "Italy", "France", 
                            "Netherlands", "Portugal", "Brazil", "Argentina"]
    
    is_top_tier = ("premier" in league_name.lower() or 
                   "1" in league_name or 
                   "first" in league_name.lower() or
                   "a" == league_name[-1:])  # Serie A, Liga A patterns
                   
    if country_name in top_football_nations and is_top_tier and not is_cup:
        logger.info(f"Classified {league_name} as major league (top-tier in major nation)")
        return True
    
    # Default to non-major league
    logger.info(f"Classified {league_name} as non-major league (default)")
    return False

def get_league_configuration(league_code: str, is_major: bool) -> Dict:
    """
    Get league-specific configuration, including weight adjustments based on
    league characteristics and data availability
    """
    from config.leagues import DEFAULT_MAJOR_LEAGUE_CONFIG, DEFAULT_SMALLER_LEAGUE_CONFIG
    
    # Start with default config based on league type
    if is_major:
        config = DEFAULT_MAJOR_LEAGUE_CONFIG.copy()
    else:
        config = DEFAULT_SMALLER_LEAGUE_CONFIG.copy()
    
    # Apply league-specific adjustments if needed
    # This could be expanded with more detailed rules
    
    return config
