# fdr_calculator.py

import logging
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
import numpy as np
from pymongo.server_api import ServerApi
from pymongo.errors import ServerSelectionTimeoutError
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("fdr_calculation.log")]
)
logger = logging.getLogger(__name__)

class FDRCalculator:
    """Calculate Fixture Difficulty Ratings using data from MongoDB"""
    
    def __init__(self, mongo_uri=None):
        # Load environment variables
        load_dotenv()
        
        # MongoDB connection
        if mongo_uri is None:
            mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://naymul504:soupnaymul09@pf365.2pguj.mongodb.net/?retryWrites=true&w=majority&appName=pf365")
        
        self.client = MongoClient(mongo_uri, server_api=ServerApi('1'), serverSelectionTimeoutMS=5000)
        self.db = self.client['Analytics']
        
        # FDR color coding based on the 0-10 scale
        self.color_map = {
            "EASIEST": "#00E563",  # 0-2 (Very Easy)
            "EASIER": "#80FFB7",   # 3-4 (Easy)
            "AVERAGE": "#E1EBE5",  # 5-6 (Average)
            "TOUGH": "#FFA0A0",    # 7-8 (Difficult)
            "TOUGHEST": "#FF6060"  # 9-10 (Very Difficult)
        }
        
        # Category ranges
        self.category_ranges = {
            (0, 2): "EASIEST",
            (3, 4): "EASIER", 
            (5, 6): "AVERAGE",
            (7, 8): "TOUGH",
            (9, 10): "TOUGHEST"
        }
    
    def calculate_all_fixtures(self, days_ahead=14):
        """Calculate FDR for all upcoming fixtures"""
        logger.info("Starting FDR calculation for all upcoming fixtures")
        
        # Get upcoming fixtures
        start_date = datetime.now().strftime("%Y-%m-%d")
        end_date = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        
        fixtures = self.db.fixtures.find({
        "starting_at": {"$gte": start_date}  # Use "starting_at" instead of "match_date"
        })
        
        total_fixtures = 0
        calculated_fixtures = 0
        
        for fixture in fixtures:
            try:
                # Calculate FDR for this fixture
                result = self.calculate_fixture_fdr(fixture)
                if result:
                    calculated_fixtures += 1
                total_fixtures += 1
            except Exception as e:
                logger.error(f"Error calculating FDR for fixture {fixture.get('id')}: {str(e)}")
        
        logger.info(f"Completed FDR calculation: {calculated_fixtures}/{total_fixtures} fixtures processed")
        return calculated_fixtures

    def calculate_fixture_fdr(self, fixture):
        """Calculate FDR for a specific fixture"""
        fixture_id = fixture.get("id") or fixture.get("sportmonks_id")
        logger.info(f"Calculating FDR for fixture {fixture_id}")
        
        # Extract teams
        home_team_id = None
        away_team_id = None
        
        # Handle different data structures
        if "participants" in fixture:
            participants = fixture["participants"]
            if len(participants) >= 2:
                home_team_id = participants[0].get("id")
                away_team_id = participants[1].get("id")
        else:
            home_team_id = fixture.get("home_team_id")
            away_team_id = fixture.get("away_team_id")
        
        if not home_team_id or not away_team_id:
            logger.error(f"Unable to extract team IDs from fixture {fixture_id}")
            return False
        
        # Get league information
        league_id = fixture.get("league_id")
        if not league_id and "league" in fixture:
            league_id = fixture["league"].get("id")
        
        if not league_id:
            logger.error(f"League ID not found for fixture {fixture_id}")
            return False
        
        # Determine if this is a major league
        league = self.db.leagues.find_one({"id": league_id})
        is_major = league.get("is_major", False) if league else False
        
        # Check if this is a derby match
        is_derby = self.check_if_derby(home_team_id, away_team_id)
        
        # Calculate all components
        # 1. Historical Data (25% for major leagues, 35% for smaller leagues)
        home_historical, away_historical = self.calculate_historical_component(
            home_team_id, away_team_id, is_derby
        )
        
        # 2. Recent Form (15%)
        home_form = self.calculate_form_component(home_team_id)
        away_form = self.calculate_form_component(away_team_id)
        
        # 3. Season Outright (20% for major leagues only)
        home_outright = self.calculate_outright_component(home_team_id) if is_major else 0
        away_outright = self.calculate_outright_component(away_team_id) if is_major else 0
        
        # 4. Fixture Odds (30%)
        home_odds, away_odds = self.calculate_odds_component(fixture_id, home_team_id, away_team_id)
        
        # 5. Player Availability (10% for major leagues, 20% for smaller leagues)
        home_availability = self.calculate_availability_component(home_team_id)
        away_availability = self.calculate_availability_component(away_team_id)
        
        # Apply weights based on league type
        if is_major:
            # Major league formula (25%, 15%, 20%, 30%, 10%)
            home_fdr_raw = (
                0.25 * home_historical +
                0.15 * home_form +
                0.20 * home_outright +
                0.30 * home_odds +
                0.10 * home_availability
            )
            
            away_fdr_raw = (
                0.25 * away_historical +
                0.15 * away_form +
                0.20 * away_outright +
                0.30 * away_odds +
                0.10 * away_availability
            )
        else:
            # Smaller league formula (35%, 15%, 0%, 30%, 20%)
            home_fdr_raw = (
                0.35 * home_historical +
                0.15 * home_form +
                0.30 * home_odds +
                0.20 * home_availability
            )
            
            away_fdr_raw = (
                0.35 * away_historical +
                0.15 * away_form +
                0.30 * away_odds +
                0.20 * away_availability
            )
        
        # Scale FDR to 0-10 range
        home_fdr = self.scale_to_range(home_fdr_raw)
        away_fdr = self.scale_to_range(away_fdr_raw)
        
        # Calculate specialized metrics
        home_attacking_fdr = self.calculate_attacking_fdr(home_fdr_raw, home_historical, home_odds)
        away_attacking_fdr = self.calculate_attacking_fdr(away_fdr_raw, away_historical, away_odds)
        
        home_defending_fdr = self.calculate_defending_fdr(home_fdr_raw, home_historical, home_odds)
        away_defending_fdr = self.calculate_defending_fdr(away_fdr_raw, away_historical, away_odds)
        
        home_clean_sheet_fdr = self.calculate_clean_sheet_fdr(home_fdr_raw, home_historical, home_odds)
        away_clean_sheet_fdr = self.calculate_clean_sheet_fdr(away_fdr_raw, away_historical, away_odds)
        
        # Get categories and colors
        home_category = self.get_fdr_category(home_fdr)
        away_category = self.get_fdr_category(away_fdr)
        
        home_attacking_category = self.get_fdr_category(self.scale_to_range(home_attacking_fdr))
        away_attacking_category = self.get_fdr_category(self.scale_to_range(away_attacking_fdr))
        
        home_defending_category = self.get_fdr_category(self.scale_to_range(home_defending_fdr))
        away_defending_category = self.get_fdr_category(self.scale_to_range(away_defending_fdr))
        
        home_clean_sheet_category = self.get_fdr_category(self.scale_to_range(home_clean_sheet_fdr))
        away_clean_sheet_category = self.get_fdr_category(self.scale_to_range(away_clean_sheet_fdr))
        
        # Create FDR data structure
        fdr_data = {
            "overall": {
                "home": {
                    "raw_score": home_fdr_raw,
                    "fdr": home_fdr,
                    "category": home_category,
                    "color": self.color_map[home_category]
                },
                "away": {
                    "raw_score": away_fdr_raw,
                    "fdr": away_fdr,
                    "category": away_category,
                    "color": self.color_map[away_category]
                }
            },
            "attacking": {
                "home": {
                    "raw_score": home_attacking_fdr,
                    "fdr": self.scale_to_range(home_attacking_fdr),
                    "category": home_attacking_category,
                    "color": self.color_map[home_attacking_category]
                },
                "away": {
                    "raw_score": away_attacking_fdr,
                    "fdr": self.scale_to_range(away_attacking_fdr),
                    "category": away_attacking_category,
                    "color": self.color_map[away_attacking_category]
                }
            },
            "defending": {
                "home": {
                    "raw_score": home_defending_fdr,
                    "fdr": self.scale_to_range(home_defending_fdr),
                    "category": home_defending_category,
                    "color": self.color_map[home_defending_category]
                },
                "away": {
                    "raw_score": away_defending_fdr,
                    "fdr": self.scale_to_range(away_defending_fdr),
                    "category": away_defending_category,
                    "color": self.color_map[away_defending_category]
                }
            },
            "clean_sheet": {
                "home": {
                    "raw_score": home_clean_sheet_fdr,
                    "fdr": self.scale_to_range(home_clean_sheet_fdr),
                    "category": home_clean_sheet_category,
                    "color": self.color_map[home_clean_sheet_category]
                },
                "away": {
                    "raw_score": away_clean_sheet_fdr,
                    "fdr": self.scale_to_range(away_clean_sheet_fdr),
                    "category": away_clean_sheet_category,
                    "color": self.color_map[away_clean_sheet_category]
                }
            },
            "components": {
                "home": {
                    "historical": home_historical,
                    "form": home_form,
                    "outright": home_outright,
                    "odds": home_odds,
                    "availability": home_availability
                },
                "away": {
                    "historical": away_historical,
                    "form": away_form,
                    "outright": away_outright,
                    "odds": away_odds,
                    "availability": away_availability
                }
            },
            "is_derby": is_derby,
            "is_major_league": is_major,
            "calculated_at": datetime.now()
        }
        
        # Update fixture with FDR data
        self.db.fixtures.update_one(
            {"id": fixture_id} if "id" in fixture else {"sportmonks_id": fixture_id},
            {"$set": {"fdr": fdr_data}}
        )
        
        logger.info(f"FDR calculation completed for fixture {fixture_id}")
        return True
    
    def calculate_historical_component(self, home_team_id, away_team_id, is_derby=False):
        """Calculate historical performance component (25% or 35% of FDR)"""
        # Get historical matches between these teams
        historical_matches = self.db.historicalMatches.find({
            "$or": [
                {"home_team_id": home_team_id, "away_team_id": away_team_id},
                {"home_team_id": away_team_id, "away_team_id": home_team_id}
            ]
        }).sort("match_date", -1)
        
        # Count wins, draws, losses for home team
        home_wins = 0
        draws = 0
        away_wins = 0
        total_matches = 0
        
        # Apply recency weightage
        current_year = datetime.now().year
        weights = []
        weighted_matches = 0
        
        for match in historical_matches:
            match_date = match.get("match_date")
            if not match_date:
                continue
            
            # Extract year from date string
            try:
                if isinstance(match_date, str):
                    match_year = datetime.strptime(match_date, "%Y-%m-%d").year
                else:
                    match_year = match_date.year
            except:
                # Use current year minus 1 as fallback
                match_year = current_year - 1
            
            # Weight decreases by 10% per year
            weight = max(0.1, 1 - 0.1 * (current_year - match_year))
            weights.append(weight)
            
            # Determine match result
            if match.get("home_team_id") == home_team_id:
                home_score = match.get("home_score", 0)
                away_score = match.get("away_score", 0)
                
                if home_score > away_score:
                    home_wins += weight
                elif home_score == away_score:
                    draws += weight
                else:
                    away_wins += weight
            else:
                home_score = match.get("away_score", 0)
                away_score = match.get("home_score", 0)
                
                if home_score > away_score:
                    home_wins += weight
                elif home_score == away_score:
                    draws += weight
                else:
                    away_wins += weight
            
            weighted_matches += weight
            total_matches += 1
        
        # If there are no historical matches, return moderate difficulty
        if weighted_matches == 0:
            return 0.5, 0.5
        
        # Calculate weighted win rates
        home_win_rate = home_wins / weighted_matches
        draw_rate = draws / weighted_matches
        away_win_rate = away_wins / weighted_matches
        
        # Convert win rates to difficulty (1 - win_rate)
        # Higher win rate means easier fixture (lower difficulty)
        home_difficulty = 1 - home_win_rate - (0.5 * draw_rate)
        away_difficulty = 1 - away_win_rate - (0.5 * draw_rate)
        
        # Apply derby boost if applicable
        if is_derby:
            derby_boost = 0.15  # 15% boost to difficulty for derby matches
            home_difficulty = min(1.0, home_difficulty + derby_boost)
            away_difficulty = min(1.0, away_difficulty + derby_boost)
        
        return home_difficulty, away_difficulty
    
    def calculate_form_component(self, team_id):
        """Calculate recent form component (15% of FDR)"""
        # Get last 3 matches for this team
        recent_matches = self.db.historicalMatches.find({
            "$or": [
                {"home_team_id": team_id},
                {"away_team_id": team_id}
            ],
            "state_id": 5  # Completed matches only
        }).sort("match_date", -1).limit(3)
        
        recent_matches = list(recent_matches)
        
        # If there aren't enough recent matches, return moderate difficulty
        if len(recent_matches) < 3:
            return 0.5
        
        # Calculate form score with recency weighting
        form_points = 0
        max_points = 0
        
        for i, match in enumerate(recent_matches):
            # More recent matches have higher weight
            weight = 1.0 - (i * 0.2)  # 1.0, 0.8, 0.6
            max_points += weight * 3  # Maximum possible points
            
            if match.get("home_team_id") == team_id:
                home_score = match.get("home_score", 0)
                away_score = match.get("away_score", 0)
                
                if home_score > away_score:
                    form_points += weight * 3  # Win
                elif home_score == away_score:
                    form_points += weight * 1  # Draw
            else:
                home_score = match.get("home_score", 0)
                away_score = match.get("away_score", 0)
                
                if away_score > home_score:
                    form_points += weight * 3  # Win
                elif home_score == away_score:
                    form_points += weight * 1  # Draw
        
        # Calculate form ratio
        if max_points > 0:
            form_ratio = form_points / max_points
            
            # Convert form ratio to difficulty score (1 - form_ratio)
            # Higher form ratio means easier fixture (lower difficulty)
            return 1 - form_ratio
        
        # Fallback
        return 0.5
    
    def calculate_outright_component(self, team_id):
        """Calculate season outright component (20% of FDR for major leagues)"""
        # Get outright odds for this team
        outright_data = self.db.outrightOdds.find_one({
            "team_id": team_id,
            "market": "Championship Winner"
        })
        
        if not outright_data:
            # Try by team name
            team = self.db.teams.find_one({"id": team_id})
            if team:
                outright_data = self.db.outrightOdds.find_one({
                    "team": team.get("name"),
                    "market": "Championship Winner"
                })
        
        if not outright_data or not outright_data.get("odds"):
            # No outright data available, use default moderate difficulty
            return 0.5
        
        # Get best (lowest) odds across all bookmakers
        best_odds = float('inf')
        for odd_data in outright_data.get("odds", []):
            odd_value = odd_data.get("odd")
            if odd_value and odd_value < best_odds:
                best_odds = odd_value
        
        if best_odds == float('inf'):
            return 0.5
        
        # Convert odds to implied probability (1/odds)
        implied_probability = 1 / best_odds
        
        # Normalize probability (typical winning probabilities are 0.05-0.40)
        # Use logistic function to convert to 0-1 range
        normalized_probability = 1 / (1 + np.exp(-10 * (implied_probability - 0.2)))
        
        # Convert to difficulty score (1 - normalized_probability)
        # Higher probability of winning title = stronger team = easier fixture
        return 1 - normalized_probability
    
    def calculate_odds_component(self, fixture_id, home_team_id, away_team_id):
        """Calculate fixture odds component (30% of FDR)"""
        # Try goalserve odds first
        goalserve_odds = self.db.fixtureOdds.find_one({"match_id": str(fixture_id)})
        
        if goalserve_odds and goalserve_odds.get("odds"):
            for odds_type in goalserve_odds.get("odds", []):
                if odds_type.get("type_value") == "Match Winner" and odds_type.get("bookmakers"):
                    # Get average odds across bookmakers
                    home_odds = []
                    draw_odds = []
                    away_odds = []
                    
                    for bookmaker in odds_type.get("bookmakers", []):
                        if "home_odd" in bookmaker and "draw_odd" in bookmaker and "away_odd" in bookmaker:
                            home_odds.append(bookmaker["home_odd"])
                            draw_odds.append(bookmaker["draw_odd"])
                            away_odds.append(bookmaker["away_odd"])
                    
                    if home_odds and draw_odds and away_odds:
                        # Calculate average odds
                        avg_home_odd = sum(home_odds) / len(home_odds)
                        avg_draw_odd = sum(draw_odds) / len(draw_odds)
                        avg_away_odd = sum(away_odds) / len(away_odds)
                        
                        # Convert to probabilities
                        p_home = 1 / avg_home_odd
                        p_draw = 1 / avg_draw_odd
                        p_away = 1 / avg_away_odd
                        
                        # Normalize probabilities to sum to 1
                        total = p_home + p_draw + p_away
                        p_home /= total
                        p_draw /= total
                        p_away /= total
                        
                        # Calculate difficulty scores
                        home_difficulty = 1 - p_home - (0.5 * p_draw)
                        away_difficulty = 1 - p_away - (0.5 * p_draw)
                        
                        return home_difficulty, away_difficulty
        
        # If no goalserve odds, try sportmonks odds
        sportmonks_odds = self.db.sportmonksPrematchOdds.find({
            "fixture_id": fixture_id,
            "market_id": 1  # Match Winner market
        })
        
        sportmonks_odds = list(sportmonks_odds)
        
        if sportmonks_odds:
            home_odds = []
            draw_odds = []
            away_odds = []
            
            for odd in sportmonks_odds:
                if odd.get("label") == "1":  # Home win
                    home_odds.append(float(odd.get("value")))
                elif odd.get("label") == "X":  # Draw
                    draw_odds.append(float(odd.get("value")))
                elif odd.get("label") == "2":  # Away win
                    away_odds.append(float(odd.get("value")))
            
            if home_odds and draw_odds and away_odds:
                # Calculate average odds
                avg_home_odd = sum(home_odds) / len(home_odds)
                avg_draw_odd = sum(draw_odds) / len(draw_odds)
                avg_away_odd = sum(away_odds) / len(away_odds)
                
                # Convert to probabilities
                p_home = 1 / avg_home_odd
                p_draw = 1 / avg_draw_odd
                p_away = 1 / avg_away_odd
                
                # Normalize probabilities to sum to 1
                total = p_home + p_draw + p_away
                p_home /= total
                p_draw /= total
                p_away /= total
                
                # Calculate difficulty scores
                home_difficulty = 1 - p_home - (0.5 * p_draw)
                away_difficulty = 1 - p_away - (0.5 * p_draw)
                
                return home_difficulty, away_difficulty
        
        # If no odds data at all, use predictions if available
        predictions = self.db.predictions.find_one({
            "fixture_id": fixture_id,
            "type_id": 237  # Match winner prediction type
        })
        
        if predictions and "predictions" in predictions:
            pred_data = predictions["predictions"]
            p_home = pred_data.get("home", 0) / 100
            p_draw = pred_data.get("draw", 0) / 100
            p_away = pred_data.get("away", 0) / 100
            
            # Normalize probabilities
            total = p_home + p_draw + p_away
            if total > 0:
                p_home /= total
                p_draw /= total
                p_away /= total
                
                # Calculate difficulty scores
                home_difficulty = 1 - p_home - (0.5 * p_draw)
                away_difficulty = 1 - p_away - (0.5 * p_draw)
                
                return home_difficulty, away_difficulty
        
        # Fallback: use historical data
        return self.calculate_historical_component(home_team_id, away_team_id)[0], \
               self.calculate_historical_component(home_team_id, away_team_id)[1]
    
    def calculate_availability_component(self, team_id):
        """Calculate player availability component (10% or 20% of FDR)"""
        # Get player availability for this team
        player_data = self.db.playerAvailability.find({
            "team_id": team_id,
            "completed": False,  # Only active injuries/suspensions
            "end_date": {"$gte": datetime.now().strftime("%Y-%m-%d")}  # Only current issues
        })
        
        player_data = list(player_data)
        
        if not player_data:
            # No player availability issues, return 0 (best scenario)
            return 0.0
        
        # Count non-available players
        missing_players = len(player_data)
        
        # Apply a simple scaling function
        # 0 players missing = 0.0, 3+ players missing = 1.0
        difficulty = min(1.0, missing_players / 3)
        
        return difficulty
    
    def calculate_attacking_fdr(self, overall_fdr, historical, odds):
        """Calculate attacking-specific FDR"""
        # For attacking difficulty, weight recent form and odds more heavily
        return 0.7 * overall_fdr + 0.3 * (1 - odds)
    
    def calculate_defending_fdr(self, overall_fdr, historical, odds):
        """Calculate defending-specific FDR"""
        # For defending difficulty, weight historical performance more heavily
        return 0.6 * overall_fdr + 0.4 * historical
    
    def calculate_clean_sheet_fdr(self, overall_fdr, historical, odds):
        """Calculate clean sheet potential FDR"""
        # For clean sheet potential, historical defensive strength is key
        return 0.5 * overall_fdr + 0.5 * historical
    
    def check_if_derby(self, team1_id, team2_id):
        """Check if this match is a derby"""
        # Check in rivals collection
        derby = self.db.rivals.find_one({
            "$or": [
                {"team_id": team1_id, "rival_id": team2_id},
                {"team_id": team2_id, "rival_id": team1_id}
            ]
        })
        
        return True if derby else False
    
    def scale_to_range(self, score, old_min=0, old_max=1, new_min=0, new_max=10):
        """Scale a score from one range to another"""
        # Ensure score is within bounds
        score = max(old_min, min(old_max, score))
        
        # Scale to new range
        return (score - old_min) * (new_max - new_min) / (old_max - old_min) + new_min
    
    def get_fdr_category(self, score):
        """Get the FDR category based on the score"""
        for (low, high), category in self.category_ranges.items():
            if low <= score <= high:
                return category
        
        # Default to TOUGHEST for scores > 10
        return "TOUGHEST"


# Usage example
if __name__ == "__main__":
    # Create an instance of the FDR calculator
    calculator = FDRCalculator()
    
    # Calculate FDR for all upcoming fixtures
    calculator.calculate_all_fixtures(days_ahead=14)