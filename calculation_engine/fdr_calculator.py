# fdr_calculator.py

import logging
from pymongo import MongoClient
from datetime import datetime, timedelta
import os
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
import numpy as np
from pymongo.server_api import ServerApi
import re
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
            (0, 2.99): "EASIEST",
            (3, 4.99): "EASIER", 
            (5, 6.99): "AVERAGE",
            (7, 8.99): "TOUGH",
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
            home_fdr_raw = (
                0.20 * home_historical +  # Historical: 20%
                0.20 * home_form +        # Recent Form: 20%
                0.15 * home_outright +    # Outright: 15%
                0.40 * home_odds +        # Fix Odds: 40%
                0.05 * home_availability  # Player Availability: 5%
            )
            
            away_fdr_raw = (
                0.20 * away_historical +
                0.20 * away_form +
                0.15 * away_outright +
                0.40 * away_odds +
                0.05 * away_availability
            )
        else:
            # Smaller league formula (35%, 15%, 0%, 30%, 20%)
            home_fdr_raw = (
                0.30 * home_historical +  # Increased historical importance
                0.20 * home_form +
                0.40 * home_odds +
                0.10 * home_availability  # Slightly increased for smaller leagues
            )
            
            away_fdr_raw = (
                0.30 * away_historical +
                0.20 * away_form +
                0.40 * away_odds +
                0.10 * away_availability
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
            # Convert team IDs to integers if they're strings
            if isinstance(home_team_id, str):
                home_team_id = int(home_team_id)
            if isinstance(away_team_id, str):
                away_team_id = int(away_team_id)
                
            # Generate slightly different values based on team IDs for variety
            home_value = 0.45 + (home_team_id % 10) / 100
            away_value = 0.45 + (away_team_id % 10) / 100
            return min(0.65, home_value), min(0.65, away_value)
        
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
        print("Not able to counter form")
    
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
        """Calculate fixture odds component (30% of FDR) with improved ID mapping"""
        
        # First check for a mapping in our dedicated mapping collection
        mapping = self.db.fixture_id_mapping.find_one({"sportmonks_id": fixture_id})
        goalserve_id = mapping.get("goalserve_id") if mapping else None
        
        # Try multiple strategies to find odds
        goalserve_odds = None
        
        # Strategy 1: Direct Sportmonks ID match
        if not goalserve_odds:
            goalserve_odds = self.db.fixtureOdds.find_one({"sportmonks_id": fixture_id})
        
        # Strategy 2: GoalServe ID from mapping
        if not goalserve_odds and goalserve_id:
            goalserve_odds = self.db.fixtureOdds.find_one({"match_id": goalserve_id})
        
        # Strategy 3: Team name + date matching as fallback
        if not goalserve_odds:
            # Get fixture details
            fixture = self.db.fixtures.find_one({"id": fixture_id})
            if fixture and "participants" in fixture and len(fixture["participants"]) >= 2:
                home_team_name = fixture["participants"][0].get("name", "")
                away_team_name = fixture["participants"][1].get("name", "")
                match_date = fixture["starting_at"].split(" ")[0] if " " in fixture["starting_at"] else ""
                
                # Get teams with normalized names
                home_normalized = self._normalize_team_name(home_team_name)
                away_normalized = self._normalize_team_name(away_team_name)
                
                # Try to find by team names with fuzzy matching
                potential_matches = list(self.db.fixtureOdds.find({
                    "$or": [
                        {"local_team.name": {"$regex": home_team_name.split(" ")[0], "$options": "i"}},
                        {"visitor_team.name": {"$regex": away_team_name.split(" ")[0], "$options": "i"}}
                    ]
                }))
                
                best_match = None
                best_score = 0
                
                for match in potential_matches:
                    local_normalized = self._normalize_team_name(match.get("local_team", {}).get("name", ""))
                    visitor_normalized = self._normalize_team_name(match.get("visitor_team", {}).get("name", ""))
                    
                    # Calculate match scores both ways (in case teams are reversed)
                    direct_score = (
                        self._fuzzy_match_score(home_normalized, local_normalized) +
                        self._fuzzy_match_score(away_normalized, visitor_normalized)
                    )
                    
                    reverse_score = (
                        self._fuzzy_match_score(home_normalized, visitor_normalized) +
                        self._fuzzy_match_score(away_normalized, local_normalized)
                    )
                    
                    match_score = max(direct_score, reverse_score)
                    
                    # Date matching bonus
                    match_date_str = match.get("match_date", "")
                    if match_date and match_date_str:
                        normalized_date = self._normalize_date(match_date_str)
                        if normalized_date and normalized_date == match_date:
                            match_score += 40
                    
                    if match_score > best_score and match_score >= 160:  # Threshold for good match
                        best_score = match_score
                        best_match = match
                
                if best_match:
                    goalserve_odds = best_match
                    
                    # Update mapping for future use
                    self.db.fixture_id_mapping.update_one(
                        {"sportmonks_id": fixture_id},
                        {"$set": {
                            "goalserve_id": best_match["match_id"],
                            "confidence": best_score,
                            "last_updated": datetime.now()
                        }},
                        upsert=True
                    )
        
        if goalserve_odds and goalserve_odds.get("odds"):
            for odds_type in goalserve_odds.get("odds", []):
                if odds_type.get("type_value") == "Match Winner" and odds_type.get("bookmakers"):
                    # Calculate average odds across all bookmakers
                    home_odds = []
                    draw_odds = []
                    away_odds = []
                    
                    for bookmaker in odds_type.get("bookmakers", []):
                        home_odds.append(bookmaker.get("home_odd"))
                        draw_odds.append(bookmaker.get("draw_odd"))
                        away_odds.append(bookmaker.get("away_odd"))
                    
                    if home_odds and draw_odds and away_odds:
                        avg_home = sum(home_odds) / len(home_odds)
                        avg_draw = sum(draw_odds) / len(draw_odds)
                        avg_away = sum(away_odds) / len(away_odds)
                        
                        # Convert to probabilities
                        p_home = 1 / avg_home
                        p_draw = 1 / avg_draw
                        p_away = 1 / avg_away
                        
                        # Normalize probabilities
                        total = p_home + p_draw + p_away
                        p_home /= total
                        p_draw /= total
                        p_away /= total
                        
                        # Calculate difficulty scores
                        home_diff = 1 - p_home - (0.5 * p_draw)
                        away_diff = 1 - p_away - (0.5 * p_draw)
                        
                        return home_diff, away_diff
        
        # Fallback: use historical data
        return self.calculate_historical_component(home_team_id, away_team_id)[0], \
               self.calculate_historical_component(home_team_id, away_team_id)[1]
    
    def _normalize_team_name(self, name):
        """Helper method to normalize team names"""
        if not name:
            return ""
        
        name = name.lower()
        
        # Remove common suffixes
        suffixes = [" fc", " cf", " united", " utd", " city", " athletic"]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        
        # Remove special characters and extra spaces
        name = re.sub(r'[^\w\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name

    def _normalize_date(self, date_str):
        """Helper method to normalize date formats"""
        if not date_str:
            return None
            
        # Handle "May 14" format
        match = re.match(r"(\w+)\s+(\d+)", date_str)
        if match:
            month_name, day = match.groups()
            current_year = datetime.now().year
            try:
                date_obj = datetime.strptime(f"{month_name} {day} {current_year}", "%b %d %Y")
                # If date is in the past by more than a week, it might be next year
                if (datetime.now() - date_obj).days > 7:
                    date_obj = datetime.strptime(f"{month_name} {day} {current_year+1}", "%b %d %Y")
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                pass
        
        # Already in ISO format
        if re.match(r"\d{4}-\d{2}-\d{2}", date_str):
            return date_str
            
        return None

    def _fuzzy_match_score(self, str1, str2):
        """Calculate fuzzy matching score between two strings"""
        if not str1 or not str2:
            return 0
        return fuzz.ratio(str1, str2)
    
    
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
        if score <= 2.99:
            return "EASIEST"
        elif score <= 4.99:
            return "EASIER"
        elif score <= 6.99:
            return "AVERAGE"
        elif score <= 8.99:
            return "TOUGH"
        else:
            return "TOUGHEST"
        
    def calculate_xg_from_scoreline(self, fixture_id):
        """Calculate expected goals from scoreline probabilities"""
        if isinstance(fixture_id, int) or fixture_id.isdigit():
            # This might be a SportMonks ID, check for mapping
            mapping = self.db.fixture_id_mapping.find_one({"sportmonks_id": int(fixture_id)})
            if mapping:
                # Get the corresponding Goalserve ID
                goalserve_id = mapping["goalserve_id"]
            else:
                goalserve_id = fixture_id
        else:
            goalserve_id = fixture_id
            
        # Rest of your existing function using goalserve_id
        probabilities = list(self.db.scorelineProbabilities.find({
            "fixture_id": goalserve_id,
            "source": "goalserve"
        }))
        # probabilities = list(self.db.scorelineProbabilities.find({"fixture_id": fixture_id}))
    
        if not probabilities:
            # Check if we have direct xG data in the database instead
            xg_data = self.db.fixtureExpectedGoals.find_one({"fixture_id": fixture_id})
            if xg_data:
                return xg_data.get("home_xg"), xg_data.get("away_xg")
            logger.warning(f"No xG data available for fixture {fixture_id}")
            return None, None
        
        home_xg = 0
        away_xg = 0
        
        # Calculate xG by summing (goals × probability)
        for prob in probabilities:
            home_goals = prob.get("home_goals", 0)
            away_goals = prob.get("away_goals", 0)
            # Fix: Convert percentage to decimal probability properly (7.169% -> 0.07169)
            probability = prob.get("probability", 0) / 100
            
            home_xg += home_goals * probability
            away_xg += away_goals * probability
        
        return home_xg, away_xg
    
    def get_league_average_xg(self, league_id):
        """Get league average xG for home and away teams"""
        league_avg = self.db.leagueAverages.find_one({"league_id": league_id})
        
        if league_avg:
            return league_avg.get("home_xg_avg", 1.4), league_avg.get("away_xg_avg", 1.1)
        
        # Default values based on typical averages in top leagues
        return 1.4, 1.1
    
    def calculate_league_averages(self):
        """Calculate league average xG values from historical matches"""
        leagues = self.db.leagues.find({})
        
        for league in leagues:
            league_id = league["id"]
            
            # Get historical matches for this league
            matches = self.db.historicalMatches.find({"league_id": league_id})
            
            home_xg_values = []
            away_xg_values = []
            
            for match in matches:
                # Extract xG values if available
                if "stats" in match and "xg" in match["stats"]:
                    home_xg = match["stats"]["xg"].get("home")
                    away_xg = match["stats"]["xg"].get("away")
                    
                    if home_xg is not None and away_xg is not None:
                        home_xg_values.append(home_xg)
                        away_xg_values.append(away_xg)
            
            # Calculate averages
            if home_xg_values and away_xg_values:
                home_avg = sum(home_xg_values) / len(home_xg_values)
                away_avg = sum(away_xg_values) / len(away_xg_values)
                
                # Save to MongoDB
                self.db.leagueAverages.update_one(
                    {"league_id": league_id},
                    {"$set": {
                        "home_xg_avg": home_avg,
                        "away_xg_avg": away_avg,
                        "last_updated": datetime.now()
                    }},
                    upsert=True
                )
                logger.info(f"Updated xG averages for league {league['name']}: Home {home_avg:.2f}, Away {away_avg:.2f}")
            else:
                # Use default values if no historical xG data
                self.db.leagueAverages.update_one(
                    {"league_id": league_id},
                    {"$set": {
                        "home_xg_avg": 1.4,
                        "away_xg_avg": 1.1,
                        "last_updated": datetime.now(),
                        "is_default": True
                    }},
                    upsert=True
                )
                logger.info(f"Set default xG averages for league {league['name']} (no historical data)")

    def convert_strength_to_fdr(self, strength):
        """Convert strength coefficient to FDR scale (0-10)"""
        # Strength of 1.0 is average (FDR 5)
        # Higher strength = easier fixture = lower FDR
        if strength >= 1.0:
            fdr = 5 - (strength - 1.0) * 5  # Easier (below 5)
        else:
            fdr = 5 + (1.0 - strength) * 5  # Harder (above 5)
        
        return max(0, min(10, fdr))  # Ensure within 0-10 range

    def calculate_fixture_fdr_new(self, fixture, use_odds_heavy=True):
        """Calculate FDR using the odds-heavy xG approach"""
        fixture_id = fixture.get("id")
        league_id = fixture.get("league_id") or fixture.get("league", {}).get("id")
        
        logger.info(f"Calculating {'odds-heavy' if use_odds_heavy else 'pure xG'} FDR for fixture {fixture_id}")
        
        # Extract team information
        home_team_id = None
        away_team_id = None
        
        if "participants" in fixture and len(fixture["participants"]) >= 2:
            home_team_id = fixture["participants"][0].get("id")
            away_team_id = fixture["participants"][1].get("id")
        else:
            home_team_id = fixture.get("local_team", {}).get("id")
            away_team_id = fixture.get("visitor_team", {}).get("id")
        
        if not home_team_id or not away_team_id:
            logger.warning(f"Missing team IDs for fixture {fixture_id}")
            return False
        
        # Calculate xG from scoreline probabilities
        home_xg, away_xg = self.calculate_xg_from_scoreline(fixture_id)
        
        # Fallback if no data available
        if home_xg is None:
            # Try historical component as fallback
            historical = self.calculate_historical_component(home_team_id, away_team_id)
            home_xg = 1.4 * (1 - historical[0])  # Convert difficulty to expected goals
            away_xg = 1.1 * (1 - historical[1])
            logger.warning(f"Using historical fallback for fixture {fixture_id}: {home_xg:.2f}, {away_xg:.2f}")
        
        # Get league average xG
        home_league_avg, away_league_avg = self.get_league_average_xg(league_id)
        
        # Store original xG for reference
        original_home_xg = home_xg
        original_away_xg = away_xg
        
        # Apply odds-heavy approach if requested
        if use_odds_heavy:
            home_odds, away_odds = self.calculate_odds_component(fixture_id, home_team_id, away_team_id)
            
            # Convert odds difficulty to strength factor
            odds_factor_home = 2 - (1.5 * home_odds)
            odds_factor_away = 2 - (1.5 * away_odds)
            
            # Apply 60% odds weight, 40% pure xG weight
            home_xg = (0.4 * home_xg) + (0.6 * odds_factor_home * home_league_avg)
            away_xg = (0.4 * away_xg) + (0.6 * odds_factor_away * away_league_avg)
        
        # Calculate team strength metrics
        home_attack = home_xg / home_league_avg
        away_attack = away_xg / away_league_avg
        home_defense = away_xg / away_league_avg
        away_defense = home_xg / home_league_avg
        
        # Calculate fixture strength
        home_fixture_strength = home_attack * away_defense
        away_fixture_strength = away_attack * home_defense
        
        # Convert to FDR scale (0-10)
        home_fdr = self.convert_strength_to_fdr(home_fixture_strength)
        away_fdr = self.convert_strength_to_fdr(away_fixture_strength)
        
        # Get categories
        home_category = self.get_fdr_category(home_fdr)
        away_category = self.get_fdr_category(away_fdr)
        
        # Create and save FDR data
        fdr_data = {
            "overall": {
                "home": {
                    "raw_score": home_fixture_strength,
                    "fdr": home_fdr,
                    "category": home_category,
                    "color": self.color_map[home_category],
                    "xg": home_xg
                },
                "away": {
                    "raw_score": away_fixture_strength,
                    "fdr": away_fdr,
                    "category": away_category,
                    "color": self.color_map[away_category],
                    "xg": away_xg
                }
            },
            "components": {
                "home": {
                    "attack_strength": home_attack,
                    "defense_weakness": home_defense
                },
                "away": {
                    "attack_strength": away_attack,
                    "defense_weakness": away_defense
                }
            },
            "calculation_method": "xg_based",
            "calculated_at": datetime.now()
        }
        print(f"FDR data: {fdr_data}")
        self.db.fixtures.update_one(
            {"id": fixture_id},
            {"$set": {"fdr": fdr_data}}
        )
        
        logger.info(f"Completed xG-based FDR calculation for fixture {fixture_id}")
        return True
    
    
    def test_new_fdr_calculation(self):
        """Test the new xG-based FDR calculation with fixtures having scoreline data"""
        # Find fixtures with scoreline probability data
        scoreline_fixtures = list(self.db.scorelineProbabilities.distinct("fixture_id"))
        
        if not scoreline_fixtures:
            logger.error("No fixtures with scoreline probabilities found")
            return
        
        logger.info(f"Found {len(scoreline_fixtures)} fixtures with scoreline probabilities")
        
        test_count = min(len(scoreline_fixtures), 5)  # Test up to 5 fixtures
        
        for i, fixture_id in enumerate(scoreline_fixtures[:test_count]):
            fixture = self.db.fixtures.find_one({"id": fixture_id})
            
            if not fixture:
                logger.warning(f"Fixture {fixture_id} not found in fixtures collection")
                continue
            
            # Get team names for logging
            home_name = "Unknown"
            away_name = "Unknown"
            
            if "participants" in fixture and len(fixture["participants"]) >= 2:
                home_name = fixture["participants"][0].get("name", "Home")
                away_name = fixture["participants"][1].get("name", "Away")
            
            logger.info(f"Testing fixture {i+1}/{test_count}: {home_name} vs {away_name}")
            
            # Calculate xG
            home_xg, away_xg = self.calculate_xg_from_scoreline(fixture_id)
            logger.info(f"Calculated xG - Home: {home_xg:.2f}, Away: {away_xg:.2f}")
            
            # Calculate FDR
            result = self.calculate_fixture_fdr_new(fixture)
            
            # Retrieve and display the FDR data
            updated_fixture = self.db.fixtures.find_one({"id": fixture_id})
            if "fdr" in updated_fixture:
                fdr_data = updated_fixture["fdr"]
                logger.info(f"Home FDR: {fdr_data['overall']['home']['fdr']:.2f}, " +
                            f"Away FDR: {fdr_data['overall']['away']['fdr']:.2f}")
                
                # Show attack and defense values
                logger.info(f"Home Attack: {fdr_data['components']['home']['attack_strength']:.2f}, " +
                            f"Home Defense: {fdr_data['components']['home']['defense_weakness']:.2f}")
                logger.info(f"Away Attack: {fdr_data['components']['away']['attack_strength']:.2f}, " +
                            f"Away Defense: {fdr_data['components']['away']['defense_weakness']:.2f}")
            
            logger.info("-" * 50)


    # def test_xg_calculation(self):
    #     """Test xG calculation from scoreline probabilities"""
    #     scoreline_fixtures = self.db.scorelineProbabilities.distinct("fixture_id")
        
    #     if not scoreline_fixtures:
    #         logger.error("No fixtures with scoreline probabilities found")
    #         return
        
    #     logger.info(f"Found {len(scoreline_fixtures)} fixtures with scoreline probabilities")
        
    #     for fixture_id in scoreline_fixtures:
    #         # First check in fixture_id_mapping collection
    #         mapping = self.db.fixture_id_mapping.find_one({"sportmonks_id": fixture_id})
            
    #         if mapping:
    #             fixture = self.db.fixtures.find_one({"id": fixture_id})
    #         # else:
    #         #     # Try direct lookup with multiple approaches
    #         #     fixture = self.db.fixtures.find_one({"id": fixture_id})
                
    #         #     # Try other options if not found
    #         #     if not fixture:
    #         #         fixture = self.db.fixtures.find_one({"sportmonks_id": fixture_id})
                
    #         #     if not fixture:
    #         #         # Try string/int conversion
    #         #         try:
    #         #             if isinstance(fixture_id, str):
    #         #                 fixture = self.db.fixtures.find_one({"id": int(fixture_id)})
    #         #             else:
    #         #                 fixture = self.db.fixtures.find_one({"id": str(fixture_id)})
    #         #         except (ValueError, TypeError):
    #         #             pass
            
    #         if not fixture:
    #             logger.warning(f"Could not find fixture for ID {fixture_id}")
    #             continue
                
    #         # Get team names for logging
    #         home_name = "Unknown"
    #         away_name = "Unknown"
    #         if "participants" in fixture and len(fixture["participants"]) >= 2:
    #             home_name = fixture["participants"][0].get("name", "Home")
    #             away_name = fixture["participants"][1].get("name", "Away")
            
    #         # Calculate xG
    #         home_xg, away_xg = self.calculate_xg_from_scoreline(fixture_id)
    #         if home_xg is None:
    #             logger.warning(f"Could not calculate xG for {home_name} vs {away_name}")
    #             continue
                    
    #         logger.info(f"Fixture: {home_name} vs {away_name}")
    #         logger.info(f"xG values - Home: {home_xg:.2f}, Away: {away_xg:.2f}")
            
    #         # Calculate league averages
    #         league_id = fixture.get("league", {}).get("id")
    #         home_league_avg, away_league_avg = self.get_league_average_xg(league_id)
    #         logger.info(f"League averages - Home: {home_league_avg:.2f}, Away: {away_league_avg:.2f}")
            
    #         # Calculate strength coefficients
    #         home_attack = home_xg / home_league_avg
    #         away_attack = away_xg / away_league_avg
    #         home_defense = away_xg / away_league_avg
    #         away_defense = home_xg / home_league_avg
            
    #         logger.info(f"Home attack: {home_attack:.2f}, Home defense: {home_defense:.2f}")
    #         logger.info(f"Away attack: {away_attack:.2f}, Away defense: {away_defense:.2f}")
            
    #         # Calculate fixture strength
    #         home_strength = home_attack * away_defense
    #         away_strength = away_attack * home_defense
            
    #         logger.info(f"Fixture strength - Home: {home_strength:.2f}, Away: {away_strength:.2f}")
            
    #         # Calculate FDR
    #         home_fdr = self.convert_strength_to_fdr(home_strength)
    #         away_fdr = self.convert_strength_to_fdr(away_strength)
            
    #         logger.info(f"FDR values - Home: {home_fdr:.2f}, Away: {away_fdr:.2f}")
    #         logger.info("-" * 50)

    def test_odds_heavy_calculation(self):
        """Test the odds-heavy xG calculation method"""
        scoreline_fixtures = self.db.scorelineProbabilities.distinct("fixture_id")
        
        if not scoreline_fixtures:
            logger.error("No fixtures with scoreline probabilities found")
            return
        
        logger.info(f"Found {len(scoreline_fixtures)} fixtures with scoreline probabilities")
        
        for fixture_id in scoreline_fixtures:
            # Find fixture with proper handling for Goalserve structure
            fixture = self.db.fixtures.find_one({"id": fixture_id})
            
            if not fixture:
                logger.warning(f"Could not find fixture for ID {fixture_id}")
                continue
                
            # Extract team names with support for both data structures
            home_name = "Unknown"
            away_name = "Unknown"
            
            if "participants" in fixture and len(fixture["participants"]) >= 2:
                home_name = fixture["participants"][0].get("name", "Home")
                away_name = fixture["participants"][1].get("name", "Away")
            elif "local_team" in fixture and "visitor_team" in fixture:
                home_name = fixture["local_team"].get("name", "Home")
                away_name = fixture["visitor_team"].get("name", "Away")
            
            # Extract team IDs with support for both data structures
            home_team_id = None
            away_team_id = None
            
            if "participants" in fixture and len(fixture["participants"]) >= 2:
                home_team_id = fixture["participants"][0].get("id")
                away_team_id = fixture["participants"][1].get("id")
            elif "local_team" in fixture and "visitor_team" in fixture:
                home_team_id = fixture["local_team"].get("id")
                away_team_id = fixture["visitor_team"].get("id")
            
            if not home_team_id or not away_team_id:
                logger.warning(f"Missing team IDs for fixture {fixture_id}")
                continue
            
            # Get league ID with support for category_id as fallback
            league_id = fixture.get("league_id") or fixture.get("league", {}).get("id") or fixture.get("category_id")
            
            if not league_id:
                logger.warning(f"No league ID found for fixture {fixture_id}")
                continue
            
            # Calculate standard xG
            home_xg, away_xg = self.calculate_xg_from_scoreline(fixture_id)
            if home_xg is None:
                logger.warning(f"Could not calculate xG for {home_name} vs {away_name}")
                continue
                    
            logger.info(f"Fixture: {home_name} vs {away_name}")
            logger.info(f"Pure xG values - Home: {home_xg:.2f}, Away: {away_xg:.2f}")
            
            # Calculate league averages
            home_league_avg, away_league_avg = self.get_league_average_xg(league_id)
            logger.info(f"League averages - Home: {home_league_avg:.2f}, Away: {away_league_avg:.2f}")
            
            # Get odds component
            home_odds, away_odds = self.calculate_odds_component(fixture_id, home_team_id, away_team_id)
            logger.info(f"Odds component - Home: {home_odds:.2f}, Away: {away_odds:.2f}")
            
            # Create odds-adjusted xG values (60% odds influence)
            odds_factor_home = 2 - (1.5 * home_odds)  # Convert odds difficulty to strength factor
            odds_factor_away = 2 - (1.5 * away_odds)
            
            logger.info(f"Odds factors - Home: {odds_factor_home:.2f}, Away: {odds_factor_away:.2f}")
            
            # Apply 60% odds weight, 40% pure xG weight
            home_xg_adjusted = (0.5 * home_xg) + (0.5 * odds_factor_home * home_league_avg)
            away_xg_adjusted = (0.5 * away_xg) + (0.5 * odds_factor_away * away_league_avg)
            
            logger.info(f"Adjusted xG values - Home: {home_xg_adjusted:.2f}, Away: {away_xg_adjusted:.2f}")
            
            # Calculate strength coefficients using adjusted xG
            home_attack = home_xg_adjusted / home_league_avg
            away_attack = away_xg_adjusted / away_league_avg
            home_defense = away_xg_adjusted / away_league_avg
            away_defense = home_xg_adjusted / home_league_avg
            
            logger.info(f"Adjusted strengths - Home attack: {home_attack:.2f}, Home defense: {home_defense:.2f}")
            logger.info(f"Adjusted strengths - Away attack: {away_attack:.2f}, Away defense: {away_defense:.2f}")
            
            # Calculate fixture strength
            home_strength = home_attack * away_defense
            away_strength = away_attack * home_defense
            
            logger.info(f"Fixture strength - Home: {home_strength:.2f}, Away: {away_strength:.2f}")
            
            # Calculate FDR
            home_fdr = self.convert_strength_to_fdr(home_strength)
            away_fdr = self.convert_strength_to_fdr(away_strength)
            
            logger.info(f"Odds-heavy FDR - Home: {home_fdr:.2f}, Away: {away_fdr:.2f}")
            logger.info("-" * 50)
    
    def test_comprehensive_fdr_calculation(self):
        """Test the comprehensive multi-factor FDR calculation method"""
        scoreline_fixtures = self.db.scorelineProbabilities.distinct("fixture_id")
        
        if not scoreline_fixtures:
            logger.error("No fixtures with scoreline probabilities found")
            return
        
        logger.info(f"Found {len(scoreline_fixtures)} fixtures with scoreline probabilities")
        
        for fixture_id in scoreline_fixtures:
            # Find fixture with proper handling for Goalserve structure
            fixture = self.db.fixtures.find_one({"id": fixture_id})
            
            if not fixture:
                logger.warning(f"Could not find fixture for ID {fixture_id}")
                continue
                
            # Extract team names with support for both data structures
            home_name = "Unknown"
            away_name = "Unknown"
            
            if "participants" in fixture and len(fixture["participants"]) >= 2:
                home_name = fixture["participants"][0].get("name", "Home")
                away_name = fixture["participants"][1].get("name", "Away")
            elif "local_team" in fixture and "visitor_team" in fixture:
                home_name = fixture["local_team"].get("name", "Home")
                away_name = fixture["visitor_team"].get("name", "Away")
            
            # Extract team IDs with support for both data structures
            home_team_id = None
            away_team_id = None
            
            if "participants" in fixture and len(fixture["participants"]) >= 2:
                home_team_id = fixture["participants"][0].get("id")
                away_team_id = fixture["participants"][1].get("id")
            elif "local_team" in fixture and "visitor_team" in fixture:
                home_team_id = fixture["local_team"].get("id")
                away_team_id = fixture["visitor_team"].get("id")
            
            if not home_team_id or not away_team_id:
                logger.warning(f"Missing team IDs for fixture {fixture_id}")
                continue
            
            # Get league ID with support for category_id as fallback
            league_id = fixture.get("league_id") or fixture.get("league", {}).get("id") or fixture.get("category_id")
            
            if not league_id:
                logger.warning(f"No league ID found for fixture {fixture_id}")
                continue
            
            # Calculate standard xG
            home_xg, away_xg = self.calculate_xg_from_scoreline(fixture_id)
            if home_xg is None:
                logger.warning(f"Could not calculate xG for {home_name} vs {away_name}")
                continue
            
            logger.info(f"Fixture: {home_name} vs {away_name}")
            logger.info(f"Pure xG values - Home: {home_xg:.2f}, Away: {away_xg:.2f}")
            
            # Calculate league averages
            home_league_avg, away_league_avg = self.get_league_average_xg(league_id)
            logger.info(f"League averages - Home: {home_league_avg:.2f}, Away: {away_league_avg:.2f}")
            
            # Get odds component
            home_odds, away_odds = self.calculate_odds_component(fixture_id, home_team_id, away_team_id)
            logger.info(f"Odds component - Home: {home_odds:.2f}, Away: {away_odds:.2f}")
            
            # Get additional components
            historical = self.calculate_historical_component(home_team_id, away_team_id)
            home_outright = self.calculate_outright_component(home_team_id)
            away_outright = self.calculate_outright_component(away_team_id)
            home_availability = self.calculate_availability_component(home_team_id)
            away_availability = self.calculate_availability_component(away_team_id)
            
            logger.info(f"Historical - Home: {historical[0]:.2f}, Away: {historical[1]:.2f}")
            logger.info(f"Outright - Home: {home_outright:.2f}, Away: {away_outright:.2f}")
            logger.info(f"Availability - Home: {home_availability:.2f}, Away: {away_availability:.2f}")
            
            # Convert odds difficulty to strength factor
            odds_factor_home = 2 - (1.5 * home_odds)
            odds_factor_away = 2 - (1.5 * away_odds)
            
            logger.info(f"Odds factors - Home: {odds_factor_home:.2f}, Away: {odds_factor_away:.2f}")
            
            # Apply comprehensive weighting with odds emphasis (40-40-10-5-5 distribution)
            home_xg_adjusted = (
                0.20 * home_xg +                                   # Raw xG: 20%
                0.60 * (odds_factor_home * home_league_avg) +      # Fixture Odds: 60% 
                0.10 * (1.0 - historical[0]) * home_league_avg +   # Historical: 10%
                0.05 * (1.0 - home_outright) * home_league_avg +   # Outright: 5%
                0.05 * (1.0 - home_availability) * home_league_avg # Availability: 5%
            )

            
            away_xg_adjusted = (
                0.20 * away_xg +                                   # Raw xG: 40%
                0.60 * (odds_factor_away * away_league_avg) +      # Fixture Odds: 40%
                0.10 * (1.0 - historical[1]) * away_league_avg +   # Historical: 10%
                0.05 * (1.0 - away_outright) * away_league_avg +    # Outright: 5%
                0.05 * (1.0 - away_availability) * away_league_avg  # Availability: 5%
            )
            
            logger.info(f"Adjusted xG values - Home: {home_xg_adjusted:.2f}, Away: {away_xg_adjusted:.2f}")
            
            # Calculate strength coefficients using adjusted xG
            home_attack = home_xg_adjusted / home_league_avg
            away_attack = away_xg_adjusted / away_league_avg
            home_defense = away_xg_adjusted / away_league_avg
            away_defense = home_xg_adjusted / home_league_avg
            
            logger.info(f"Adjusted strengths - Home attack: {home_attack:.2f}, Home defense: {home_defense:.2f}")
            logger.info(f"Adjusted strengths - Away attack: {away_attack:.2f}, Away defense: {away_defense:.2f}")
            
            # Calculate fixture strength
            home_strength = home_attack * away_defense
            away_strength = away_attack * home_defense
            
            logger.info(f"Fixture strength - Home: {home_strength:.2f}, Away: {away_strength:.2f}")
            
            # Calculate FDR
            home_fdr = self.convert_strength_to_fdr(home_strength)
            away_fdr = self.convert_strength_to_fdr(away_strength)
            
            logger.info(f"Multi-factor FDR - Home: {home_fdr:.2f}, Away: {away_fdr:.2f}")
            logger.info("-" * 50)



    def test_comprehensive_fdr_calculation2(self):
        """Test the comprehensive multi-factor FDR calculation method"""
        scoreline_fixtures = self.db.scorelineProbabilities.distinct("fixture_id")
        
        if not scoreline_fixtures:
            logger.error("No fixtures with scoreline probabilities found")
            return
        
        logger.info(f"Found {len(scoreline_fixtures)} fixtures with scoreline probabilities")
        
        for fixture_id in scoreline_fixtures:
            # Find fixture with proper handling for Goalserve structure
            fixture = self.db.fixtures.find_one({"id": fixture_id})
            
            if not fixture:
                logger.warning(f"Could not find fixture for ID {fixture_id}")
                continue
                
            # Extract team data
            home_name, away_name, home_team_id, away_team_id = self.extract_team_data(fixture)
            
            if not home_team_id or not away_team_id:
                logger.warning(f"Missing team IDs for fixture {fixture_id}")
                continue
            
            # Get league ID with support for category_id as fallback
            league_id = fixture.get("league_id") or fixture.get("league", {}).get("id") or fixture.get("category_id")
            
            if not league_id:
                logger.warning(f"No league ID found for fixture {fixture_id}")
                continue
            
            # Calculate xG from scoreline probabilities
            home_xg, away_xg = self.calculate_xg_from_scoreline(fixture_id)
            if home_xg is None:
                logger.warning(f"Could not calculate xG for {home_name} vs {away_name}")
                continue
            
            logger.info(f"Fixture: {home_name} vs {away_name}")
            logger.info(f"Pure xG values - Home: {home_xg:.2f}, Away: {away_xg:.2f}")
            
            # Calculate league averages
            home_league_avg, away_league_avg = self.get_league_average_xg(league_id)
            logger.info(f"League averages - Home: {home_league_avg:.2f}, Away: {away_league_avg:.2f}")
            
            # Get additional components
            home_outright = self.calculate_outright_component(home_team_id)
            away_outright = self.calculate_outright_component(away_team_id)
            home_form = self.calculate_form_component(home_team_id)
            away_form = self.calculate_form_component(away_team_id)
            historical = self.calculate_historical_component(home_team_id, away_team_id)
            home_availability = self.calculate_availability_component(home_team_id)
            away_availability = self.calculate_availability_component(away_team_id)
            
            logger.info(f"Outright - Home: {home_outright:.2f}, Away: {away_outright:.2f}")
            logger.info(f"Form - Home: {home_form:.2f}, Away: {away_form:.2f}")
            logger.info(f"Historical - Home: {historical[0]:.2f}, Away: {historical[1]:.2f}")
            logger.info(f"Availability - Home: {home_availability:.2f}, Away: {away_availability:.2f}")
            
            # Apply comprehensive weighting with your priority order
            home_xg_adjusted = (
                0.60 * home_xg +                                   # xG: 60%
                0.15 * (1.0 - home_outright) * home_league_avg +   # Outright: 15%
                0.10 * home_form * home_league_avg +               # Recent Form: 10%
                0.10 * (1.0 - historical[0]) * home_league_avg +   # Historical: 10% 
                0.05 * (1.0 - home_availability) * home_league_avg # Availability: 5%
            )
            
            away_xg_adjusted = (
                0.60 * away_xg +                                   # xG: 60%
                0.15 * (1.0 - away_outright) * away_league_avg +   # Outright: 15%
                0.10 * away_form * away_league_avg +               # Recent Form: 10%
                0.10 * (1.0 - historical[1]) * away_league_avg +   # Historical: 10%
                0.05 * (1.0 - away_availability) * away_league_avg # Availability: 5%
            )
            
            logger.info(f"Adjusted xG values - Home: {home_xg_adjusted:.2f}, Away: {away_xg_adjusted:.2f}")
            
            # Calculate strength coefficients using adjusted xG
            home_attack = home_xg_adjusted / home_league_avg
            away_attack = away_xg_adjusted / away_league_avg
            home_defense = away_xg_adjusted / away_league_avg
            away_defense = home_xg_adjusted / home_league_avg
            
            logger.info(f"Adjusted strengths - Home attack: {home_attack:.2f}, Home defense: {home_defense:.2f}")
            logger.info(f"Adjusted strengths - Away attack: {away_attack:.2f}, Away defense: {away_defense:.2f}")
            
            # Calculate fixture strength
            home_strength = home_attack * away_defense
            away_strength = away_attack * home_defense
            
            logger.info(f"Fixture strength - Home: {home_strength:.2f}, Away: {away_strength:.2f}")
            
            # Calculate FDR
            home_fdr = self.convert_strength_to_fdr(home_strength)
            away_fdr = self.convert_strength_to_fdr(away_strength)
            
            logger.info(f"Multi-factor FDR - Home: {home_fdr:.2f}, Away: {away_fdr:.2f}")
            logger.info("-" * 50)
    
    def extract_team_data(self, fixture):
        """Extract team names and IDs from fixture"""
        home_name = away_name = "Unknown"
        home_id = away_id = None
        
        if "participants" in fixture and len(fixture["participants"]) >= 2:
            home_name = fixture["participants"][0].get("name", "Home")
            away_name = fixture["participants"][1].get("name", "Away")
            home_id = fixture["participants"][0].get("id")
            away_id = fixture["participants"][1].get("id")
        elif "local_team" in fixture and "visitor_team" in fixture:
            home_name = fixture["local_team"].get("name", "Home")
            away_name = fixture["visitor_team"].get("name", "Away")
            home_id = fixture["local_team"].get("id")
            away_id = fixture["visitor_team"].get("id")
        
        return home_name, away_name, home_id, away_id



    
    def generate_match_summaries(self, days_ahead=14):
        """Generate simplified match summaries with FDR ratings"""
        start_date = datetime.now().strftime("%Y-%m-%d")
        end_date = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        
        fixtures = self.db.fixtures.find({
            "starting_at": {"$gte": start_date}
        })
        
        summaries = []
        for fixture in fixtures:
            try:
                # Calculate FDR if not already calculated
                if "fdr" not in fixture:
                    self.calculate_fixture_fdr(fixture)
                    # Fetch the updated fixture with FDR data
                    fixture = self.db.fixtures.find_one({"_id": fixture["_id"]})
                
                if "fdr" not in fixture:
                    continue
                    
                # Get team names
                home_team = ""
                away_team = ""
                if "participants" in fixture and len(fixture["participants"]) >= 2:
                    home_team = fixture["participants"][0].get("name", "Home")
                    away_team = fixture["participants"][1].get("name", "Away")
                
                # Get FDR categories AND numerical values
                home_fdr = fixture["fdr"]["overall"]["home"]["fdr"]
                away_fdr = fixture["fdr"]["overall"]["away"]["fdr"]
                home_category = fixture["fdr"]["overall"]["home"]["category"].lower()
                away_category = fixture["fdr"]["overall"]["away"]["category"].lower()
                
                # Format the summary including numerical values
                summary = f"{home_team} vs {away_team}\nfdr h {home_category} ({home_fdr:.1f}) - a {away_category} ({away_fdr:.1f})"
                summaries.append(summary)
                
                # Print and also return the summaries
                print(summary)
                print("---")
            except Exception as e:
                continue
        
        return summaries
    
    def test_epl_fixtures(self):
        """Test FDR components for all English Premier League fixtures"""
        print("\n=== TESTING FDR COMPONENTS FOR ALL PREMIER LEAGUE FIXTURES ===\n")
        
        # Find all upcoming Premier League fixtures
        fixtures = list(self.db.fixtures.find({
            "league.short_code": "UK PL",
            "starting_at": {"$gte": datetime.now().strftime("%Y-%m-%d")}
        }))
        
        if not fixtures:
            print("No Premier League fixtures found. Trying alternative lookup method...")
            # Try getting league ID first
            premier_league = self.db.leagues.find_one({"short_code": "UK PL"})
            if premier_league:
                league_id = premier_league["id"]
                fixtures = list(self.db.fixtures.find({
                    "$or": [
                        {"league.id": league_id},
                        {"league_id": league_id}
                    ],
                    "starting_at": {"$gte": datetime.now().strftime("%Y-%m-%d")}
                }))
        
        if not fixtures:
            print("No Premier League fixtures found in the database.")
            return
        
        print(f"Found {len(fixtures)} Premier League fixtures\n")
        
        # Component weight combinations to test
        weight_scenarios = [
            {"name": "Odds Heavy", "historical": 0.10, "form": 0.10, "outright": 0.10, "odds": 0.60, "availability": 0.10},
            {"name": "Historical Heavy", "historical": 0.40, "form": 0.20, "outright": 0.10, "odds": 0.25, "availability": 0.05},
            {"name": "Balanced", "historical": 0.25, "form": 0.15, "outright": 0.15, "odds": 0.35, "availability": 0.10},
            {"name": "Current", "historical": 0.20, "form": 0.20, "outright": 0.15, "odds": 0.40, "availability": 0.05}
        ]
        
        # Process each fixture
        for fixture in fixtures:
            self._test_individual_fixture(fixture, weight_scenarios)
        
        print("\n=== COMPLETED TESTING ALL PREMIER LEAGUE FIXTURES ===")

    def _test_individual_fixture(self, fixture, weight_scenarios):
        """Test different weight combinations for a single fixture"""
        # Extract team data
        home_team_id = None
        away_team_id = None
        home_team_name = "Home Team"
        away_team_name = "Away Team"
        
        if "participants" in fixture and len(fixture["participants"]) >= 2:
            home_team_id = fixture["participants"][0].get("id")
            away_team_id = fixture["participants"][1].get("id")
            home_team_name = fixture["participants"][0].get("name", "Home Team")
            away_team_name = fixture["participants"][1].get("name", "Away Team")
        
        # Get league information
        league_id = fixture.get("league_id")
        if not league_id and "league" in fixture:
            league_id = fixture["league"].get("id")
        
        league = self.db.leagues.find_one({"id": league_id})
        is_major = league.get("is_major", True)  # Premier League is major by default
        is_derby = self.check_if_derby(home_team_id, away_team_id)
        
        # Calculate each component
        historical = self.calculate_historical_component(home_team_id, away_team_id, is_derby)
        home_form = self.calculate_form_component(home_team_id)
        away_form = self.calculate_form_component(away_team_id)
        home_outright = self.calculate_outright_component(home_team_id)
        away_outright = self.calculate_outright_component(away_team_id)
        odds = self.calculate_odds_component(fixture.get("id"), home_team_id, away_team_id)
        home_availability = self.calculate_availability_component(home_team_id)
        away_availability = self.calculate_availability_component(away_team_id)
        
        # Store components
        home_components = {
            "historical": historical[0],
            "form": home_form,
            "outright": home_outright,
            "odds": odds[0],
            "availability": home_availability
        }
        
        away_components = {
            "historical": historical[1],
            "form": away_form,
            "outright": away_outright,
            "odds": odds[1],
            "availability": away_availability
        }
        
        # Print fixture information
        print(f"\n=== TESTING FDR COMPONENTS: {home_team_name} vs {away_team_name} ===")
        print(f"League: {league.get('name') if league else 'Premier League'} (Derby: {is_derby})")
        
        # Print raw component values
        print("\n=== RAW COMPONENT VALUES ===")
        print(f"{'Component':<15} {'Home':<10} {'Away':<10}")
        print("-" * 35)
        for comp in home_components:
            print(f"{comp.capitalize():<15} {home_components[comp]:<10.3f} {away_components[comp]:<10.3f}")
        
        # Test each component with 100% weight
        print("\n=== FDR WITH 100% WEIGHT ON SINGLE COMPONENT ===")
        print(f"{'Component':<15} {'Home FDR':<10} {'Away FDR':<10} {'Home Cat':<12} {'Away Cat':<12}")
        print("-" * 60)
        
        for comp in home_components:
            # Calculate with 100% weight on this component
            home_fdr_raw = home_components[comp]
            away_fdr_raw = away_components[comp]
            
            # Scale to 0-10
            home_fdr = self.scale_to_range(home_fdr_raw)
            away_fdr = self.scale_to_range(away_fdr_raw)
            
            # Get categories
            home_category = self.get_fdr_category(home_fdr)
            away_category = self.get_fdr_category(away_fdr)
            
            print(f"{comp.capitalize():<15} {home_fdr:<10.2f} {away_fdr:<10.2f} {home_category:<12} {away_category:<12}")
        
        # Test different weight scenarios
        print("\n=== WEIGHT SCENARIO COMPARISON ===")
        print(f"{'Scenario':<15} {'Home FDR':<10} {'Away FDR':<10} {'Home Cat':<12} {'Away Cat':<12}")
        print("-" * 60)
        
        for scenario in weight_scenarios:
            weights = scenario.copy()
            scenario_name = weights.pop("name")
            
            # Calculate weighted FDR
            home_fdr_raw = sum(home_components[comp] * weights[comp] for comp in home_components)
            away_fdr_raw = sum(away_components[comp] * weights[comp] for comp in away_components)
            
            # Scale to 0-10
            home_fdr = self.scale_to_range(home_fdr_raw)
            away_fdr = self.scale_to_range(away_fdr_raw)
            
            # Get categories
            home_category = self.get_fdr_category(home_fdr)
            away_category = self.get_fdr_category(away_fdr)
            
            print(f"{scenario_name:<15} {home_fdr:<10.2f} {away_fdr:<10.2f} {home_category:<12} {away_category:<12}")
        
        print("-" * 80)
    
    def calculate_xg_from_scoreline(self, fixture_id):
        """Calculate xG from scoreline probabilities with source priority"""
        # Try Goalserve data first (primary)
        probabilities = list(self.db.scorelineProbabilities.find({
            "fixture_id": fixture_id,
            "source": "goalserve"
        }))
        
        # Fall back to SportMonks if needed
        if not probabilities:
            probabilities = list(self.db.scorelineProbabilities.find({
                "fixture_id": fixture_id
            }))
        
        if not probabilities:
            # Try direct xG data as final fallback
            xg_data = self.db.fixtureExpectedGoals.find_one({"fixture_id": fixture_id})
            if xg_data:
                return xg_data.get("home_xg"), xg_data.get("away_xg")
            
            logger.warning(f"No xG data available for fixture {fixture_id}")
            return None, None
        
        home_xg = 0
        away_xg = 0
        
        for prob in probabilities:
            home_goals = prob.get("home_goals", 0)
            away_goals = prob.get("away_goals", 0)
            probability = prob.get("probability", 0) / 100
            home_xg += home_goals * probability
            away_xg += away_goals * probability
        
        return home_xg, away_xg




# Usage example
if __name__ == "__main__":
    # Create an instance of the FDR calculator
    calculator = FDRCalculator()
    # calculator.test_epl_fixtures()
    # Calculate FDR for all upcoming fixtures
    # calculator.calculate_all_fixtures(days_ahead=14)
    # from fdr_calculator import FDRDataCollector
    # sportmonks_token = os.getenv("SPORTMONKS_API_KEY")
    # goalserve_token = os.getenv("GOALSERVE_API_KEY", "0f6230689b674453eee508dd50f5b2ca")
    
    # MongoDB URI is directly configured in the class
    # collector = FDRDataCollector(sportmonks_token, goalserve_token)
    # colector = FDRDataCollector()
    # colector.create_missing_fixture_mappings()

    # Calculate FDR for each fixture
    # calculator.calculate_fixture_fdr_new()
    calculator.test_comprehensive_fdr_calculation2()
    