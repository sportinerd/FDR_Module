# fdr_data_collector.py

import requests
import logging
from datetime import datetime, timedelta
import json
import os
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
from pymongo.server_api import ServerApi
from fuzzywuzzy import fuzz, process
import re
from datetime import datetime

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("fdr_collection.log")]
)
logger = logging.getLogger(__name__)

class FDRDataCollector:
    """Collect data from Sportmonks and GoalServe for FDR calculations"""
    
    def __init__(self, sportmonks_token, goalserve_token, mongo_uri=None, data_dir="data"):
        self.sportmonks_token = sportmonks_token
        self.goalserve_token = goalserve_token
        self.data_dir = data_dir
        
        # MongoDB connection
        if mongo_uri is None:
            mongo_uri = "mongodb+srv://naymul504:soupnaymul09@pf365.2pguj.mongodb.net/?retryWrites=true&w=majority&appName=pf365"
        
        # Initialize MongoDB client and database
        self.client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        self.db = self.client['Analytics']
        
        # Create data directory if it doesn't exist (for fallback)
        os.makedirs(data_dir, exist_ok=True)
        
        # API endpoints
        self.sportmonks_base_url = "https://api.sportmonks.com/v3/football"
        self.goalserve_base_url = "http://www.goalserve.com/getfeed"
        self.goalserve_outright_url = f"http://oddsfeed.goalserve.com/api/v1/odds/pre-game/outrights/4?k={goalserve_token}"
    
    def sportmonks_request(self, endpoint, params=None):
        """Make a request to Sportmonks API"""
        url = f"{self.sportmonks_base_url}{endpoint}"
        headers = {"Authorization": self.sportmonks_token}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error requesting {url}: {str(e)}")
            return None
    
    def goalserve_request(self, endpoint, params=None):
        """Make a request to GoalServe API"""
        url = f"{self.goalserve_base_url}/{self.goalserve_token}/{endpoint}"
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.text  # GoalServe typically returns XML
        except Exception as e:
            logger.error(f"Error requesting {url}: {str(e)}")
            return None
    
    def save_to_mongodb(self, collection_name, data, identifier_field=None, save_local_backup=False):
        """
        Save data to MongoDB collection with optional upsert for existing records
        
        Args:
            collection_name: Name of the MongoDB collection
            data: Data to save (list or single document)
            identifier_field: Field to use for identifying existing records (for upsert)
            save_local_backup: Whether to save a local JSON backup file
        """
        collection = self.db[collection_name]
        timestamp = datetime.now()

        # Process differently depending on whether data is a list or single document
        if isinstance(data, list):
            if len(data) == 0:
                logger.info(f"No data to save to {collection_name}")
                return 0
            
            # Add timestamp to all records
            for item in data:
                item['last_updated'] = timestamp
            
            # Use bulk operations for better performance
            if identifier_field and identifier_field in data[0]:
                # Use bulk upsert if we have an identifier field
                operations = []
                for item in data:
                    query = {identifier_field: item[identifier_field]}
                    operations.append(
                        UpdateOne(query, {'$set': item}, upsert=True)
                    )
                
                try:
                    result = collection.bulk_write(operations)
                    logger.info(f"MongoDB: Upserted {result.upserted_count}, modified {result.modified_count} records in {collection_name}")
                except BulkWriteError as bwe:
                    logger.error(f"Bulk write error on {collection_name}: {bwe.details}")
                    return 0
            else:
                # Without identifier, do a simple insert_many
                try:
                    result = collection.insert_many(data)
                    logger.info(f"MongoDB: Inserted {len(result.inserted_ids)} records into {collection_name}")
                except Exception as e:
                    logger.error(f"Error inserting into {collection_name}: {str(e)}")
                    return 0
        else:
            # Single document
            data['last_updated'] = timestamp
            
            if identifier_field and identifier_field in data:
                query = {identifier_field: data[identifier_field]}
                result = collection.update_one(query, {'$set': data}, upsert=True)
                logger.info(f"MongoDB: {'Inserted' if result.upserted_id else 'Updated'} document in {collection_name}")
            else:
                result = collection.insert_one(data)
                logger.info(f"MongoDB: Inserted document into {collection_name} with ID {result.inserted_id}")
        
        # Optionally save local backup
        if save_local_backup:
            filepath = os.path.join(self.data_dir, f"{collection_name}.json")
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved local backup to {filepath}")
        
        return len(data) if isinstance(data, list) else 1
    
    def get_leagues(self):
        """Get all leagues with country information"""
        logger.info("Fetching leagues data")
        
        response = self.sportmonks_request("/leagues", {
            "include": "country",
            "per_page": 150
        })
        
        if not response or "data" not in response:
            logger.error("Failed to fetch leagues")
            return []
        
        leagues = response["data"]
        
        # Save to MongoDB
        self.save_to_mongodb("leagues", leagues, identifier_field="id")
        
        logger.info(f"Saved {len(leagues)} leagues")
        return leagues
    
    def get_teams_by_league(self, league_id):
        """Get all teams for a league"""
        logger.info(f"Fetching teams for league ID {league_id}")
        
        response = self.sportmonks_request("/teams", {
            "filters": f"leagueIds:{league_id}",
            "per_page": 100
        })
        
        if not response or "data" not in response:
            logger.error(f"Failed to fetch teams for league {league_id}")
            return []
        
        teams = response["data"]
        
        # Add league_id to each team document
        for team in teams:
            team['league_id'] = league_id
        
        # Save to MongoDB
        self.save_to_mongodb("teams", teams, identifier_field="id")
        
        logger.info(f"Saved {len(teams)} teams for league {league_id}")
        return teams
    
    def get_historical_matches(self, league_id, seasons=10):
        """Get historical matches for the last N seasons of a league"""
        logger.info(f"Fetching historical matches for league {league_id}, last {seasons} seasons")
        
        # First get all seasons
        seasons_response = self.sportmonks_request("/seasons", {
            "filters": f"leagueIds:{league_id}",
            "per_page": seasons
        })
        
        if not seasons_response or "data" not in seasons_response:
            logger.error(f"Failed to fetch seasons for league {league_id}")
            return []
        
        all_matches = []
        
        for season in seasons_response["data"]:
            season_id = season["id"]
            logger.info(f"Fetching matches for season {season_id}")
            
            # Get season data with fixtures included
            season_response = self.sportmonks_request(f"/seasons/{season_id}", {
                "include": "fixtures.participants",
                "per_page": 500
            })
            
            if not season_response or "data" not in season_response or "fixtures" not in season_response["data"]:
                logger.warning(f"Failed to fetch fixtures for season {season_id}")
                continue
            
            # Extract fixtures from the response
            fixtures = season_response["data"]["fixtures"]
            
            # Only include completed matches
            matches = [m for m in fixtures if m.get("state_id") == 5]
            
            # Add league_id to each match
            for match in matches:
                match['league_id'] = league_id
                match['season_id'] = season_id
            
            all_matches.extend(matches)
            logger.info(f"Found {len(matches)} completed matches for season {season_id}")
        
        # Save to MongoDB
        self.save_to_mongodb("historicalMatches", all_matches, identifier_field="id")
        
        logger.info(f"Saved {len(all_matches)} historical matches for league {league_id}")
        return all_matches
    
    def get_upcoming_fixtures(self, days_ahead=14):
        """Get upcoming fixtures for next N days with odds"""
        today = datetime.now().strftime("%Y-%m-%d")
        future = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        
        logger.info(f"Fetching upcoming fixtures from {today} to {future}")
        
        response = self.sportmonks_request(f"/fixtures/between/{today}/{future}", {
            "include": "participants;odds;league",
            "per_page": 200
        })
        
        if not response or "data" not in response:
            logger.error("Failed to fetch upcoming fixtures")
            return []
        
        fixtures = response["data"]
        
        # Save to MongoDB
        self.save_to_mongodb("fixtures", fixtures, identifier_field="id")
        
        logger.info(f"Saved {len(fixtures)} upcoming fixtures")
        return fixtures
    
    def get_player_availability(self):
        """Get player injuries and suspensions using the teams endpoint with sidelined data"""
        logger.info("Fetching player availability data using teams sidelined data")
        
        # Get teams with sidelined players
        teams_response = self.sportmonks_request("/teams", {
            "include": "sidelined",
            "per_page": 100
        })
        
        if not teams_response or "data" not in teams_response:
            logger.error("Failed to fetch teams with sidelined data")
            return {"injuries": [], "suspensions": []}
        
        teams = teams_response["data"]
        
        # Process sidelined data
        player_availability = []
        
        for team in teams:
            team_id = team["id"]
            team_name = team["name"]
            
            # Process sidelined players if available
            if "sidelined" in team and team["sidelined"]:
                for sidelined in team["sidelined"]:
                    player_id = sidelined["player_id"]
                    category = sidelined["category"]
                    start_date = sidelined["start_date"]
                    end_date = sidelined["end_date"]
                    
                    # Create player availability record
                    availability_record = {
                        "player_id": player_id,
                        "team_id": team_id,
                        "team_name": team_name,
                        "category": category,
                        "start_date": start_date,
                        "end_date": end_date,
                        "games_missed": sidelined["games_missed"],
                        "completed": sidelined["completed"]
                    }
                    
                    player_availability.append(availability_record)
        
        # Save to MongoDB
        self.save_to_mongodb("playerAvailability", player_availability, identifier_field="player_id")
        
        # Count injuries and suspensions for logging
        injuries = sum(1 for item in player_availability if item.get("category") == "injury")
        suspensions = sum(1 for item in player_availability if item.get("category") != "injury")
        
        logger.info(f"Saved player availability data: {injuries} injuries, {suspensions} suspensions")
        return player_availability
    
    def get_predictions(self, league_id=None, fixture_id=None):
        """Get match predictions from Sportmonks"""
        logger.info("Fetching predictions data")
        
        if fixture_id:
            # For specific fixture
            endpoint = f"/predictions/probabilities/fixtures/{fixture_id}"
            params = {}
        else:
            # For all probabilities
            endpoint = "/predictions/probabilities"
            params = {"per_page": 200}
            if league_id:
                params["filters"] = f"leagueIds:{league_id}"
        
        response = self.sportmonks_request(endpoint, params)
        
        if not response or "data" not in response:
            logger.error(f"Failed to fetch predictions using {endpoint}")
            return []
        
        predictions = response["data"]
        
        # Save to MongoDB
        self.save_to_mongodb("predictions", predictions, identifier_field="id")
        
        logger.info(f"Saved {len(predictions)} predictions")
        return predictions
    
    def get_rivals(self):
        """Get team rivalries (for derby identification)"""
        logger.info("Fetching rivals data")
        
        response = self.sportmonks_request("/rivals", {"per_page": 200})
        
        if not response or "data" not in response:
            logger.error("Failed to fetch rivals")
            return []
        
        rivals = response["data"]
        
        # Save to MongoDB
        self.save_to_mongodb("rivals", rivals, identifier_field="id")
        
        logger.info(f"Saved {len(rivals)} rivalry records")
        return rivals
    
    def get_goalserve_outright_odds(self):
        """Get outright odds from GoalServe"""
        logger.info("Fetching outright odds from GoalServe")
        
        try:
            response = requests.get(self.goalserve_outright_url)
            response.raise_for_status()
            xml_data = response.text
            
            # Parse XML to structured data
            import xml.etree.ElementTree as ET
            root = ET.fromstring(xml_data)
            
            outright_odds = []
            
            for category in root.findall('category'):
                category_name = category.get('name')
                
                for outrights in category.findall('outrights'):
                    for market in outrights.findall('market'):
                        market_name = market.get('name')
                        market_id = market.get('id')
                        
                        for sel in market.findall('sel'):
                            team_name = sel.get('name')
                            team_id = sel.get('id')
                            
                            odds_list = []
                            
                            for bookmaker in sel.findall('bookmaker'):
                                bookmaker_name = bookmaker.get('name')
                                bookmaker_id = bookmaker.get('id')
                                odd_value = bookmaker.find('odd').get('value')
                                
                                odds_list.append({
                                    'bookmaker': bookmaker_name,
                                    'bookmaker_id': bookmaker_id,
                                    'odd': float(odd_value)
                                })
                            
                            outright_odds.append({
                                'category': category_name,
                                'market': market_name,
                                'market_id': market_id,
                                'team': team_name,
                                'team_id': team_id,
                                'odds': odds_list
                            })
            
            # Save to MongoDB
            self.save_to_mongodb("outrightOdds", outright_odds)
            
            logger.info(f"Saved outright odds data: {len(outright_odds)} records")
            return outright_odds
            
        except Exception as e:
            logger.error(f"Error fetching outright odds: {str(e)}")
            return None
    
    def get_goalserve_fixture_odds(self):
        """Get fixture odds from GoalServe with improved Sportmonks ID mapping"""
        logger.info("Fetching fixture odds from GoalServe")
        
        endpoint = "getodds/soccer"
        params = {"cat": "soccer_10"}
        
        xml_response = self.goalserve_request(endpoint, params)
        if not xml_response:
            logger.error("Failed to fetch fixture odds")
            return None
        
        import xml.etree.ElementTree as ET
        
        try:
            root = ET.fromstring(xml_response)
            fixture_odds = []
            mapped_count = 0
            
            # Create a cache of upcoming fixtures for faster lookup
            upcoming_fixtures = list(self.db.fixtures.find({
                "starting_at": {"$gte": datetime.now().strftime("%Y-%m-%d")}
            }))
            logger.info(f"Loaded {len(upcoming_fixtures)} upcoming fixtures for mapping")
            
            for category in root.findall('category'):
                category_name = category.get('name')
                category_id = category.get('id')
                
                matches_elem = category.find('matches')
                if matches_elem is None:
                    continue
                    
                for match in matches_elem.findall('match'):
                    match_id = match.get('id')
                    match_date = match.get('date')
                    match_time = match.get('time')
                    match_status = match.get('status')
                    
                    local_team = match.find('localteam')
                    visitor_team = match.find('visitorteam')
                    
                    if local_team is None or visitor_team is None:
                        continue
                        
                    local_team_name = local_team.get('name')
                    visitor_team_name = visitor_team.get('name')
                    
                    # Check for existing mapping first
                    existing_mapping = self.db.fixture_id_mapping.find_one({"goalserve_id": match_id})
                    sportmonks_id = None
                    
                    if existing_mapping:
                        sportmonks_id = existing_mapping["sportmonks_id"]
                    else:
                        # Find matching fixture
                        matching_fixture = self.find_matching_fixture(
                            local_team_name, 
                            visitor_team_name,
                            match_date,
                            category_name,
                            match_time
                        )
                        
                        if matching_fixture:
                            sportmonks_id = matching_fixture["id"]
                            # Create mapping for future use
                            self.update_fixture_mapping(match_id, sportmonks_id)
                            mapped_count += 1
                    
                    match_odds = {
                        'match_id': match_id,
                        'category_name': category_name,
                        'category_id': category_id,
                        'match_date': match_date,
                        'match_time': match_time,
                        'match_status': match_status,
                        'local_team': {
                            'name': local_team_name,
                            'id': local_team.get('id')
                        },
                        'visitor_team': {
                            'name': visitor_team_name,
                            'id': visitor_team.get('id')
                        },
                        'odds': []
                    }
                    
                    if sportmonks_id:
                        match_odds["sportmonks_id"] = sportmonks_id
                    else:
                        logger.warning(f"No Sportmonks fixture found for {local_team_name} vs {visitor_team_name} on {match_date}")
                    
                    # Process odds data
                    odds_elem = match.find('odds')
                    if odds_elem is not None:
                        for odds_type in odds_elem.findall('type'):
                            type_value = odds_type.get('value')
                            type_id = odds_type.get('id')
                            
                            bookmakers_data = []
                            for bookmaker in odds_type.findall('bookmaker'):
                                bookmaker_name = bookmaker.get('name')
                                bookmaker_id = bookmaker.get('id')
                                
                                odds = {
                                    'bookmaker_id': bookmaker_id,
                                    'bookmaker_name': bookmaker_name,
                                    'home_odd': None,
                                    'draw_odd': None,
                                    'away_odd': None
                                }
                                
                                for odd in bookmaker.findall('odd'):
                                    odd_name = odd.get('name')
                                    odd_value = odd.get('value')
                                    
                                    if odd_name == "Home":
                                        odds['home_odd'] = float(odd_value)
                                    elif odd_name == "Draw":
                                        odds['draw_odd'] = float(odd_value)
                                    elif odd_name == "Away":
                                        odds['away_odd'] = float(odd_value)
                                        
                                if all([odds['home_odd'], odds['draw_odd'], odds['away_odd']]):
                                    bookmakers_data.append(odds)
                                    
                            if bookmakers_data:
                                match_odds['odds'].append({
                                    'type_id': type_id,
                                    'type_value': type_value,
                                    'bookmakers': bookmakers_data
                                })
                    
                    if match_odds['odds']:
                        fixture_odds.append(match_odds)
            
            self.save_to_mongodb("fixtureOdds", fixture_odds, identifier_field="match_id")
            logger.info(f"Saved {len(fixture_odds)} fixture odds records with {mapped_count} new Sportmonks mappings")
            
            return fixture_odds
            
        except Exception as e:
            logger.error(f"Error parsing fixture odds XML: {str(e)}")
            return None


    
    def get_sportmonks_prematch_odds(self, fixture_id=None):
        """Get pre-match odds from Sportmonks API"""
        logger.info("Fetching pre-match odds from Sportmonks")
        
        odds_base_url = "https://api.sportmonks.com/v3/football/odds"
        
        if fixture_id:
            url = f"{odds_base_url}/pre-match/fixtures/{fixture_id}"
            params = {"include": "market;bookmaker"}
        else:
            url = f"{odds_base_url}/pre-match"
            params = {"include": "market;bookmaker;fixture", "per_page": 100}
        
        headers = {"Authorization": self.sportmonks_token}
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "data" not in data:
                logger.error(f"Failed to fetch pre-match odds from Sportmonks")
                return []
            
            odds_data = data["data"]
            
            # Save to MongoDB
            self.save_to_mongodb("sportmonksPrematchOdds", odds_data, identifier_field="id")
            
            logger.info(f"Saved {len(odds_data)} Sportmonks pre-match odds records")
            return odds_data
            
        except Exception as e:
            logger.error(f"Error fetching pre-match odds from Sportmonks: {str(e)}")
            return []
    
    def get_sportmonks_inplay_odds(self, fixture_id=None):
        """Get in-play odds from Sportmonks API"""
        logger.info("Fetching in-play odds from Sportmonks")
        
        odds_base_url = "https://api.sportmonks.com/v3/football/odds"
        
        if fixture_id:
            url = f"{odds_base_url}/inplay/fixtures/{fixture_id}"
            params = {"include": "market;bookmaker"}
        else:
            url = f"{odds_base_url}/inplay"
            params = {"include": "market;bookmaker;fixture", "per_page": 100}
        
        headers = {"Authorization": self.sportmonks_token}
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if "data" not in data:
                logger.error(f"Failed to fetch in-play odds from Sportmonks")
                return []
            
            odds_data = data["data"]
            
            # Save to MongoDB
            self.save_to_mongodb("sportmonksInplayOdds", odds_data, identifier_field="id")
            
            logger.info(f"Saved {len(odds_data)} Sportmonks in-play odds records")
            return odds_data
            
        except Exception as e:
            logger.error(f"Error fetching in-play odds: {str(e)}")
            return []
    
    def normalize_team_name(self, name):
        """Normalize team names for better matching"""
        if not name:
            return ""
        
        # Convert to lowercase
        name = name.lower()
        
        # Remove common suffixes
        suffixes = [" fc", " cf", " united", " utd", " city", " athletic", " academy", 
                    " u21", " u23", " u19", " reserves", " ladies", " women"]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        
        # Remove special characters and extra spaces
        name = re.sub(r'[^\w\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name

    def normalize_date(self, date_str):
        """Convert various date formats to YYYY-MM-DD"""
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

    def find_matching_fixture(self, local_team, visitor_team, match_date, category_name=None, match_time=None):
        """Find matching fixture in Sportmonks data using multiple criteria"""
        # Normalize team names
        norm_local = self.normalize_team_name(local_team)
        norm_visitor = self.normalize_team_name(visitor_team)
        
        # Normalize date
        normalized_date = self.normalize_date(match_date)
        if not normalized_date:
            logger.warning(f"Could not normalize date: {match_date}")
            return None
        
        # Build query - start with date match
        query = {"starting_at": {"$regex": f"^{normalized_date}"}}
        
        # Add league filter if available
        if category_name:
            # Extract main league name from category
            league_name = category_name.split(":")[-1].strip() if ":" in category_name else category_name
            query["$or"] = [
                {"league.name": {"$regex": league_name, "$options": "i"}},
                {"league.short_code": {"$regex": league_name, "$options": "i"}}
            ]
        
        # Find potential matches
        potential_matches = list(self.db.fixtures.find(query))
        
        if not potential_matches:
            return None
        
        # Score each potential match
        best_score = 0
        best_match = None
        
        for fixture in potential_matches:
            # Skip fixtures without participants
            if "participants" not in fixture or len(fixture["participants"]) < 2:
                continue
            
            # Get team names from fixture
            home_name = self.normalize_team_name(fixture["participants"][0].get("name", ""))
            away_name = self.normalize_team_name(fixture["participants"][1].get("name", ""))
            
            # Calculate name similarity scores
            home_local_score = fuzz.ratio(home_name, norm_local)
            away_visitor_score = fuzz.ratio(away_name, norm_visitor)
            
            # Also check reversed (in case teams are swapped)
            home_visitor_score = fuzz.ratio(home_name, norm_visitor)
            away_local_score = fuzz.ratio(away_name, norm_local)
            
            # Get best team name match configuration
            direct_match = home_local_score + away_visitor_score
            reverse_match = home_visitor_score + away_local_score
            
            # Use the better of the two matching patterns
            match_score = max(direct_match, reverse_match)
            
            # Time matching bonus (if available)
            time_bonus = 0
            if match_time and "starting_at" in fixture:
                fixture_time = fixture["starting_at"].split(" ")[1][:5] if " " in fixture["starting_at"] else ""
                if fixture_time and match_time:
                    # Convert match_time to the same format if needed
                    parsed_match_time = match_time.replace(".", ":")[:5]
                    if parsed_match_time == fixture_time:
                        time_bonus = 20
            
            total_score = match_score + time_bonus
            
            # Update best match if this one is better
            if total_score > best_score and total_score >= 160:  # Threshold for good match
                best_score = total_score
                best_match = fixture
                
        return best_match

    def update_fixture_mapping(self, goalserve_id, sportmonks_id, confidence_score=100):
        """Store mapping between GoalServe and Sportmonks fixture IDs"""
        mapping = {
            "goalserve_id": goalserve_id,
            "sportmonks_id": sportmonks_id,
            "confidence": confidence_score,
            "last_updated": datetime.now()
        }
        
        # Save to mapping collection
        self.db.fixture_id_mapping.update_one(
            {"goalserve_id": goalserve_id},
            {"$set": mapping},
            upsert=True
        )
        
        logger.info(f"Created mapping: GoalServe ID {goalserve_id} → Sportmonks ID {sportmonks_id}")

    def get_sportmonks_scoreline_probabilities(self):
        """Get scoreline probabilities from Sportmonks Predictions API"""
        logger.info("Fetching scoreline probabilities from Sportmonks")
        
        endpoint = "/predictions/probabilities"
        params = {
            "include": "type",
            "per_page": 150
        }
        
        response = self.sportmonks_request(endpoint, params)
        
        if not response or "data" not in response:
            logger.error("Failed to fetch scoreline probabilities")
            return []
        
        probabilities = response["data"]
        scoreline_probs = []
        
        for prob in probabilities:
            if prob.get("type_id") == 240:  # Correct Score Probability
                fixture_id = prob.get("fixture_id")
                prediction = prob.get("predictions", {})
                
                # Check if 'scores' exists in prediction
                if "scores" in prediction:
                    # Process each scoreline in the scores object
                    for scoreline, probability in prediction["scores"].items():
                        if "-" in scoreline:
                            home_goals, away_goals = scoreline.split("-")
                            try:
                                scoreline_probs.append({
                                    "fixture_id": fixture_id,
                                    "home_goals": int(home_goals),
                                    "away_goals": int(away_goals),
                                    "probability": float(probability)
                                })
                            except (ValueError, TypeError):
                                logger.warning(f"Invalid scoreline format: {scoreline} with probability {probability}")
        
        # Save to MongoDB
        if scoreline_probs:
            self.save_to_mongodb("scorelineProbabilities", scoreline_probs)
            logger.info(f"Saved {len(scoreline_probs)} scoreline probabilities")
        else:
            logger.warning("No valid scoreline probabilities found to save")
        
        return scoreline_probs
    
    def _format_goalserve_datetime(self, date_str, time_str):
        """Format Goalserve date and time into standard format"""
        if not date_str or not time_str:
            return None
        
        try:
            # Handle "May 18" format
            if re.match(r"^\w+ \d+$", date_str):
                current_year = datetime.now().year
                date_obj = datetime.strptime(f"{date_str} {current_year}", "%b %d %Y")
                if (datetime.now() - date_obj).days > 7:  # If date is past, assume next year
                    date_obj = datetime.strptime(f"{date_str} {current_year+1}", "%b %d %Y")
                date_str = date_obj.strftime("%Y-%m-%d")
            
            # Combine date and time
            return f"{date_str} {time_str}"
        except ValueError:
            return None

    def _normalize_scoreline_probabilities(self, probabilities):
        """Normalize probabilities per fixture to account for bookmaker margin"""
        fixtures = {}
        
        # Group by fixture
        for prob in probabilities:
            fixture_id = prob["fixture_id"]
            if fixture_id not in fixtures:
                fixtures[fixture_id] = []
            fixtures[fixture_id].append(prob)
        
        # Normalize each fixture's probabilities
        for fixture_id, fixture_probs in fixtures.items():
            total_prob = sum(p["probability"] for p in fixture_probs)
            for prob in fixture_probs:
                prob["probability"] = (prob["probability"] / total_prob) * 100

    def get_goalserve_fixtures(self, days_ahead=14):
        """Get upcoming fixtures from Goalserve using direct API call"""
        today = datetime.now().strftime("%d.%m.%Y")
        future = (datetime.now() + timedelta(days=days_ahead)).strftime("%d.%m.%Y")
        
        logger.info(f"Fetching upcoming fixtures from Goalserve ({today} to {future})")
        
        # Construct the full URL directly
        url = f"https://www.goalserve.com/getfeed/{self.goalserve_token}/getscores/soccer?date_start={today}&date_end={future}&json=true"
        
        try:
            # Make the request directly using requests library
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            
            # Parse JSON response
            data = response.json()
            fixtures = []
            
            # Extract fixtures from categories
            if "scores" in data and "categories" in data["scores"]:
                for category in data["scores"]["categories"]:
                    if "matches" in category:
                        for match in category["matches"]:
                            fixture = {
                                "id": match.get("id"),
                                "starting_at": self._format_goalserve_datetime(match.get("date"), match.get("time")),
                                "category_name": category.get("name"),
                                "category_id": category.get("id"),
                                "local_team": match.get("localteam", {}),
                                "visitor_team": match.get("visitorteam", {})
                            }
                            fixtures.append(fixture)
            
            self.save_to_mongodb("fixtures", fixtures, identifier_field="id")
            logger.info(f"Saved {len(fixtures)} fixtures from Goalserve")
            return fixtures
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error processing Goalserve fixtures: {str(e)}")
            return []


    def get_goalserve_scoreline_probabilities(self):
        """Extract scoreline probabilities from Goalserve correct score odds"""
        logger.info("Fetching scoreline probabilities from Goalserve")
        
        today = datetime.now().strftime("%d.%m.%Y")
        future = (datetime.now() + timedelta(days=14)).strftime("%d.%m.%Y")
        
        url = f"https://www.goalserve.com/getfeed/0f6230689b674453eee508dd50f5b2ca/getodds/soccer?cat=soccer_10&league=1204&date_start={today}&date_end={future}&market=81,22845,23132&json=true"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            scoreline_probs = []
            
            # Use response.json() instead of json.loads(response)
            data = response.json()
            
            if "scores" in data and "categories" in data["scores"]:
                for category in data["scores"]["categories"]:
                    for match in category.get("matches", []):
                        fixture_id = match.get("id")
                        
                        for odds_type in match.get("odds", []):
                            if odds_type.get("value") == "Correct Score":
                                for bookmaker in odds_type.get("bookmakers", []):
                                    for odd in bookmaker.get("odds", []):
                                        scoreline = odd.get("name")
                                        value = odd.get("value")
                                        
                                        if scoreline and value and odd.get("stop") != "True":
                                            try:
                                                home_goals, away_goals = scoreline.split(":")
                                                odd_value = float(value)
                                                probability = (1 / odd_value) * 100
                                                
                                                scoreline_probs.append({
                                                    "fixture_id": fixture_id,
                                                    "home_goals": int(home_goals),
                                                    "away_goals": int(away_goals),
                                                    "probability": probability,
                                                    "source": "goalserve"
                                                })
                                            except (ValueError, TypeError):
                                                continue
            
            # Normalize probabilities per fixture
            self._normalize_scoreline_probabilities(scoreline_probs)
            
            # Save to MongoDB
            self.save_to_mongodb("scorelineProbabilities", scoreline_probs)
            logger.info(f"Saved {len(scoreline_probs)} scoreline probabilities")
            
            return scoreline_probs
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error processing scoreline odds: {str(e)}")
            return []


    def create_missing_fixture_mappings(self):
        """Create fixture mappings between Goalserve and SportMonks fixtures"""
        scoreline_fixtures = self.db.scorelineProbabilities.distinct("fixture_id")
        
        logger.info(f"Checking mappings for {len(scoreline_fixtures)} fixtures with scoreline probabilities")
        created_count = 0
        
        for goalserve_id in scoreline_fixtures:
            # Check if mapping already exists
            existing = self.db.fixture_id_mapping.find_one({"goalserve_id": goalserve_id})
            if existing:
                continue
                
            # Get fixture details from your Goalserve fixtures collection
            goalserve_fixture = self.db.fixtures.find_one({"id": goalserve_id})
            
            if not goalserve_fixture:
                logger.warning(f"No fixture found in fixtures collection for ID {goalserve_id}")
                continue
                
            try:
                # Extract team names and date from Goalserve fixture
                if "participants" in goalserve_fixture and len(goalserve_fixture["participants"]) >= 2:
                    home_team = goalserve_fixture["participants"][0]["name"]
                    away_team = goalserve_fixture["participants"][1]["name"]
                else:
                    home_team = goalserve_fixture.get("local_team", {}).get("name")
                    away_team = goalserve_fixture.get("visitor_team", {}).get("name")
                
                match_date = goalserve_fixture["starting_at"].split(" ")[0]
                
                # Search SportMonks for this match
                search_params = {
                    "filters": f"date:{match_date}",
                    "include": "participants"
                }
                sportmonks_fixtures = self.sportmonks_request("/fixtures", search_params)
                
                if sportmonks_fixtures and "data" in sportmonks_fixtures:
                    # Find best matching fixture in SportMonks data
                    for sportmonks_fixture in sportmonks_fixtures["data"]:
                        if "participants" not in sportmonks_fixture or len(sportmonks_fixture["participants"]) < 2:
                            continue
                            
                        sm_home = sportmonks_fixture["participants"][0]["name"]
                        sm_away = sportmonks_fixture["participants"][1]["name"]
                        
                        # Compare team names with some fuzzy matching
                        if (fuzz.ratio(self.normalize_team_name(sm_home), self.normalize_team_name(home_team)) > 80 and
                            fuzz.ratio(self.normalize_team_name(sm_away), self.normalize_team_name(away_team)) > 80):
                            # We found a match!
                            sportmonks_id = sportmonks_fixture["id"]
                            self.update_fixture_mapping(goalserve_id, sportmonks_id)
                            logger.info(f"Created mapping for {home_team} vs {away_team}")
                            created_count += 1
                            break
                
            except (KeyError, IndexError) as e:
                logger.error(f"Error processing fixture {goalserve_id}: {str(e)}")
        
        logger.info(f"Created {created_count} new fixture mappings")
        return created_count

    def get_fixtures_from_both_sources(self, days_ahead=14):
        """Get fixtures from both SportMonks and Goalserve and create mappings"""
        # First get SportMonks fixtures
        sportmonks_fixtures = self.get_upcoming_fixtures(days_ahead)
        
        # Then get Goalserve fixtures
        goalserve_fixtures = self.get_goalserve_fixtures(days_ahead)
        
        # Create mappings between them
        self.create_bidirectional_mappings(sportmonks_fixtures, goalserve_fixtures)
        
        return sportmonks_fixtures

    def create_bidirectional_mappings(self, sportmonks_fixtures, goalserve_fixtures):
        """Create mappings between SportMonks and Goalserve fixtures"""
        sportmonks_dict = {}
        for fixture in sportmonks_fixtures:
            if "participants" in fixture and len(fixture["participants"]) >= 2:
                home = self.normalize_team_name(fixture["participants"][0]["name"])
                away = self.normalize_team_name(fixture["participants"][1]["name"])
                date = fixture["starting_at"].split(" ")[0]
                key = f"{home}|{away}|{date}"
                sportmonks_dict[key] = fixture["id"]
        
        mapped_count = 0
        for fixture in goalserve_fixtures:
            home = self.normalize_team_name(fixture.get("local_team", {}).get("name", ""))
            away = self.normalize_team_name(fixture.get("visitor_team", {}).get("name", ""))
            date_str = fixture.get("starting_at", "").split(" ")[0]
            
            key = f"{home}|{away}|{date_str}"
            if key in sportmonks_dict:
                sportmonks_id = sportmonks_dict[key]
                goalserve_id = fixture["id"]
                
                # Create the mapping
                self.update_fixture_mapping(goalserve_id, sportmonks_id)
                mapped_count += 1
        
        logger.info(f"Created {mapped_count} fixture mappings")
        return mapped_count

    # Add this to collect_all_fdr_data after collecting fixtures
    def verify_fixture_mappings(self):
        """Verify all scoreline fixtures have corresponding entries"""
        scoreline_fixtures = self.db.scorelineProbabilities.distinct("fixture_id")
        logger.info(f"Verifying mappings for {len(scoreline_fixtures)} fixtures with scoreline data")
        
        missing = 0
        for fixture_id in scoreline_fixtures:
            # Try direct lookup
            fixture = self.db.fixtures.find_one({"id": fixture_id})
            if fixture:
                continue
                
            # Try mapping lookup
            mapping = self.db.fixture_id_mapping.find_one({"goalserve_id": fixture_id})
            if mapping and mapping.get("sportmonks_id"):
                fixture = self.db.fixtures.find_one({"id": mapping["sportmonks_id"]})
                if fixture:
                    continue
                    
            missing += 1
            logger.warning(f"No fixture found for scoreline fixture ID: {fixture_id}")
        
        if missing:
            logger.warning(f"Missing {missing}/{len(scoreline_fixtures)} fixtures with scoreline data")
        else:
            logger.info("All fixtures with scoreline data have corresponding fixtures")



    def collect_all_fdr_data(self, major_league_ids=None):
        """Collect all data needed for FDR calculation"""
        logger.info("Starting complete FDR data collection")
        
        # 1. Get all leagues
        leagues = self.get_leagues()
        
        # Determine which leagues are major based on data availability
        if not major_league_ids:
            major_league_ids = []
            for league in leagues:
                # Check data richness criteria (country, teams, etc)
                country = league.get("country", {}).get("name", "")
                major_countries = ["England", "Spain", "Germany", "Italy", "France"]
                if country in major_countries:
                    major_league_ids.append(league["id"])
        
        # 2. For each league, get teams and historical data
        for league in leagues:
            league_id = league["id"]
            is_major = league_id in major_league_ids
            
            # Store major/smaller classification
            league["is_major"] = is_major
            
            # Update league with major classification
            self.db.leagues.update_one(
                {"id": league_id},
                {"$set": {"is_major": is_major}}
            )
            
            # Get teams
            self.get_teams_by_league(league_id)
            
            # Get historical matches (more seasons for major leagues)
            seasons = 5
            self.get_historical_matches(league_id, seasons)
        
        # 3. Get upcoming fixtures with odds
        # self.get_upcoming_fixtures(days_ahead=14)
        # 3. Get upcoming fixtures from both sources and create mappings
        self.get_fixtures_from_both_sources(days_ahead=14)

        # self.get_goalserve_fixtures(days_ahead=14)

        # # 4. Get player availability
        self.get_player_availability()
        
        # # 5. Get predictions
        # # self.get_predictions()
        self.get_goalserve_scoreline_probabilities()
        
        # # 6. Get rivals for derby identification
        self.get_rivals()
        
        # # 7. Get outright odds from GoalServe
        self.get_goalserve_outright_odds()
        
        # # 8. Get fixture odds from GoalServe
        self.get_goalserve_fixture_odds()
        
        # # 9. Get pre-match odds from Sportmonks
        # # self.get_sportmonks_prematch_odds()
        # self.create_missing_fixture_mappings()
        # 10. Get in-play odds from Sportmonks
        # self.get_sportmonks_inplay_odds()
        collector.verify_fixture_mappings()
        
        logger.info("Complete FDR data collection finished")

# Usage example
if __name__ == "__main__":
    # Get API tokens from environment variables
    sportmonks_token = os.getenv("SPORTMONKS_API_KEY")
    goalserve_token = os.getenv("GOALSERVE_API_KEY", "0f6230689b674453eee508dd50f5b2ca")
    
    # MongoDB URI is directly configured in the class
    collector = FDRDataCollector(sportmonks_token, goalserve_token)
    collector.collect_all_fdr_data()
    
    # collector.get_sportmonks_scoreline_probabilities()