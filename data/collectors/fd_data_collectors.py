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
        """Get fixture odds from GoalServe"""
        logger.info("Fetching fixture odds from GoalServe")
        
        endpoint = "getodds/soccer"
        params = {"cat": "soccer_10"}
        
        xml_response = self.goalserve_request(endpoint, params)
        if not xml_response:
            logger.error("Failed to fetch fixture odds")
            return None
        
        # Parse XML
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(xml_response)
            
            fixture_odds = []
            
            # Process each category (league)
            for category in root.findall('category'):
                category_name = category.get('name')
                category_id = category.get('id')
                
                # Process matches in this category
                matches_elem = category.find('matches')
                if matches_elem is None:
                    continue
                
                for match in matches_elem.findall('match'):
                    match_id = match.get('id')
                    match_date = match.get('date')
                    match_time = match.get('time')
                    match_status = match.get('status')
                    
                    # Get teams
                    local_team = match.find('localteam')
                    visitor_team = match.find('visitorteam')
                    
                    if local_team is None or visitor_team is None:
                        continue
                    
                    local_team_name = local_team.get('name')
                    local_team_id = local_team.get('id')
                    visitor_team_name = visitor_team.get('name')
                    visitor_team_id = visitor_team.get('id')
                    
                    # Get odds
                    odds_elem = match.find('odds')
                    if odds_elem is None:
                        continue
                    
                    match_odds = {
                        'match_id': match_id,
                        'category_name': category_name,
                        'category_id': category_id,
                        'match_date': match_date,
                        'match_time': match_time,
                        'match_status': match_status,
                        'local_team': {
                            'id': local_team_id,
                            'name': local_team_name
                        },
                        'visitor_team': {
                            'id': visitor_team_id,
                            'name': visitor_team_name
                        },
                        'odds': []
                    }
                    
                    # Process odds by type (focusing on Match Winner for FDR calculations)
                    for odds_type in odds_elem.findall('type'):
                        type_value = odds_type.get('value')
                        type_id = odds_type.get('id')
                        
                        bookmakers_data = []
                        
                        for bookmaker in odds_type.findall('bookmaker'):
                            bookmaker_name = bookmaker.get('name')
                            bookmaker_id = bookmaker.get('id')
                            
                            # Extract home, draw, away odds
                            home_odd = None
                            draw_odd = None
                            away_odd = None
                            
                            for odd in bookmaker.findall('odd'):
                                odd_name = odd.get('name')
                                odd_value = odd.get('value')
                                
                                if odd_name == "Home":
                                    home_odd = float(odd_value)
                                elif odd_name == "Draw":
                                    draw_odd = float(odd_value)
                                elif odd_name == "Away":
                                    away_odd = float(odd_value)
                            
                            # Only add complete sets of odds
                            if type_value == "Match Winner" and home_odd and draw_odd and away_odd:
                                bookmakers_data.append({
                                    'bookmaker_id': bookmaker_id,
                                    'bookmaker_name': bookmaker_name,
                                    'home_odd': home_odd,
                                    'draw_odd': draw_odd,
                                    'away_odd': away_odd
                                })
                        
                        if bookmakers_data:
                            match_odds['odds'].append({
                                'type_id': type_id,
                                'type_value': type_value,
                                'bookmakers': bookmakers_data
                            })
                    
                    # Only add matches with valid odds data
                    if match_odds['odds']:
                        fixture_odds.append(match_odds)
            
            # Save to MongoDB
            self.save_to_mongodb("fixtureOdds", fixture_odds, identifier_field="match_id")
            
            logger.info(f"Saved fixture odds data: {len(fixture_odds)} records")
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
            seasons = 10 if is_major else 5
            self.get_historical_matches(league_id, seasons)
        
        # 3. Get upcoming fixtures with odds
        self.get_upcoming_fixtures()
        
        # 4. Get player availability
        self.get_player_availability()
        
        # 5. Get predictions
        self.get_predictions()
        
        # 6. Get rivals for derby identification
        self.get_rivals()
        
        # 7. Get outright odds from GoalServe
        self.get_goalserve_outright_odds()
        
        # 8. Get fixture odds from GoalServe
        self.get_goalserve_fixture_odds()
        
        # 9. Get pre-match odds from Sportmonks
        self.get_sportmonks_prematch_odds()
        
        # 10. Get in-play odds from Sportmonks
        self.get_sportmonks_inplay_odds()
        
        logger.info("Complete FDR data collection finished")

# Usage example
if __name__ == "__main__":
    # Get API tokens from environment variables
    sportmonks_token = os.getenv("SPORTMONKS_API_KEY")
    goalserve_token = os.getenv("GOALSERVE_API_KEY", "0f6230689b674453eee508dd50f5b2ca")
    
    # MongoDB URI is directly configured in the class
    collector = FDRDataCollector(sportmonks_token, goalserve_token)
    collector.collect_all_fdr_data()