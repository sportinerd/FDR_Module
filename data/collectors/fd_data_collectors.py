# fdr_data_collector.py

import requests
import logging
from datetime import datetime, timedelta
import json
import os
from typing import Dict, List, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("fdr_collection.log")]
)
logger = logging.getLogger(__name__)

class FDRDataCollector:
    """Collect data from Sportmonks and GoalServe for FDR calculations"""
    
    def __init__(self, sportmonks_token, goalserve_token, data_dir="data"):
        self.sportmonks_token = sportmonks_token
        self.goalserve_token = goalserve_token
        self.data_dir = data_dir
        
        # Create data directory if it doesn't exist
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
        self._save_data("leagues.json", leagues)
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
        self._save_data(f"teams_league_{league_id}.json", teams)
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
            all_matches.extend(matches)
            
            logger.info(f"Found {len(matches)} completed matches for season {season_id}")
        
        self._save_data(f"historical_matches_league_{league_id}.json", all_matches)
        logger.info(f"Saved {len(all_matches)} historical matches for league {league_id}")
        return all_matches

    
    def get_upcoming_fixtures(self, days_ahead=14):
        """Get upcoming fixtures for next N days with odds"""
        today = datetime.now().strftime("%Y-%m-%d")
        future = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
        
        logger.info(f"Fetching upcoming fixtures from {today} to {future}")
        
        # Add 'f' before the string to make it an f-string
        response = self.sportmonks_request(f"/fixtures/between/{today}/{future}", {
            "include": "participants;odds;league",
            "per_page": 200
        })
        
        if not response or "data" not in response:
            logger.error("Failed to fetch upcoming fixtures")
            return []
        
        fixtures = response["data"]
        self._save_data("upcoming_fixtures.json", fixtures)
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
        injuries = []
        suspensions = []
        
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
                        "start_date": start_date,
                        "end_date": end_date,
                        "games_missed": sidelined["games_missed"],
                        "completed": sidelined["completed"]
                    }
                    
                    # Categorize as injury or suspension
                    if category == "injury":
                        injuries.append(availability_record)
                    else:
                        suspensions.append(availability_record)
        
        # Combine data
        availability_data = {
            "injuries": injuries,
            "suspensions": suspensions
        }
        
        self._save_data("player_availability.json", availability_data)
        logger.info(f"Saved player availability data: {len(injuries)} injuries, {len(suspensions)} suspensions")
        return availability_data

    
    def get_predictions(self, league_id=None, fixture_id=None):
        """Get match predictions from Sportmonks"""
        logger.info("Fetching predictions data")
        
        # Base endpoint is different than what you're using
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
        filename = "predictions.json"
        if fixture_id:
            filename = f"predictions_fixture_{fixture_id}.json"
        elif league_id:
            filename = f"predictions_league_{league_id}.json"
        
        self._save_data(filename, predictions)
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
        self._save_data("rivals.json", rivals)
        logger.info(f"Saved {len(rivals)} rivalry records")
        return rivals
    
    def get_goalserve_outright_odds(self):
        """Get outright odds from GoalServe"""
        logger.info("Fetching outright odds from GoalServe")
        
        try:
            response = requests.get(self.goalserve_outright_url)
            response.raise_for_status()
            data = response.json()
            
            self._save_data("goalserve_outright_odds.json", data)
            logger.info("Saved outright odds data")
            return data
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
        
        # Save raw XML for now (would need XML parser for structured data)
        with open(os.path.join(self.data_dir, "goalserve_fixture_odds.xml"), "w") as f:
            f.write(xml_response)
        
        logger.info("Saved fixture odds data")
        return xml_response
    
    def _save_data(self, filename, data):
        """Save data to JSON file"""
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    
    def collect_all_fdr_data(self, major_league_ids=None):
        """Collect all data needed for FDR calculation"""
        logger.info("Starting complete FDR data collection")
        
        # 1. Get all leagues
        leagues = self.get_leagues()
        
        # Determine which leagues are major based on data availability 
        # This replaces static classification
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
            
            # Get teams
            self.get_teams_by_league(league_id)
            
            # Get historical matches (more seasons for major leagues)
            seasons = 2
            self.get_historical_matches(league_id, seasons)
        
        # Update leagues with classification
        self._save_data("leagues.json", leagues)
        
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
        
        logger.info("Complete FDR data collection finished")

# Usage example
if __name__ == "__main__":
    # Replace with your actual API tokens
    sportmonks_token = "GH7YT0CYNgDWcl99LLPkYRR6cWnVZchVhz1IL6QpTBy5ciYSyyPs5mEv8n82"
    goalserve_token = "0f6230689b674453eee508dd50f5b2ca"  # From your example
    
    collector = FDRDataCollector(sportmonks_token, goalserve_token)
    collector.collect_all_fdr_data()
