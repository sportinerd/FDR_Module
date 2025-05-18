import requests
import json
from datetime import datetime
import sys

class FDRApiResponseExplorer:
    """Utility to explore responses from FDR API endpoints"""
    
    def __init__(self, base_url="http://localhost:5000"):
        """Initialize with the base URL for the API"""
        self.base_url = base_url
        self.session = requests.Session()
        
    def explore_all_endpoints(self):
        """Get and display responses from all main endpoints"""
        print("\n==========================================")
        print(f"FDR API Response Explorer - {self.base_url}")
        print("==========================================\n")
        
        # Explore health endpoint
        self.explore_health_endpoint()
        
        # Explore fixtures endpoint
        self.explore_fixtures_endpoint()
        
        # Explore team FDR endpoint
        self.explore_team_fdr_endpoint()
        
        # Explore league FDR endpoint
        self.explore_league_fdr_endpoint()
        
        # Explore Premier League specific endpoint
        self.explore_premier_league_endpoint()
        
        # Explore teams endpoint
        self.explore_teams_endpoint()
        
        # Explore leagues endpoint
        self.explore_leagues_endpoint()
    
    def explore_health_endpoint(self):
        """Explore the health check endpoint"""
        print("\n----- Health Check Endpoint -----")
        try:
            resp = self.session.get(f"{self.base_url}/health")
            self._print_response(resp, "Health Check")
        except Exception as e:
            print(f"❌ Error accessing health endpoint: {str(e)}")
    
    def explore_fixtures_endpoint(self):
        """Explore the fixtures endpoint with various parameters"""
        print("\n----- Fixtures Endpoint -----")
        
        # Default parameters
        try:
            print("\n🔍 Default fixtures query:")
            resp = self.session.get(f"{self.base_url}/api/fixtures")
            self._print_response(resp, "Fixtures (Default)", max_items=3)
        except Exception as e:
            print(f"❌ Error accessing fixtures endpoint: {str(e)}")
        
        # With Premier League filter
        try:
            print("\n🔍 Premier League fixtures query:")
            resp = self.session.get(f"{self.base_url}/api/fixtures?league_id=8")
            self._print_response(resp, "Fixtures (Premier League)", max_items=2)
        except Exception as e:
            print(f"❌ Error accessing PL fixtures endpoint: {str(e)}")
        
        # With team filter
        try:
            print("\n🔍 Team-specific fixtures query (Arsenal, Team ID 1):")
            resp = self.session.get(f"{self.base_url}/api/fixtures?team_id=1")
            self._print_response(resp, "Fixtures (Team Arsenal)", max_items=2)
        except Exception as e:
            print(f"❌ Error accessing team fixtures endpoint: {str(e)}")
    
    def explore_team_fdr_endpoint(self):
        """Explore the team FDR endpoint"""
        print("\n----- Team FDR Endpoint -----")
        
        # Arsenal (ID = 1)
        team_id = 1
        try:
            print(f"\n🔍 Team FDR for team ID {team_id} (Arsenal):")
            resp = self.session.get(f"{self.base_url}/api/fdr/team/{team_id}")
            self._print_response(resp, f"Team FDR (Arsenal)", max_items=3)
            
            # With averages included
            print(f"\n🔍 Team FDR with averages for team ID {team_id} (Arsenal):")
            resp = self.session.get(f"{self.base_url}/api/fdr/team/{team_id}?include_averages=true")
            self._print_response(resp, f"Team FDR with Averages (Arsenal)", max_items=3)
        except Exception as e:
            print(f"❌ Error accessing team FDR endpoint: {str(e)}")
    
    def explore_league_fdr_endpoint(self):
        """Explore the league FDR endpoint"""
        print("\n----- League FDR Endpoint -----")
        
        # Premier League (ID = 8)
        league_id = 8
        try:
            print(f"\n🔍 League FDR for league ID {league_id} (Premier League):")
            resp = self.session.get(f"{self.base_url}/api/fdr/league/{league_id}")
            self._print_response(resp, f"League FDR (Premier League)", max_items=2, max_nested=2)
            
            # With averages included
            print(f"\n🔍 League FDR with averages for league ID {league_id} (Premier League):")
            resp = self.session.get(f"{self.base_url}/api/fdr/league/{league_id}?include_averages=true")
            self._print_response(resp, f"League FDR with Averages (Premier League)", max_items=2, max_nested=2)
        except Exception as e:
            print(f"❌ Error accessing league FDR endpoint: {str(e)}")
    
    def explore_premier_league_endpoint(self):
        """Explore the dedicated Premier League FDR endpoint"""
        print("\n----- Premier League FDR Endpoint -----")
        
        try:
            print("\n🔍 Premier League dedicated endpoint:")
            resp = self.session.get(f"{self.base_url}/api/fdr/premier-league")
            self._print_response(resp, "Premier League FDR", max_items=2, max_nested=2)
        except Exception as e:
            print(f"❌ Error accessing Premier League FDR endpoint: {str(e)}")
    
    def explore_teams_endpoint(self):
        """Explore the teams endpoint"""
        print("\n----- Teams Endpoint -----")
        
        try:
            print("\n🔍 All teams:")
            resp = self.session.get(f"{self.base_url}/api/teams")
            self._print_response(resp, "Teams (All)", max_items=5)
            
            # With league filter
            print("\n🔍 Premier League teams:")
            resp = self.session.get(f"{self.base_url}/api/teams?league_id=8")
            self._print_response(resp, "Teams (Premier League)", max_items=5)
        except Exception as e:
            print(f"❌ Error accessing teams endpoint: {str(e)}")
    
    def explore_leagues_endpoint(self):
        """Explore the leagues endpoint"""
        print("\n----- Leagues Endpoint -----")
        
        try:
            print("\n🔍 All leagues:")
            resp = self.session.get(f"{self.base_url}/api/leagues")
            self._print_response(resp, "Leagues", max_items=5)
        except Exception as e:
            print(f"❌ Error accessing leagues endpoint: {str(e)}")
    
    def _print_response(self, response, title, max_items=None, max_nested=None):
        """Print formatted API response"""
        try:
            print(f"STATUS: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                
                # Print summary stats if available
                self._print_summary_stats(data)
                
                # Format and print content
                formatted_json = self._format_response_for_display(data, max_items, max_nested)
                print("\nRESPONSE CONTENT:")
                print(formatted_json)
            else:
                print(f"ERROR: {response.text}")
        except json.JSONDecodeError:
            print("Not a valid JSON response")
            print(f"RAW RESPONSE: {response.text[:500]}...")
        except Exception as e:
            print(f"Error formatting response: {str(e)}")
    
    def _print_summary_stats(self, data):
        """Print summary statistics from the response if available"""
        stats = []
        
        # Common count fields
        for count_field in ['count', 'fixtures_count', 'teams_count']:
            if count_field in data:
                stats.append(f"{count_field.replace('_', ' ').title()}: {data[count_field]}")
        
        # Check for specific data types
        if 'fixtures' in data:
            stats.append(f"Fixtures: {len(data['fixtures'])}")
        if 'teams' in data:
            stats.append(f"Teams: {len(data['teams'])}")
        if 'leagues' in data:
            stats.append(f"Leagues: {len(data['leagues'])}")
        
        # Check for Premier League flag
        if 'is_premier_league' in data:
            stats.append(f"Premier League: {'Yes' if data['is_premier_league'] else 'No'}")
            
        # Print stats if any were found
        if stats:
            print("\nSUMMARY:")
            for stat in stats:
                print(f"  • {stat}")
    
    def _format_response_for_display(self, data, max_items=None, max_nested=None):
        """Format response JSON for readable display, with optional limits on array items and nesting"""
        
        if isinstance(data, dict):
            # Limit dict content for display
            if max_nested is not None and max_nested <= 0:
                return "{...}"  # Truncate at max nesting level
            
            result = {}
            for key, value in data.items():
                # Limit nested levels recursively
                next_nested = None if max_nested is None else max_nested - 1
                result[key] = self._format_response_for_display(value, max_items, next_nested)
            
            return json.dumps(result, indent=2)
            
        elif isinstance(data, list):
            # Limit list items for display
            if max_items is not None and len(data) > max_items:
                # Format first max_items and indicate truncation
                shortened = data[:max_items]
                # Limit nested levels recursively
                next_nested = None if max_nested is None else max_nested - 1
                formatted = [self._format_response_for_display(item, max_items, next_nested) for item in shortened]
                return f"{json.dumps(formatted, indent=2)[:-1]},\n  ... {len(data) - max_items} more items\n]"
            else:
                # Format all items
                next_nested = None if max_nested is None else max_nested - 1
                return [self._format_response_for_display(item, max_items, next_nested) for item in data]
                
        else:
            # Return primitive values as is
            return data


if __name__ == "__main__":
    # Get base URL from command line argument or use default
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:5000"
    
    # Create explorer and run
    explorer = FDRApiResponseExplorer(base_url)
    explorer.explore_team_fdr_endpoint()