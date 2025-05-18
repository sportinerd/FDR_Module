# # from flask import Flask, jsonify, request
# # from flask_cors import CORS
# # from apscheduler.schedulers.background import BackgroundScheduler
# # from apscheduler.triggers.cron import CronTrigger
# # import logging
# # from pymongo import MongoClient
# # from pymongo.server_api import ServerApi
# # import os
# # from datetime import datetime, timedelta
# # from dotenv import load_dotenv
# # import atexit

# # # Import our FDR modules
# # from data.collectors.fd_data_collectors import FDRDataCollector
# # from calculation_engine.fdr_calculator import FDRCalculator

# # # Load environment variables
# # load_dotenv()

# # # Configure logging
# # logging.basicConfig(
# #     level=logging.INFO,
# #     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
# #     handlers=[logging.StreamHandler(), logging.FileHandler("fdr_api.log")]
# # )
# # logger = logging.getLogger(__name__)

# # # Initialize Flask app
# # app = Flask(__name__)
# # CORS(app)  # Enable CORS for all routes

# # # MongoDB connection
# # mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://naymul504:soupnaymul09@pf365.2pguj.mongodb.net/?retryWrites=true&w=majority&appName=pf365")
# # client = MongoClient(mongo_uri, server_api=ServerApi('1'))
# # db = client['Analytics']

# # # API authentication
# # # API_KEY = os.getenv("API_KEY", "your_default_api_key")

# # # def validate_api_key():
# # #     """Validate API key from request"""
# # #     key = request.headers.get('X-API-KEY')
# # #     return key == API_KEY

# # # ----------------- API Routes -----------------

# # @app.route('/health', methods=['GET'])
# # def health_check():
# #     """Health check endpoint"""
# #     return jsonify({
# #         "status": "ok",
# #         "timestamp": datetime.now().isoformat(),
# #         "version": "1.0.0"
# #     })

# # @app.route('/api/fixtures', methods=['GET'])
# # def get_fixtures():
# #     """Get upcoming fixtures with FDR data"""
# #     # if not validate_api_key():
# #     #     return jsonify({"error": "Unauthorized"}), 401
    
# #     try:
# #         days = int(request.args.get('days', 14))
# #         league_id = request.args.get('league_id')
# #         team_id = request.args.get('team_id')
        
# #         start_date = datetime.now().strftime("%Y-%m-%d")
# #         end_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        
# #         # Build MongoDB query
# #         query = {"starting_at": {"$regex": f"^{start_date}|^{end_date}"}}
        
# #         if league_id:
# #             query["$or"] = [
# #                 {"league_id": league_id},
# #                 {"league.id": league_id}
# #             ]
        
# #         if team_id:
# #             query["$or"] = query.get("$or", []) + [
# #                 {"participants.id": team_id}
# #             ]
        
# #         # Only get fixtures with FDR data
# #         query["fdr"] = {"$exists": True}
        
# #         # Execute query
# #         fixtures = list(db.fixtures.find(query, {"_id": 0}))
        
# #         return jsonify({
# #             "count": len(fixtures),
# #             "fixtures": fixtures
# #         })
    
# #     except Exception as e:
# #         logger.error(f"Error fetching fixtures: {str(e)}")
# #         return jsonify({"error": str(e)}), 500

# # @app.route('/api/fdr/team/<team_id>', methods=['GET'])
# # def get_team_fdr(team_id):
# #     """Get all FDR data for a specific team"""
# #     # if not validate_api_key():
# #     #     return jsonify({"error": "Unauthorized"}), 401
    
# #     try:
# #         days = int(request.args.get('days', 60))
# #         start_date = datetime.now().strftime("%Y-%m-%d")
# #         end_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        
# #         # Convert team_id to integer if it's a string
# #         if isinstance(team_id, str) and team_id.isdigit():
# #             team_id = int(team_id)
        
# #         # Debug: Check how many fixtures we find before applying FDR filter
# #         fixtures_before_fdr = list(db.fixtures.find({
# #             "$or": [
# #                 {"participants.id": team_id},
# #                 {"local_team.id": team_id},
# #                 {"visitor_team.id": team_id}
# #             ]
# #         }))
# #         print(f"Debug: Found {len(fixtures_before_fdr)} fixtures for team {team_id} before FDR filter")
        
# #         # Make query more flexible
# #         query = {
# #             "$or": [
# #                 {"participants.id": team_id},
# #                 {"local_team.id": team_id},
# #                 {"visitor_team.id": team_id}
# #             ],
# #             "fdr": {"$exists": True}
# #         }

# #         # Use regex for more lenient date comparison
# #         query["starting_at"] = {"$regex": f"^{start_date[:10]}"}
        
# #         fixtures = list(db.fixtures.find(query, {"_id": 0}))
# #         print(f"Found {len(fixtures)} fixtures with FDR data for team {team_id}")
# #         # Extract and format FDR data specifically for this team
# #         team_fixtures = []
# #         for fixture in fixtures:
# #             # Determine if team is home or away
# #             is_home = False
# #             if "participants" in fixture:
# #                 is_home = fixture["participants"][0].get("id") == team_id
# #             elif "local_team" in fixture:
# #                 is_home = fixture["local_team"].get("id") == team_id
            
# #             # Get opponent
# #             opponent_name = "Unknown"
# #             if "participants" in fixture and len(fixture["participants"]) >= 2:
# #                 opponent_name = fixture["participants"][1].get("name") if is_home else fixture["participants"][0].get("name")
# #             elif "local_team" in fixture and "visitor_team" in fixture:
# #                 opponent_name = fixture["visitor_team"].get("name") if is_home else fixture["local_team"].get("name")
            
# #             # Get FDR data
# #             fdr_data = fixture.get("fdr", {})
# #             team_fdr = fdr_data.get("overall", {}).get("home" if is_home else "away", {})
# #             print("Here is the team fdr data")
# #             print(team_fdr)
# #             team_fixtures.append({
# #                 "fixture_id": fixture.get("id"),
# #                 "date": fixture.get("starting_at"),
# #                 "is_home": is_home,
# #                 "opponent": opponent_name,
# #                 "fdr": team_fdr.get("fdr"),
# #                 "category": team_fdr.get("category"),
# #                 # "color": team_fdr.get("color"),
# #                 "raw_score": team_fdr.get("raw_score"),
# #                 "xg": team_fdr.get("xg"),
# #                 # Components data (new field names)
# #                 "attacking_strength": fdr_data.get("components", {}).get("home" if is_home else "away", {}).get("attack_strength"),
# #                 "defending_strength": fdr_data.get("components", {}).get("home" if is_home else "away", {}).get("defense_weakness"),
# #                 # Legacy field names (for backward compatibility)
# #                 "attacking_fdr": fdr_data.get("components", {}).get("home" if is_home else "away", {}).get("attack_strength"),
# #                 "defending_fdr": fdr_data.get("components", {}).get("home" if is_home else "away", {}).get("defense_weakness")
# #             })

# #         return jsonify({
# #             "team_id": team_id,
# #             "fixtures_count": len(team_fixtures),
# #             "fixtures": team_fixtures
# #         })
    
# #     except Exception as e:
# #         logger.error(f"Error fetching team FDR: {str(e)}")
# #         return jsonify({"error": str(e)}), 500

# # @app.route('/api/fdr/league/<league_id>', methods=['GET'])
# # def get_league_fdr(league_id):
# #     """Get FDR data for all teams in a league"""
# #     # if not validate_api_key():
# #     #     return jsonify({"error": "Unauthorized"}), 401
    
# #     try:
# #         # Get all teams in this league
# #         teams = list(db.teams.find({
# #             "$or": [
# #                 {"league_id": league_id},
# #                 {"league.id": league_id},
# #                 {"league.league_id": league_id}
# #             ]
# #         }, {"_id": 0, "id": 1, "name": 1}))
        
# #         print(f"Debug: Found {len(teams)} teams for league {league_id}")
        
# #         team_fdr_data = []
# #         for team in teams:
# #             team_id = team.get("id")
# #             team_name = team.get("name")
            
# #             # Get upcoming fixtures for this team
# #             query = {
# #                 "starting_at": {"$gte": datetime.now().strftime("%Y-%m-%d")},
# #                 "$or": [
# #                     {"participants.id": team_id},
# #                     {"local_team.id": team_id},
# #                     {"visitor_team.id": team_id}
# #                 ],
# #                 "fdr": {"$exists": True}
# #             }
            
# #             fixtures = list(db.fixtures.find(query, {"_id": 0}).limit(5))
            
# #             # Extract FDR data
# #             next_fixtures = []
# #             for fixture in fixtures:
# #                 # Determine if team is home or away
# #                 is_home = False
# #                 if "participants" in fixture:
# #                     is_home = fixture["participants"][0].get("id") == team_id
# #                 elif "local_team" in fixture:
# #                     is_home = fixture["local_team"].get("id") == team_id
                
# #                 # Get FDR
# #                 fdr_data = fixture.get("fdr", {}).get("overall", {}).get("home" if is_home else "away", {})
                
# #                 next_fixtures.append({
# #                     "fixture_id": fixture.get("id"),
# #                     "date": fixture.get("starting_at"),
# #                     "fdr": fdr_data.get("fdr"),
# #                     "category": fdr_data.get("category")
# #                 })
            
# #             team_fdr_data.append({
# #                 "team_id": team_id,
# #                 "team_name": team_name,
# #                 "next_fixtures": next_fixtures
# #             })
        
# #         return jsonify({
# #             "league_id": league_id,
# #             "teams": team_fdr_data
# #         })
    
# #     except Exception as e:
# #         logger.error(f"Error fetching league FDR: {str(e)}")
# #         return jsonify({"error": str(e)}), 500


# # # ----------------- Background Jobs -----------------

# # def run_data_collection_job():
# #     """Run the FDR data collection process"""
# #     try:
# #         logger.info("Starting scheduled FDR data collection job")
        
# #         # Get API tokens from environment variables
# #         sportmonks_token = os.getenv("SPORTMONKS_API_KEY")
# #         goalserve_token = os.getenv("GOALSERVE_API_KEY", "0f6230689b674453eee508dd50f5b2ca")
        
# #         # Run data collection
# #         collector = FDRDataCollector(sportmonks_token, goalserve_token)
# #         collector.collect_all_fdr_data()
        
# #         logger.info("FDR data collection job completed")
        
# #     except Exception as e:
# #         logger.error(f"Error in data collection job: {str(e)}")

# # def run_fdr_calculation_job():
# #     """Run the FDR calculation process"""
# #     try:
# #         logger.info("Starting scheduled FDR calculation job")
        
# #         # Initialize calculator and run comprehensive calculation
# #         calculator = FDRCalculator()
# #         calculator.comprehensive_fdr_calculation()
        
# #         logger.info("FDR calculation job completed")
        
# #     except Exception as e:
# #         logger.error(f"Error in FDR calculation job: {str(e)}")

# # # Initialize scheduler
# # scheduler = BackgroundScheduler()

# # # Schedule data collection job (runs daily at 1:00 AM)
# # scheduler.add_job(
# #     func=run_data_collection_job,
# #     trigger=CronTrigger(hour=1, minute=0),
# #     id='data_collection_job',
# #     name='Daily FDR Data Collection',
# #     replace_existing=True
# # )

# # # Schedule FDR calculation job (runs daily at 2:00 AM)
# # scheduler.add_job(
# #     func=run_fdr_calculation_job,
# #     trigger=CronTrigger(hour=2, minute=0),
# #     id='fdr_calculation_job',
# #     name='Daily FDR Calculation',
# #     replace_existing=True
# # )

# # # Start the scheduler
# # scheduler.start()

# # # Register shutdown
# # atexit.register(lambda: scheduler.shutdown())

# # # Run the application
# # if __name__ == '__main__':
# #     # Run jobs immediately on startup (optional)
# #     # run_data_collection_job()
# #     # run_fdr_calculation_job()
    
# #     app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)), debug=False)


# from flask import Flask, jsonify, request
# from flask_cors import CORS
# from apscheduler.schedulers.background import BackgroundScheduler
# from apscheduler.triggers.cron import CronTrigger
# import logging
# from pymongo import MongoClient
# from pymongo.server_api import ServerApi
# import os
# from datetime import datetime, timedelta
# from dotenv import load_dotenv
# import atexit

# # Import our FDR modules
# from data.collectors.fd_data_collectors import FDRDataCollector
# from calculation_engine.fdr_calculator import FDRCalculator

# # Load environment variables
# load_dotenv()

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
#     handlers=[logging.StreamHandler(), logging.FileHandler("fdr_api.log")]
# )

# logger = logging.getLogger(__name__)

# # Initialize Flask app
# app = Flask(__name__)
# CORS(app)  # Enable CORS for all routes

# # MongoDB connection
# mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://naymul504:soupnaymul09@pf365.2pguj.mongodb.net/?retryWrites=true&w=majority&appName=pf365")
# client = MongoClient(mongo_uri, server_api=ServerApi('1'))
# db = client['Analytics']

# # Constants
# PREMIER_LEAGUE_ID = 8  # English Premier League ID

# # ----------------- Helper Functions -----------------

# def is_premier_league(league_id):
#     """Check if the league_id is for the Premier League"""
#     return str(league_id) == str(PREMIER_LEAGUE_ID)

# # ----------------- API Routes -----------------

# @app.route('/health', methods=['GET'])
# def health_check():
#     """Health check endpoint"""
#     return jsonify({
#         "status": "ok",
#         "timestamp": datetime.now().isoformat(),
#         "version": "1.0.0"
#     })

# @app.route('/api/fixtures', methods=['GET'])
# def get_fixtures():
#     """Get upcoming fixtures with FDR data"""
#     try:
#         days = int(request.args.get('days', 14))
#         league_id = request.args.get('league_id')
#         team_id = request.args.get('team_id')
        
#         # Default to Premier League if not specified
#         if not league_id:
#             league_id = str(PREMIER_LEAGUE_ID)
        
#         start_date = datetime.now().strftime("%Y-%m-%d")
#         end_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        
#         # Build MongoDB query
#         query = {
#             "starting_at": {"$gte": start_date},
#             "fdr": {"$exists": True}
#         }
        
#         # Add league filter
#         if league_id:
#             query["$or"] = [
#                 {"league_id": league_id},
#                 {"league.id": league_id}
#             ]
        
#         # Add team filter if provided
#         if team_id:
#             if "$or" in query:
#                 query["$or"] = query["$or"] + [{"participants.id": team_id}]
#             else:
#                 query["$or"] = [{"participants.id": team_id}]
        
#         # Execute query
#         fixtures = list(db.fixtures.find(query, {"_id": 0}))
        
#         return jsonify({
#             "count": len(fixtures),
#             "fixtures": fixtures,
#             "is_premier_league": is_premier_league(league_id)
#         })
        
#     except Exception as e:
#         logger.error(f"Error fetching fixtures: {str(e)}")
#         return jsonify({"error": str(e)}), 500

# @app.route('/api/fdr/team/<team_id>', methods=['GET'])
# def get_team_fdr(team_id):
#     """Get all FDR data for a specific team"""
#     try:
#         days = int(request.args.get('days', 60))
#         start_date = datetime.now().strftime("%Y-%m-%d")
        
#         # Convert team_id to integer if it's a string
#         if isinstance(team_id, str) and team_id.isdigit():
#             team_id = int(team_id)
        
#         # Make query more flexible
#         query = {
#             "$or": [
#                 {"participants.id": team_id},
#                 {"local_team.id": team_id},
#                 {"visitor_team.id": team_id}
#             ],
#             "fdr": {"$exists": True},
#             "starting_at": {"$gte": start_date}
#         }
        
#         # Add Premier League filter for EPL-specific data
#         premier_league_query = query.copy()
#         premier_league_query["$or"] = premier_league_query.get("$or", []) + [
#             {"league_id": str(PREMIER_LEAGUE_ID)},
#             {"league.id": str(PREMIER_LEAGUE_ID)}
#         ]
        
#         # Try Premier League specific query first
#         fixtures = list(db.fixtures.find(premier_league_query, {"_id": 0}))
        
#         # If no Premier League fixtures, fall back to all leagues
#         if not fixtures:
#             fixtures = list(db.fixtures.find(query, {"_id": 0}))
#             is_premier_league = False
#         else:
#             is_premier_league = True
        
#         logger.info(f"Found {len(fixtures)} fixtures with FDR data for team {team_id}")
        
#         # Extract and format FDR data specifically for this team
#         team_fixtures = []
#         for fixture in fixtures:
#             # Determine if team is home or away
#             is_home = False
#             if "participants" in fixture:
#                 is_home = fixture["participants"][0].get("id") == team_id
#             elif "local_team" in fixture:
#                 is_home = fixture["local_team"].get("id") == team_id
                
#             # Get opponent
#             opponent_name = "Unknown"
#             if "participants" in fixture and len(fixture["participants"]) >= 2:
#                 opponent_name = fixture["participants"][1].get("name") if is_home else fixture["participants"][0].get("name")
#             elif "local_team" in fixture and "visitor_team" in fixture:
#                 opponent_name = fixture["visitor_team"].get("name") if is_home else fixture["local_team"].get("name")
                
#             # Get FDR data
#             fdr_data = fixture.get("fdr", {})
#             team_fdr = fdr_data.get("overall", {}).get("home" if is_home else "away", {})
            
#             # Get attacking/defending data
#             attacking_fdr = fdr_data.get("components", {}).get("home" if is_home else "away", {}).get("attack_strength")
#             defending_fdr = fdr_data.get("components", {}).get("home" if is_home else "away", {}).get("defense_weakness")
            
#             team_fixtures.append({
#                 "fixture_id": fixture.get("id"),
#                 "date": fixture.get("starting_at"),
#                 "is_home": is_home,
#                 "opponent": opponent_name,
#                 "fdr": team_fdr.get("fdr"),
#                 "category": team_fdr.get("category"),
#                 "raw_score": team_fdr.get("raw_score"),
#                 "xg": team_fdr.get("xg"),
#                 "attacking_strength": attacking_fdr,
#                 "defending_strength": defending_fdr,
#                 "attacking_fdr": attacking_fdr,
#                 "defending_fdr": defending_fdr
#             })
        
#         # Get team name
#         team_name = "Unknown"
#         team = db.teams.find_one({"id": team_id})
#         if team:
#             team_name = team.get("name", "Unknown")
        
#         return jsonify({
#             "team_id": team_id,
#             "team_name": team_name,
#             "fixtures_count": len(team_fixtures),
#             "fixtures": team_fixtures,
#             "is_premier_league": is_premier_league
#         })
        
#     except Exception as e:
#         logger.error(f"Error fetching team FDR: {str(e)}")
#         return jsonify({"error": str(e)}), 500

# @app.route('/api/fdr/league/<league_id>', methods=['GET'])
# def get_league_fdr(league_id):
#     """Get FDR data for all teams in a league"""
#     try:
#         # Check if we're dealing with Premier League
#         is_epl = is_premier_league(league_id)
        
#         # DIRECTLY get fixtures with FDR data for this league
#         fixtures_query = {
#             "$or": [
#                 {"league_id": league_id},
#                 {"league.id": league_id}
#             ],
#             "starting_at": {"$gte": datetime.now().strftime("%Y-%m-%d")},
#             "fdr": {"$exists": True}
#         }
        
#         fixtures = list(db.fixtures.find(fixtures_query, {"_id": 0}))
#         logger.info(f"Found {len(fixtures)} fixtures for league {league_id}")
        
#         # Extract unique teams from fixtures participants
#         team_dict = {}  # Use dict to avoid duplicates
#         for fixture in fixtures:
#             if "participants" in fixture and len(fixture["participants"]) >= 2:
#                 for participant in fixture["participants"]:
#                     team_id = participant.get("id")
#                     team_name = participant.get("name")
#                     if team_id and team_name:
#                         team_dict[team_id] = team_name
        
#         logger.info(f"Extracted {len(team_dict)} unique teams from fixtures")
        
#         # Get league name
#         league_name = "Unknown League"
#         league = db.leagues.find_one({"id": league_id})
#         if league:
#             league_name = league.get("name", "Unknown League")
        
#         team_fdr_data = []
#         # Process each team's fixtures
#         for team_id, team_name in team_dict.items():
#             team_fixtures = []
#             for fixture in fixtures:
#                 # Check if this team is in the fixture
#                 is_in_fixture = False
#                 is_home = False
#                 opponent_name = "Unknown"
                
#                 if "participants" in fixture and len(fixture["participants"]) >= 2:
#                     for idx, participant in enumerate(fixture["participants"]):
#                         if participant.get("id") == team_id:
#                             is_in_fixture = True
#                             is_home = (idx == 0)  # First team is home
#                             opponent_idx = 1 if idx == 0 else 0
#                             opponent_name = fixture["participants"][opponent_idx].get("name", "Unknown")
#                             break
                
#                 if is_in_fixture:
#                     # Get FDR data
#                     fdr_data = fixture.get("fdr", {}).get("overall", {}).get("home" if is_home else "away", {})
                    
#                     team_fixtures.append({
#                         "fixture_id": fixture.get("id"),
#                         "date": fixture.get("starting_at"),
#                         "is_home": is_home,
#                         "opponent": opponent_name,
#                         "fdr": fdr_data.get("fdr"),
#                         "category": fdr_data.get("category")
#                     })
            
#             # Only include teams with fixtures
#             if team_fixtures:
#                 team_fdr_data.append({
#                     "team_id": team_id,
#                     "team_name": team_name,
#                     "next_fixtures": team_fixtures
#                 })
        
#         return jsonify({
#             "league_id": league_id,
#             "league_name": league_name,
#             "is_premier_league": is_epl,
#             "teams": team_fdr_data
#         })
        
#     except Exception as e:
#         logger.error(f"Error fetching league FDR: {str(e)}")
#         return jsonify({"error": str(e)}), 500


# @app.route('/api/fdr/premier-league', methods=['GET'])
# def get_premier_league_fdr():
#     """Dedicated endpoint for Premier League FDR data"""
#     return get_league_fdr(PREMIER_LEAGUE_ID)

# # ----------------- Background Jobs -----------------

# def run_data_collection_job():
#     """Run the FDR data collection process"""
#     try:
#         logger.info("Starting scheduled FDR data collection job")
        
#         # Get API tokens from environment variables
#         sportmonks_token = os.getenv("SPORTMONKS_API_KEY")
#         goalserve_token = os.getenv("GOALSERVE_API_KEY", "0f6230689b674453eee508dd50f5b2ca")
        
#         # Run data collection with focus on Premier League
#         collector = FDRDataCollector(sportmonks_token, goalserve_token)
#         collector.collect_all_fdr_data(major_league_ids=[PREMIER_LEAGUE_ID])
        
#         logger.info("FDR data collection job completed")
#     except Exception as e:
#         logger.error(f"Error in data collection job: {str(e)}")

# def run_fdr_calculation_job():
#     """Run the FDR calculation process"""
#     try:
#         logger.info("Starting scheduled FDR calculation job")
        
#         # Initialize calculator and run comprehensive calculation
#         calculator = FDRCalculator()
#         calculator.comprehensive_fdr_calculation()
        
#         logger.info("FDR calculation job completed")
#     except Exception as e:
#         logger.error(f"Error in FDR calculation job: {str(e)}")

# # Initialize scheduler
# scheduler = BackgroundScheduler()

# # Schedule data collection job (runs daily at 1:00 AM)
# scheduler.add_job(
#     func=run_data_collection_job,
#     trigger=CronTrigger(hour=1, minute=0),
#     id='data_collection_job',
#     name='Daily FDR Data Collection',
#     replace_existing=True
# )

# # Schedule FDR calculation job (runs daily at 2:00 AM)
# scheduler.add_job(
#     func=run_fdr_calculation_job,
#     trigger=CronTrigger(hour=2, minute=0),
#     id='fdr_calculation_job',
#     name='Daily FDR Calculation',
#     replace_existing=True
# )

# # Start the scheduler
# scheduler.start()

# # Register shutdown
# atexit.register(lambda: scheduler.shutdown())

# # Run the application
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)), debug=False)





from flask import Flask, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import atexit

# Import our FDR modules
from data.collectors.fd_data_collectors import FDRDataCollector
from calculation_engine.fdr_calculator import FDRCalculator

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("fdr_api.log")]
)

logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# MongoDB connection
mongo_uri = os.getenv("MONGO_URI", "mongodb+srv://naymul504:soupnaymul09@pf365.2pguj.mongodb.net/?retryWrites=true&w=majority&appName=pf365")
client = MongoClient(mongo_uri, server_api=ServerApi('1'))
db = client['Analytics']

# Constants
PREMIER_LEAGUE_ID = 8  # English Premier League ID

# ----------------- Helper Functions -----------------

def is_premier_league(league_id):
    """Check if the league_id is for the Premier League"""
    return str(league_id) == str(PREMIER_LEAGUE_ID)

def get_team_data_from_fixture(fixture, team_id):
    """Extract team information from fixture data"""
    # Check if team exists in participants
    if "participants" in fixture and isinstance(fixture["participants"], list):
        for participant in fixture["participants"]:
            if participant.get("id") == team_id:
                return participant
    
    # Fallback to local_team/visitor_team
    if "local_team" in fixture and fixture["local_team"].get("id") == team_id:
        return fixture["local_team"]
    
    if "visitor_team" in fixture and fixture["visitor_team"].get("id") == team_id:
        return fixture["visitor_team"]
    
    return None

def get_opponent_data(fixture, team_id):
    """Get opponent team data from fixture"""
    # Check participants first
    if "participants" in fixture and isinstance(fixture["participants"], list) and len(fixture["participants"]) >= 2:
        for idx, participant in enumerate(fixture["participants"]):
            if participant.get("id") == team_id:
                # Return the other participant
                other_idx = 1 if idx == 0 else 0
                if other_idx < len(fixture["participants"]):
                    return fixture["participants"][other_idx]
    
    # Fallback to local_team/visitor_team
    if "local_team" in fixture and "visitor_team" in fixture:
        if fixture["local_team"].get("id") == team_id:
            return fixture["visitor_team"]
        elif fixture["visitor_team"].get("id") == team_id:
            return fixture["local_team"]
    
    return {"name": "Unknown Team", "id": None}

def is_home_team(fixture, team_id):
    """Determine if the team is playing at home"""
    # Check participants (first team is typically home)
    if "participants" in fixture and isinstance(fixture["participants"], list) and len(fixture["participants"]) > 0:
        return fixture["participants"][0].get("id") == team_id
    
    # Fallback to local_team
    if "local_team" in fixture:
        return fixture["local_team"].get("id") == team_id
    
    return False

def extract_fdr_data(fixture, team_id):
    """Extract FDR data for a specific team from fixture"""
    home = is_home_team(fixture, team_id)
    opponent = get_opponent_data(fixture, team_id)
    
    # Get overall FDR data
    fdr_data = fixture.get("fdr", {})
    overall_fdr = fdr_data.get("overall", {}).get("home" if home else "away", {})
    
    # Get component data
    component_data = fdr_data.get("components", {}).get("home" if home else "away", {})
    
    return {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("starting_at"),
        "is_home": home,
        "opponent": opponent.get("name", "Unknown"),
        "opponent_id": opponent.get("id"),
        "league_id": fixture.get("league_id") or fixture.get("league", {}).get("id"),
        "league_name": fixture.get("league", {}).get("name"),
        "fdr": overall_fdr.get("fdr"),
        "category": overall_fdr.get("category"),
        "raw_score": overall_fdr.get("raw_score"),
        "xg": overall_fdr.get("xg"),
        "attacking_strength": component_data.get("attack_strength"),
        "defending_strength": component_data.get("defense_weakness"),
        "attacking_fdr": component_data.get("attack_strength"),  # Legacy naming
        "defending_fdr": component_data.get("defense_weakness"),  # Legacy naming
        "fixture_name": fixture.get("name"),
        "fixture_status": fixture.get("state_id")
    }

def extract_league_data(fixture):
    """Extract league information from fixture"""
    if "league" in fixture and isinstance(fixture["league"], dict):
        return fixture["league"]
    
    # Fallback to basic league info
    return {
        "id": fixture.get("league_id"),
        "name": "Unknown League"
    }

# ----------------- API Routes -----------------

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "version": "1.1.0"
    })

@app.route('/api/fixtures', methods=['GET'])
def get_fixtures():
    """Get fixtures with FDR data only"""
    try:
        league_id = request.args.get('league_id')
        team_id = request.args.get('team_id')
        
        # Try to convert team_id to int if it's a number string
        if team_id and team_id.isdigit():
            team_id = int(team_id)
            
        if league_id and league_id.isdigit():
            league_id = int(league_id)
            
        # Default to Premier League if not specified
        if not league_id:
            league_id = PREMIER_LEAGUE_ID
        
        # Base query: only fetch fixtures with FDR data
        base_query = {
            "fdr": {"$exists": True}
        }
        
        # Build additional filter conditions
        and_conditions = []
        
        # Add league filter with all possible field paths
        if league_id:
            and_conditions.append({
                "$or": [
                    {"league_id": league_id},
                    {"league.id": league_id},
                    {"league.league_id": league_id}
                ]
            })
            
        # Add team filter with proper array handling
        if team_id:
            team_query = [
                {"participants.id": team_id},
                {"participants": {"$elemMatch": {"id": team_id}}},
                {"local_team.id": team_id},
                {"visitor_team.id": team_id}
            ]
            and_conditions.append({"$or": team_query})
            
        # Combine base query with optional filters
        final_query = base_query
        if and_conditions:
            final_query = {
                "$and": [base_query] + and_conditions
            }
            
        logger.debug(f"Final MongoDB query: {final_query}")
        
        # Fetch matching fixtures
        fixtures = list(db.fixtures.find(final_query, {"_id": 0}).sort("starting_at", 1))
        logger.info(f"Found {len(fixtures)} fixtures with FDR data")
        
        # Extract unique leagues
        leagues = {}
        
        # Process fixtures to create simplified response structure
        simplified_fixtures = []
        for fixture in fixtures:
            # Extract team names
            home_team = "Unknown"
            away_team = "Unknown"
            
            if "participants" in fixture and isinstance(fixture["participants"], list) and len(fixture["participants"]) >= 2:
                home_team = fixture["participants"][0].get("name", "Unknown")
                away_team = fixture["participants"][1].get("name", "Unknown")
            elif "local_team" in fixture and "visitor_team" in fixture:
                home_team = fixture["local_team"].get("name", "Unknown")
                away_team = fixture["visitor_team"].get("name", "Unknown")
            
            # Make sure FDR data exists with the expected structure
            if "fdr" not in fixture:
                fixture["fdr"] = {
                    "overall": {
                        "home": {
                            "raw_score": 0,
                            "fdr": 0,
                            "category": "UNKNOWN",
                            "color": "#CCCCCC",
                            "xg": 0,
                            "original_xg": 0
                        },
                        "away": {
                            "raw_score": 0,
                            "fdr": 0,
                            "category": "UNKNOWN",
                            "color": "#CCCCCC",
                            "xg": 0,
                            "original_xg": 0
                        }
                    },
                    "components": {
                        "home": {
                            "attack_strength": 0,
                            "defense_weakness": 0,
                            "outright": 0.5,
                            "form": 0.5,
                            "historical": 0.5,
                            "availability": 0
                        },
                        "away": {
                            "attack_strength": 0,
                            "defense_weakness": 0,
                            "outright": 0.5,
                            "form": 0.5,
                            "historical": 0.5,
                            "availability": 0
                        }
                    }
                }
            
            # Extract league information
            league_data = extract_league_data(fixture)
            league_id = league_data.get("id")
            if league_id and league_id not in leagues:
                leagues[league_id] = {
                    "id": league_id,
                    "name": league_data.get("name", "Unknown")
                }
            
            # Create simplified fixture with only the requested fields
            simplified_fixture = {
                "home_team": home_team,
                "away_team": away_team,
                "fdr": fixture.get("fdr", {}),
                "starting_at": fixture.get("starting_at")
            }
            
            simplified_fixtures.append(simplified_fixture)
        
        # Return the simplified fixtures structure
        return jsonify({
            "count": len(simplified_fixtures),
            "fixtures": simplified_fixtures,
            "leagues": list(leagues.values()),
            "is_premier_league": is_premier_league(league_id)
        })
        
    except Exception as e:
        logger.error(f"Error fetching fixtures: {str(e)}")
        return jsonify({"error": str(e)}), 500



# was for testing purpose
# @app.route('/api/fixtures', methods=['GET'])
# def get_fixtures():
#     """Get fixtures with FDR data only"""
#     try:
#         league_id = request.args.get('league_id')
#         team_id = request.args.get('team_id')
#         # Try to convert team_id to int if it's a number string
#         if team_id and team_id.isdigit():
#             team_id = int(team_id)
        
#         if league_id and league_id.isdigit():
#             league_id = int(league_id)

#         # Default to Premier League if not specified
#         if not league_id:
#             league_id = PREMIER_LEAGUE_ID  # Already an int
        
#         # Log sample fixtures for diagnostics
#         # sample_fixtures = list(db.fixtures.find({}, {"_id": 0, "starting_at": 1}).limit(3))
#         # sample = db.fixtures.find({ fdr: { $exists: true } }).limit(5)
#         # logger.info(f"Sample fixtures in database: {sample_fixtures}")
        
#         # Base query: only fetch fixtures with FDR data
#         base_query = {
#             "fdr": {"$exists": True}
#         }
        
#         # Build additional filter conditions
#         and_conditions = []
        
#         # Add league filter with all possible field paths
#         if league_id:
#             and_conditions.append({
#                 "$or": [
#                     {"league_id": league_id},
#                     {"league.id": league_id},
#                     {"league.league_id": league_id}
#                 ]
#             })
        
#         # Add team filter with proper array handling
#         if team_id:
#             team_query = [
#                 {"participants.id": team_id},
#                 {"participants": {"$elemMatch": {"id": team_id}}},
#                 {"local_team.id": team_id},
#                 {"visitor_team.id": team_id}
#             ]
#             and_conditions.append({"$or": team_query})
        
#         # Combine base query with optional filters
#         final_query = base_query
#         if and_conditions:
#             final_query = {
#                 "$and": [base_query] + and_conditions
#             }
        
#         logger.debug(f"Final MongoDB query: {final_query}")
        
#         # Fetch matching fixtures
#         fixtures = list(db.fixtures.find(final_query, {"_id": 0}).sort("starting_at", 1))
#         logger.info(f"Found {len(fixtures)} fixtures with FDR data")
        
#         # Extract unique leagues
#         leagues = {}
#         for fixture in fixtures:
#             league_data = extract_league_data(fixture)
#             league_id = league_data.get("id")
#             if league_id and league_id not in leagues:
#                 leagues[league_id] = {
#                     "id": league_id,
#                     "name": league_data.get("name", "Unknown")
#                 }
        
#         return jsonify({
#             "count": len(fixtures),
#             "fixtures": fixtures,
#             "leagues": list(leagues.values()),
#             "is_premier_league": is_premier_league(league_id)
#         })
    
#     except Exception as e:
#         logger.error(f"Error fetching fixtures: {str(e)}")
#         return jsonify({"error": str(e)}), 500



@app.route('/api/fdr/team/<team_id>', methods=['GET'])
def get_team_fdr(team_id):
    """Get all FDR data for a specific team"""
    try:
        days = int(request.args.get('days', 60))
        include_averages = request.args.get('include_averages', 'false').lower() == 'true'
        start_date = datetime.now().strftime("%Y-%m-%d")
        
        # Convert team_id to integer if it's a string
        if isinstance(team_id, str) and team_id.isdigit():
            team_id = int(team_id)
        
        # Make query more flexible
        query = {
            "$or": [
                {"participants.id": team_id},
                {"local_team.id": team_id},
                {"visitor_team.id": team_id}
            ],
            "fdr": {"$exists": True}
        }
        
        # Add Premier League filter for EPL-specific data
        premier_league_query = query.copy()
        premier_league_query["$or"] = premier_league_query.get("$or", []) + [
            {"league_id": str(PREMIER_LEAGUE_ID)},
            {"league.id": str(PREMIER_LEAGUE_ID)}
        ]
        
        # Try Premier League specific query first
        fixtures = list(db.fixtures.find(premier_league_query, {"_id": 0}).sort("starting_at", 1))
        
        # If no Premier League fixtures, fall back to all leagues
        if not fixtures:
            fixtures = list(db.fixtures.find(query, {"_id": 0}).sort("starting_at", 1))
            is_premier_league = False
        else:
            is_premier_league = True
        
        logger.info(f"Found {len(fixtures)} fixtures with FDR data for team {team_id}")
        
        # Extract team info from the first fixture (if available)
        team_data = None
        if fixtures:
            team_data = get_team_data_from_fixture(fixtures[0], team_id)
        
        # Extract FDR data for each fixture
        team_fixtures = [extract_fdr_data(fixture, team_id) for fixture in fixtures]
        
        # Create response with extended team info
        response = {
            "team_id": team_id,
            "team_name": team_data.get("name", "Unknown") if team_data else "Unknown",
            "team_code": team_data.get("short_code") if team_data else None,
            "team_image": team_data.get("image_path") if team_data else None,
            "fixtures_count": len(team_fixtures),
            "fixtures": team_fixtures,
            "is_premier_league": is_premier_league
        }
        
        # Only include averages if requested
        if include_averages and team_fixtures:
            avg_fdr = sum(f["fdr"] for f in team_fixtures if f["fdr"] is not None) / len(team_fixtures) if team_fixtures else 0
            avg_attacking = sum(f["attacking_strength"] for f in team_fixtures if f["attacking_strength"] is not None) / len(team_fixtures) if team_fixtures else 0
            avg_defending = sum(f["defending_strength"] for f in team_fixtures if f["defending_strength"] is not None) / len(team_fixtures) if team_fixtures else 0
            
            response.update({
                "average_fdr": round(avg_fdr, 2),
                "average_attacking": round(avg_attacking, 2),
                "average_defending": round(avg_defending, 2)
            })
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error fetching team FDR: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/fdr/league/<league_id>', methods=['GET'])
def get_league_fdr(league_id):
    """Get FDR data for all teams in a league"""
    try:
        include_averages = request.args.get('include_averages', 'false').lower() == 'true'
        # Check if we're dealing with Premier League
        is_epl = is_premier_league(league_id)
        
        # Get upcoming fixtures with FDR data for this league
        fixtures_query = {
            "$or": [
                {"league_id": league_id},
                {"league.id": league_id}
            ],
            "starting_at": {"$gte": datetime.now().strftime("%Y-%m-%d")},
            "fdr": {"$exists": True}
        }
        
        fixtures = list(db.fixtures.find(fixtures_query, {"_id": 0}).sort("starting_at", 1))
        logger.info(f"Found {len(fixtures)} fixtures for league {league_id}")
        
        # Extract league info from the first fixture (if available)
        league_data = None
        if fixtures:
            league_data = extract_league_data(fixtures[0])
        
        # Extract unique teams from fixtures
        team_dict = {}  # Team ID -> Team Data
        for fixture in fixtures:
            if "participants" in fixture and isinstance(fixture["participants"], list):
                for participant in fixture["participants"]:
                    team_id = participant.get("id")
                    if team_id and team_id not in team_dict:
                        team_dict[team_id] = participant
            
            # Fallback to local_team/visitor_team
            if "local_team" in fixture:
                team_id = fixture["local_team"].get("id")
                if team_id and team_id not in team_dict:
                    team_dict[team_id] = fixture["local_team"]
            
            if "visitor_team" in fixture:
                team_id = fixture["visitor_team"].get("id")
                if team_id and team_id not in team_dict:
                    team_dict[team_id] = fixture["visitor_team"]
        
        logger.info(f"Extracted {len(team_dict)} unique teams from fixtures")
        
        # Process fixtures for each team
        team_fdr_data = []
        for team_id, team_info in team_dict.items():
            team_fixtures = []
            
            # Get fixtures for this team
            for fixture in fixtures:
                # Check if this team is in the fixture
                is_in_fixture = any(
                    (p.get("id") == team_id) 
                    for p in fixture.get("participants", []) 
                    if isinstance(p, dict)
                ) or fixture.get("local_team", {}).get("id") == team_id or fixture.get("visitor_team", {}).get("id") == team_id
                
                if is_in_fixture:
                    # Get FDR data for this team in this fixture
                    fdr_info = extract_fdr_data(fixture, team_id)
                    # Include all relevant FDR data for frontend display
                    team_fixtures.append({
                        "fixture_id": fdr_info["fixture_id"],
                        "date": fdr_info["date"],
                        "is_home": fdr_info["is_home"],
                        "opponent": fdr_info["opponent"],
                        "opponent_id": fdr_info["opponent_id"],
                        "fdr": fdr_info["fdr"],
                        "category": fdr_info["category"],
                        "fixture_name": fdr_info["fixture_name"],
                        "attacking_strength": fdr_info["attacking_strength"],
                        "defending_strength": fdr_info["defending_strength"]
                    })
            
            # Only include teams with fixtures
            if team_fixtures:
                team_data = {
                    "team_id": team_id,
                    "team_name": team_info.get("name", "Unknown"),
                    "team_code": team_info.get("short_code"),
                    "team_image": team_info.get("image_path"),
                    "next_fixtures": team_fixtures,
                    "fixtures_count": len(team_fixtures)
                }
                
                # Add average calculations only if requested
                if include_averages:
                    avg_fdr = sum(f["fdr"] for f in team_fixtures if f["fdr"] is not None) / len(team_fixtures) if team_fixtures else 0
                    team_data["average_fdr"] = round(avg_fdr, 2)
                
                team_fdr_data.append(team_data)
        
        # Sort teams by their next fixture date
        team_fdr_data.sort(
            key=lambda t: t["next_fixtures"][0]["date"] if t["next_fixtures"] else "9999-99-99"
        )
        
        # Prepare the full response
        response = {
            "league_id": league_id,
            "league_name": league_data.get("name", "Unknown League") if league_data else "Unknown League",
            "is_premier_league": is_epl,
            "teams_count": len(team_fdr_data),
            "teams": team_fdr_data,
            "fixture_count": len(fixtures)
        }
        
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Error fetching league FDR: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/fdr/premier-league', methods=['GET'])
def get_premier_league_fdr():
    """Dedicated endpoint for Premier League FDR data"""
    return get_league_fdr(PREMIER_LEAGUE_ID)

@app.route('/api/teams', methods=['GET'])
def get_teams():
    """Get all teams with FDR data"""
    try:
        league_id = request.args.get('league_id')
        
        # Start by finding fixtures with FDR data
        query = {
            "starting_at": {"$gte": datetime.now().strftime("%Y-%m-%d")},
            "fdr": {"$exists": True}
        }
        
        # Add league filter if provided
        if league_id:
            query["$or"] = [
                {"league_id": league_id},
                {"league.id": league_id}
            ]
        
        fixtures = list(db.fixtures.find(query, {"_id": 0}))
        
        # Extract unique teams from fixtures
        teams = {}
        for fixture in fixtures:
            # Process participants
            if "participants" in fixture and isinstance(fixture["participants"], list):
                for participant in fixture["participants"]:
                    team_id = participant.get("id")
                    if team_id and team_id not in teams:
                        league_data = extract_league_data(fixture)
                        teams[team_id] = {
                            "id": team_id,
                            "name": participant.get("name", "Unknown"),
                            "code": participant.get("short_code"),
                            "image": participant.get("image_path"),
                            "league_id": league_data.get("id"),
                            "league_name": league_data.get("name")
                        }
        
        return jsonify({
            "count": len(teams),
            "teams": list(teams.values())
        })
        
    except Exception as e:
        logger.error(f"Error fetching teams: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/leagues', methods=['GET'])
def get_leagues():
    """Get all leagues with FDR data"""
    try:
        # Find fixtures with FDR data
        query = {
            "starting_at": {"$gte": datetime.now().strftime("%Y-%m-%d")},
            "fdr": {"$exists": True}
        }
        
        fixtures = list(db.fixtures.find(query, {"_id": 0}))
        
        # Extract unique leagues from fixtures
        leagues = {}
        for fixture in fixtures:
            league_data = extract_league_data(fixture)
            league_id = league_data.get("id")
            if league_id and league_id not in leagues:
                leagues[league_id] = {
                    "id": league_id,
                    "name": league_data.get("name", "Unknown"),
                    "country": league_data.get("country", {}).get("name") if "country" in league_data else None,
                    "is_premier_league": is_premier_league(league_id)
                }
        
        # Sort leagues (Premier League first, then alphabetically)
        sorted_leagues = sorted(
            list(leagues.values()),
            key=lambda x: (0 if x["is_premier_league"] else 1, x["name"])
        )
        
        return jsonify({
            "count": len(sorted_leagues),
            "leagues": sorted_leagues
        })
        
    except Exception as e:
        logger.error(f"Error fetching leagues: {str(e)}")
        return jsonify({"error": str(e)}), 500

# ----------------- Background Jobs -----------------

def run_data_collection_job():
    """Run the FDR data collection process"""
    try:
        logger.info("Starting scheduled FDR data collection job")
        
        # Get API tokens from environment variables
        sportmonks_token = os.getenv("SPORTMONKS_API_KEY")
        goalserve_token = os.getenv("GOALSERVE_API_KEY", "0f6230689b674453eee508dd50f5b2ca")
        
        # Run data collection with focus on Premier League
        collector = FDRDataCollector(sportmonks_token, goalserve_token)
        collector.collect_all_fdr_data(major_league_ids=[PREMIER_LEAGUE_ID])
        
        logger.info("FDR data collection job completed")
    except Exception as e:
        logger.error(f"Error in data collection job: {str(e)}")

def run_fdr_calculation_job():
    """Run the FDR calculation process"""
    try:
        logger.info("Starting scheduled FDR calculation job")
        
        # Initialize calculator and run comprehensive calculation
        calculator = FDRCalculator()
        calculator.comprehensive_fdr_calculation()
        
        logger.info("FDR calculation job completed")
    except Exception as e:
        logger.error(f"Error in FDR calculation job: {str(e)}")

# Initialize scheduler
scheduler = BackgroundScheduler()

# Schedule data collection job (runs daily at 1:00 AM)
scheduler.add_job(
    func=run_data_collection_job,
    trigger=CronTrigger(hour=1, minute=0),
    id='data_collection_job',
    name='Daily FDR Data Collection',
    replace_existing=True
)

# Schedule FDR calculation job (runs daily at 2:00 AM)
scheduler.add_job(
    func=run_fdr_calculation_job,
    trigger=CronTrigger(hour=2, minute=0),
    id='fdr_calculation_job',
    name='Daily FDR Calculation',
    replace_existing=True
)

# Start the scheduler
scheduler.start()

# Register shutdown
atexit.register(lambda: scheduler.shutdown())

# Run the application
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)), debug=False)