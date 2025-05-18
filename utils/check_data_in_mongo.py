# import pymongo
# from pymongo import MongoClient
# import pprint
# import os
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

# def analyze_fixture_components():
#     """Analyze FDR components for a specific fixture"""
#     # MongoDB connection
#     mongo_uri = "mongodb+srv://naymul504:soupnaymul09@pf365.2pguj.mongodb.net/?retryWrites=true&w=majority&appName=pf365"
#     client = MongoClient(mongo_uri)
#     db = client['Analytics']  # Your database name
    
#     print("Connecting to MongoDB...")
    
#     # Step 1: Find the fixture
#     fixture = db.fixtures.find_one({
#         "participants": {
#             "$elemMatch": {"name": "Nottingham Forest"}
#         },
#         "participants": {
#             "$elemMatch": {"name": "Leicester City"}
#         }
#     })
    
#     if not fixture:
#         print("Fixture not found. Trying alternative search...")
#         # Try a more flexible search
#         fixtures = list(db.fixtures.find({
#             "$or": [
#                 {"name": {"$regex": "Nottingham.*Leicester", "$options": "i"}},
#                 {"participants.name": {"$regex": "Nottingham", "$options": "i"}}
#             ]
#         }))
        
#         if fixtures:
#             print(f"Found {len(fixtures)} potential matches.")
#             for i, fix in enumerate(fixtures):
#                 participants = []
#                 if "participants" in fix:
#                     for p in fix["participants"]:
#                         participants.append(p.get("name", "Unknown"))
                
#                 print(f"{i+1}. {fix.get('name', 'Unknown')} - {participants}")
            
#             # Use the first match for analysis
#             fixture = fixtures[0]
#         else:
#             print("No fixtures found matching Nottingham Forest vs Leicester City")
#             return
    
#     # Step 2: Extract FDR components
#     print("\n=== FIXTURE DETAILS ===")
#     if "name" in fixture:
#         print(f"Fixture: {fixture['name']}")
#     else:
#         team_names = []
#         if "participants" in fixture:
#             for p in fixture["participants"]:
#                 team_names.append(p.get("name", "Unknown"))
#         print(f"Fixture: {' vs '.join(team_names)}")
    
#     if "fdr" not in fixture:
#         print("No FDR data found for this fixture")
#         return
    
#     # Print FDR values and categories
#     print("\n=== FDR RATINGS ===")
#     print(f"Home FDR: {fixture['fdr']['overall']['home']['fdr']:.2f} ({fixture['fdr']['overall']['home']['category']})")
#     print(f"Away FDR: {fixture['fdr']['overall']['away']['fdr']:.2f} ({fixture['fdr']['overall']['away']['category']})")
    
#     # Print component values
#     print("\n=== FDR COMPONENTS ===")
#     if "components" in fixture["fdr"]:
#         home_components = fixture["fdr"]["components"]["home"]
#         away_components = fixture["fdr"]["components"]["away"]
        
#         print("Home team components:")
#         print(f"  Historical: {home_components.get('historical', 'N/A'):.3f}")
#         print(f"  Form: {home_components.get('form', 'N/A'):.3f}")
#         print(f"  Outright: {home_components.get('outright', 'N/A'):.3f}")
#         print(f"  Odds: {home_components.get('odds', 'N/A'):.3f}")
#         print(f"  Availability: {home_components.get('availability', 'N/A'):.3f}")
        
#         print("\nAway team components:")
#         print(f"  Historical: {away_components.get('historical', 'N/A'):.3f}")
#         print(f"  Form: {away_components.get('form', 'N/A'):.3f}")
#         print(f"  Outright: {away_components.get('outright', 'N/A'):.3f}")
#         print(f"  Odds: {away_components.get('odds', 'N/A'):.3f}")
#         print(f"  Availability: {away_components.get('availability', 'N/A'):.3f}")
#     else:
#         print("Component details not available")
    
#     # Step 3: Look up raw betting odds
#     print("\n=== RAW BETTING ODDS ===")
#     fixture_id = fixture.get("id") or fixture.get("sportmonks_id")
    
#     if fixture_id:
#         # Look for GoalServe odds
#         goalserve_odds = db.fixtureOdds.find_one({
#             "$or": [
#                 {"local_team.name": {"$regex": "Nottingham", "$options": "i"}, 
#                  "visitor_team.name": {"$regex": "Leicester", "$options": "i"}},
#                 {"local_team.name": {"$regex": "Leicester", "$options": "i"}, 
#                  "visitor_team.name": {"$regex": "Nottingham", "$options": "i"}}
#             ]
#         })
        
#         if goalserve_odds:
#             print("GoalServe odds found:")
#             for odds_type in goalserve_odds.get("odds", []):
#                 if odds_type.get("type_value") == "Match Winner":
#                     print(f"  Market: {odds_type.get('type_value')}")
                    
#                     for bookmaker in odds_type.get("bookmakers", []):
#                         if "home_odd" in bookmaker and "draw_odd" in bookmaker and "away_odd" in bookmaker:
#                             print(f"  Bookmaker: {bookmaker.get('bookmaker_name')}")
#                             print(f"    Home: {bookmaker.get('home_odd')}")
#                             print(f"    Draw: {bookmaker.get('draw_odd')}")
#                             print(f"    Away: {bookmaker.get('away_odd')}")
#                             break
#         else:
#             print("No GoalServe odds found")
        
#         # Look for Sportmonks odds
#         sportmonks_odds = list(db.sportmonksPrematchOdds.find({
#             "fixture_id": fixture_id,
#         }).limit(5))
        
#         if sportmonks_odds:
#             print("\nSportmonks odds found:")
#             home_odds = []
#             draw_odds = []
#             away_odds = []
            
#             for odd in sportmonks_odds:
#                 if odd.get("market_id") == 1:  # Match Winner market
#                     if odd.get("label") == "1":  # Home win
#                         home_odds.append(float(odd.get("value", 0)))
#                     elif odd.get("label") == "X":  # Draw
#                         draw_odds.append(float(odd.get("value", 0)))
#                     elif odd.get("label") == "2":  # Away win
#                         away_odds.append(float(odd.get("value", 0)))
            
#             if home_odds and draw_odds and away_odds:
#                 print(f"  Average Home Win: {sum(home_odds)/len(home_odds):.2f}")
#                 print(f"  Average Draw: {sum(draw_odds)/len(draw_odds):.2f}")
#                 print(f"  Average Away Win: {sum(away_odds)/len(away_odds):.2f}")
#             else:
#                 print("  Incomplete odds data")
#         else:
#             print("No Sportmonks odds found")
#     else:
#         print("Fixture ID not available - cannot lookup odds")

#     print("\n=== CALCULATING WEIGHTED FDR ===")
#     print("Current weights: Odds 40%, Form 20%, Historical 20%, Outright 15%, Availability 5%")
    
#     if "components" in fixture["fdr"]:
#         home_weighted = (
#             0.20 * home_components.get('historical', 0) +
#             0.20 * home_components.get('form', 0) +
#             0.15 * home_components.get('outright', 0) +
#             0.40 * home_components.get('odds', 0) +
#             0.05 * home_components.get('availability', 0)
#         )
        
#         away_weighted = (
#             0.20 * away_components.get('historical', 0) +
#             0.20 * away_components.get('form', 0) +
#             0.15 * away_components.get('outright', 0) +
#             0.40 * away_components.get('odds', 0) +
#             0.05 * away_components.get('availability', 0)
#         )
        
#         print(f"Home weighted raw score: {home_weighted:.3f}")
#         print(f"Away weighted raw score: {away_weighted:.3f}")
        
#         # Impact analysis - which components are causing similar scores
#         print("\n=== COMPONENT IMPACT ANALYSIS ===")
#         components = ['historical', 'form', 'outright', 'odds', 'availability']
#         weights = [0.20, 0.20, 0.15, 0.40, 0.05]
        
#         for i, comp in enumerate(components):
#             h_val = home_components.get(comp, 0) 
#             a_val = away_components.get(comp, 0)
#             weight = weights[i]
            
#             h_contribution = h_val * weight
#             a_contribution = a_val * weight
            
#             diff = abs(h_contribution - a_contribution)
#             print(f"{comp.capitalize()} impact differential: {diff:.4f}")

# if __name__ == "__main__":
#     analyze_fixture_components()


# import pymongo
# from pymongo import MongoClient
# from pymongo.server_api import ServerApi

# def test_odds_calculation():
#     """Test the updated odds calculation with Nottingham vs Leicester data"""
    
#     # Connect to MongoDB
#     mongo_uri = "mongodb+srv://naymul504:soupnaymul09@pf365.2pguj.mongodb.net/?retryWrites=true&w=majority&appName=pf365"
#     client = MongoClient(mongo_uri, server_api=ServerApi('1'))
#     db = client['Analytics']
    
#     print("Looking for Nottingham Forest vs Leicester City fixture...")
    
#     # Find the specific fixture
#     fixtures = list(db.fixtures.find({
#         "$or": [
#             {"name": {"$regex": "Nottingham.*Leicester", "$options": "i"}},
#             {"participants.name": {"$regex": "Nottingham", "$options": "i"}}
#         ]
#     }))
    
#     if not fixtures:
#         print("Fixture not found. Using sample data instead.")
#         # Use sample data from previous analysis
#         test_odds = {
#             "odds": [
#                 {
#                     "type_value": "Match Winner",
#                     "bookmakers": [
#                         {
#                             "bookmaker_name": "10Bet",
#                             "home_odd": 1.38,
#                             "draw_odd": 4.9,
#                             "away_odd": 7.6
#                         }
#                     ]
#                 }
#             ]
#         }
        
#         # Extract and process sample odds
#         home_odds = []
#         draw_odds = []
#         away_odds = []
        
#         for odds_type in test_odds.get("odds", []):
#             if odds_type.get("type_value") == "Match Winner":
#                 for bookmaker in odds_type.get("bookmakers", []):
#                     home_odds.append(bookmaker["home_odd"])
#                     draw_odds.append(bookmaker["draw_odd"])
#                     away_odds.append(bookmaker["away_odd"])
#     else:
#         fixture_id = fixtures[0].get("id")
#         print(f"Found fixture with ID: {fixture_id}")
        
#         # Get odds data
#         goalserve_odds = db.fixtureOdds.find_one({"match_id": str(fixture_id)})
        
#         if not goalserve_odds:
#             print("No odds data found. Using sample data.")
#             # Use the same sample data as above
#             # (Code would be identical to the sample data section above)
#         else:
#             print("Found odds data:")
#             home_odds = []
#             draw_odds = []
#             away_odds = []
            
#             for odds_type in goalserve_odds.get("odds", []):
#                 if odds_type.get("type_value") == "Match Winner":
#                     for bookmaker in odds_type.get("bookmakers", []):
#                         if "home_odd" in bookmaker and "draw_odd" in bookmaker and "away_odd" in bookmaker:
#                             home_odds.append(bookmaker["home_odd"])
#                             draw_odds.append(bookmaker["draw_odd"])
#                             away_odds.append(bookmaker["away_odd"])
    
#     # Calculate average odds
#     avg_home_odd = sum(home_odds) / len(home_odds)
#     avg_draw_odd = sum(draw_odds) / len(draw_odds)
#     avg_away_odd = sum(away_odds) / len(away_odds)
    
#     print(f"Raw odds - Home: {avg_home_odd}, Draw: {avg_draw_odd}, Away: {avg_away_odd}")
    
#     # Convert to probabilities
#     p_home = 1 / avg_home_odd
#     p_draw = 1 / avg_draw_odd
#     p_away = 1 / avg_away_odd
    
#     # Normalize probabilities to sum to 1
#     total = p_home + p_draw + p_away
#     p_home /= total
#     p_draw /= total
#     p_away /= total
    
#     print(f"Normalized probabilities - Home: {p_home:.3f}, Draw: {p_draw:.3f}, Away: {p_away:.3f}")
    
#     # Calculate difficulty scores
#     home_difficulty = 1 - p_home - (0.5 * p_draw)
#     away_difficulty = 1 - p_away - (0.5 * p_draw)
    
#     print(f"Difficulty scores - Home: {home_difficulty:.3f}, Away: {away_difficulty:.3f}")
    
#     # Calculate FDR (scaled 0-10)
#     home_fdr = home_difficulty * 10
#     away_fdr = away_difficulty * 10
#     print(f"FDR (0-10 scale) - Home: {home_fdr:.1f}, Away: {away_fdr:.1f}")
    
#     return home_difficulty, away_difficulty

# # Run the test
# test_odds_calculation()
