from pymongo import MongoClient
from pymongo.server_api import ServerApi
# Connect to MongoDB
mongo_uri = "mongodb+srv://naymul504:soupnaymul09@pf365.2pguj.mongodb.net/?retryWrites=true&w=majority&appName=pf365"
client = MongoClient(mongo_uri, server_api=ServerApi('1'), serverSelectionTimeoutMS=5000)
db = client['Analytics']

# Check total documents in fixtureOdds collection
total_odds = db.fixtureOdds.count_documents({})
print(f"Total fixtureOdds documents: {total_odds}")

# Check if this specific fixture exists in fixtureOdds
arsenal_newcastle = db.fixtureOdds.find_one({"match_id": "19135029"})
print(f"Found Arsenal vs Newcastle in fixtureOdds: {arsenal_newcastle is not None}")

# The key issue appears to be the match_id format
# Your sample shows match_id as string "6082369" but code might be searching for int or different format
# Try with string conversion
arsenal_newcastle_str = db.fixtureOdds.find_one({"match_id": str(19135029)})
print(f"Found with string conversion: {arsenal_newcastle_str is not None}")

# Check Sportmonks odds collections
sportmonks_odds = db.sportmonksPrematchOdds.count_documents({"fixture_id": 19135029})
print(f"Sportmonks prematch odds for fixture: {sportmonks_odds}")

# Check if there are any odds at all for this fixture across collections
any_odds = list(db.sportmonksPrematchOdds.find({"fixture_id": 19135029}).limit(1))
print(f"Any Sportmonks odds found: {len(any_odds) > 0}")
