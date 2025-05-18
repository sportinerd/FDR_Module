import xml.etree.ElementTree as ET
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import requests

def debug_goalserve_data_collection():
    # MongoDB Connection
    mongo_uri = "mongodb+srv://naymul504:soupnaymul09@pf365.2pguj.mongodb.net/?retryWrites=true&w=majority&appName=pf365"
    client = MongoClient(mongo_uri, server_api=ServerApi('1'))
    db = client['Analytics']

    # 1. Check Premier League coverage in existing data
    print("\n=== EXISTING PREMIER LEAGUE DATA ===")
    prem_league_fixtures = db.fixtureOdds.find({
        "category_name": {"$regex": "Premier League", "$options": "i"}
    })
    # print(f"Found {prem_league_fixtures.countDocuments()} Premier League fixtures in MongoDB")
    
    # 2. Fetch raw data from GoalServe
    print("\n=== FETCHING RAW GOALSERVE DATA ===")
    goalserve_token = "0f6230689b674453eee508dd50f5b2ca"  # From your code
    url = f"http://www.goalserve.com/getfeed/{goalserve_token}/getodds/soccer?cat=soccer_10"  # Changed to soccer_1
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open("goalserve_raw.xml", "w") as f:
            f.write(response.text)
        print("Saved raw XML to goalserve_raw.xml")
    except Exception as e:
        print(f"Error fetching GoalServe data: {str(e)}")
        return

    # 3. Parse and analyze XML
    print("\n=== ANALYZING GOALSERVE DATA ===")
    try:
        root = ET.fromstring(response.text)
        match_count = 0
        arsenal_found = False

        for category in root.findall('category'):
            if "Premier League" in category.get('name', ''):
                print(f"\nFound Premier League category: {category.get('name')}")
                matches = category.find('matches')
                
                if matches is not None:
                    for match in matches.findall('match'):
                        match_count += 1
                        home = match.find('localteam').get('name', '')
                        away = match.find('visitorteam').get('name', '')
                        
                        # Look for Arsenal matches
                        if "Arsenal" in home or "Arsenal" in away:
                            arsenal_found = True
                            print(f"\nArsenal match found:")
                            print(f"Match ID: {match.get('id')}")
                            print(f"Date: {match.get('date')} {match.get('time')}")
                            print(f"Teams: {home} vs {away}")
                            print("Odds Data:")
                            odds = match.find('odds')
                            if odds is not None:
                                for odd_type in odds.findall('type'):
                                    print(f"  {odd_type.get('value')} odds available")

        print(f"\nTotal Premier League matches found: {match_count}")
        print(f"Arsenal matches found: {arsenal_found}")

    except ET.ParseError as e:
        print(f"XML parsing error: {str(e)}")

    # 4. Check MongoDB storage
    print("\n=== MONGODB STORAGE VERIFICATION ===")
    if 'fixtureOdds' in db.list_collection_names():
        print("FixtureOdds collection structure:")
        sample = db.fixtureOdds.find_one()
        print("Keys:", sample.keys() if sample else "No documents")
    else:
        print("FixtureOdds collection does not exist")

if __name__ == "__main__":
    debug_goalserve_data_collection()