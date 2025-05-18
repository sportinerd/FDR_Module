from pymongo import MongoClient
import subprocess
subprocess.run(["pip", "install", "pymongo"])  # Ensure pymongo is installed
subprocess.run(["pip", "install", "tabulate"])  # Ensure tabulate is installed
from tabulate import tabulate  # pip install tabulate
from pymongo.server_api import ServerApi
# Connect to your database
client = MongoClient("mongodb+srv://naymul504:soupnaymul09@pf365.2pguj.mongodb.net/?retryWrites=true&w=majority&appName=pf365", server_api=ServerApi('1'))
db = client["Analytics"]

# Function to get and display odds for a market
def display_outright_odds(categories, markets):
    """Display outright odds for specified categories and markets"""
    
    for category in categories:
        print(f"\n=== {category} Outright Odds ===")
        
        for market in markets:
            # First check if this market exists for this category
            pipeline = [
                {"$match": {"category": {"$regex": category, "$options": "i"}, 
                           "market": {"$regex": market, "$options": "i"}}},
                {"$unwind": "$odds"},
                {"$group": {"_id": "$team", 
                           "team_id": {"$first": "$team_id"},
                           "best_odd": {"$min": "$odds.odd"}}},
                {"$sort": {"best_odd": 1}}
            ]
            
            results = list(db.outrightOdds.aggregate(pipeline))
            
            if results:
                # Format data for tabulate
                table_data = []
                for team in results:
                    odd_value = team['best_odd']
                    # Format American odds (positive with +, negative as is)
                    formatted_odd = f"+{int(odd_value)}" if odd_value > 0 else str(int(odd_value))
                    table_data.append([team["_id"], formatted_odd])
                
                print(f"\n{category} | {market}")
                print(tabulate(table_data, headers=["Team", "Best Odds"], tablefmt="pretty"))
            else:
                print(f"\nNo odds found for {category} | {market}")

# Categories and markets to search for
categories = [
    "Premier League", 
    "EPL",
    "English Premier", 
    "England: Premier League"
]

markets = [
    "Championship Winner", 
    "Winner", 
    "Outright Winner",
    "Top 4 Finish",
    "Relegation"
]

# Display the odds
display_outright_odds(categories, markets)