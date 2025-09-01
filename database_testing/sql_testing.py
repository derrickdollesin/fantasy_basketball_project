import sqlite3
import sys

sys.path.append(r'C:\Users\DerDo\Desktop\fantasy_basketball_project')
from functions import *

conn = sqlite3.connect("nba.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE if not exists conference_data (
    team_name TEXT PRIMARY KEY,
    W TEXT,
    L TEXT,
    WLperc TEXT,
    GB TEXT,
    PSG TEXT,
    PAG TEXT,
    SRS TEXT
);
""")

conn.commit()
conn.close()

import sqlite3
import pandas as pd

# # Example DataFrame
# df = pd.DataFrame({
#     "name": ["LeBron James", "Stephen Curry", "Nikola Jokic"],
#     "team": ["LAL", "GSW", "DEN"],
#     "id": [24, 30, 25]
# })

# # Connect to SQLite DB (creates table if not exists)
# with sqlite3.connect("nba.db") as conn:
#     # Write DataFrame to a table called "players"
#     df.to_sql("players", conn, if_exists="append", index=False)

#     # Verify by reading back
#     check = pd.read_sql("SELECT * FROM players", conn)

# print(check)

### Get team data
okc = get_roster('OKC', '2026')
hou = get_roster('HOU', '2026')

### Get conference data
conference_data = get_team_data('2025')
east_df = conference_data[1]
west_df = conference_data[0]

east_df = east_df.rename(columns={'Eastern Conference': 'team_name'})
east_df.columns =[col.replace('/', '') if '/' in col else col.replace("%", 'perc') if '%' in col else col for col in east_df.columns]
west_df = west_df.rename(columns={'Western Conference': 'team_name'})
west_df.columns = [col.replace('/', '') if '/' in col else col.replace("%", 'perc') if '%' in col else  col for col in west_df.columns]

east_df = east_df.rename(columns={'WL%':'WLperc'})
west_df = west_df.rename(columns={'WL%':'WLperc', 'Eastern Conference': 'team_name'})

print(west_df)
# Connect to SQLite DB (creates table if not exists)
with sqlite3.connect("nba.db") as conn:
    # Write DataFrame to a table called "players"
    # east_df.to_sql("conference_data", conn, if_exists="append", index=False)
    west_df.to_sql("conference_data", conn, if_exists="append", index=False)

    # Verify by reading back
    check = pd.read_sql("SELECT * FROM conference_data", conn)

print(check)