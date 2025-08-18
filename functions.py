import re
import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import urllib.request
import time
import random

# --------------------------------------------------------------------------- #
# get_soup(url)
# get_roster(team_name, season)
# get_game_data(team, opp, url)
# get_player_data(name, season)
# fantasy_score_model(player_data)
# --------------------------------------------------------------------------- #

def get_soup(url):
    '''
    Create a soup object of a web url

    Args: 
        url - String (ex. https://basketball-reference.com/)

    Returns:
        soup - BeautifulSoup object
    '''
    try:
        with urllib.request.urlopen(url) as response:
            html = response.read()
    except urllib.error.URLError as e:
        print(f"Error fetching URL: {e.reason}")

    ### create the soup object
    soup = BeautifulSoup(html, 'html.parser')

    return soup

# --------------------------------------------------------------------------- #

def get_roster(team_name, season):
    '''
    Retrieve the roster for a certain season

    Args:
        team_name - String (ex. CHI, BOS, PHI)
        season - String (ex. 2025, 2024)

    Returns:
        roster - Dataframe
    '''
    ### call get_html to request data from url
    soup = get_soup(f'https://www.basketball-reference.com/teams/{team_name}/{season}.html')

    ### extract player names from basketball-reference
    cells = soup.select('td[data-stat="player"]')
    players = [c.get_text(strip=True) for c in cells]

    ### extract player position from basketball-reference
    cells = soup.select('td[data-stat="pos"][csk="1"], '
                        'td[data-stat="pos"][csk="2"], '
                        'td[data-stat="pos"][csk="3"], '
                        'td[data-stat="pos"][csk="4"], '
                        'td[data-stat="pos"][csk="5"]')
    pos = [c.get_text(strip=True) for c in cells]

    ### build roster
    roster = pd.DataFrame({
        'Player_NAME': players,
        'Player_POSITION': pos
    })

    ### add team name 
    roster['Player_TEAM'] = team_name

    return roster

# --------------------------------------------------------------------------- #

def get_game_data(team, opp, url):
    '''
    Scrape for the game data given a certain team 

    Args:
        team - String (ex. SAC, LAC)
        opp - String (ex. SAC, OKC)
        url - String

    Returns:
        team_df
        opp_df
    '''
    ### create a second url based on the opposing team name
    other_url = url
    other_url = other_url.replace(team, opp)

    ### attempt to scrape the site (try not to get ip banned)
    for attempt in range(1, 3):
        time.sleep(0.5 + random.random() * 0.5) # sleep to avoid being banned
                                                # if the first url didn't work
        
        try: # attempt original url
            soup = get_soup(url)
            print(f'Success\n\n') # print if first was a success
            break 

        except Exception as ex: # try second url
            print(f'Attempt {attempt} failed on: {url}\n')
            print(f'Attemping reconnection at: {other_url}\n\n')

            sleep_s = 0.8 * (2 ** (attempt - 1)) + random.random()
            time.sleep(sleep_s)
            soup = get_soup(other_url)
            break

    ### scrape for basic data table
    team_table = soup.select(f'table[id="box-{team}-game-basic"]')
    opp_table = soup.select(f'table[id="box-{opp}-game-basic"]')

    ### helper to create the dataframe
    def parse_box_table(table):
        rows = []
        for tr in table.select("tbody tr"):
            row_data = {}
            for cell in tr.select("th[data-stat], td[data-stat]"):
                key = cell["data-stat"]
                val = cell.get_text(strip=True)
                row_data[key] = val
            if row_data['player'] == 'Reserves':
                continue
            rows.append(row_data)
        return rows

    ### Pick the first matching table 
    team_rows = parse_box_table(team_table[0])
    opp_rows  = parse_box_table(opp_table[0])

    ### Convert to dataframe
    team_df = pd.DataFrame(team_rows)
    opp_df = pd.DataFrame(opp_rows)

    return team_df, opp_df

# --------------------------------------------------------------------------- #

def get_player_data(name, season):
    '''
    Get player data 

    Args:
        name - String (ex jamesle01 or murrake02)
        season - String (ex 2025) # 2025 means 2024-2025 season
    '''
    url = f'https://www.basketball-reference.com/players/{name[0]}/{name}/gamelog/{season}/'
    soup = get_soup(url)

    ### parse html for table data (first 7 rows are useless)
    table = soup.find_all('td', class_=['center', 'left', 'right'])[7:]

    ### extract the data from the td tags
    data_list = []
    for i in table:
        data = i.get_text(strip=True)

        ### account for rows of inactive games
        if data.lower() in ['inactive', 'did not dress', 'did not play']:
            data_list.append(data)
            for _ in range(25):
                data_list.append('')
        else:
            data_list.append(data)

    ### format data for dataframe
    rows = []
    BATCH_SIZE = 33 # 33 columns 
    for i in range(0, len(data_list), BATCH_SIZE):
        curr_data = data_list[i: i+BATCH_SIZE] # row data

        rows.append(curr_data)

    ### set column names
    columns = [
        'Gcar','Gtm','Date','Team','at','Opp','Result','GS','MP','FG','FGA',
        'FG%','3P','3PA','3P%','2P','2PA','2P%','eFG%','FT','FTA','FT%','ORB',
        'DRB','TRB','AST','STL','BLK','TOV','PF','PTS','GmSc','+/-'
    ]

    ### create the frame
    data = pd.DataFrame(rows, columns=columns)

    ### separate the final data tables
    totals = data[
        (data['Gcar'] == '') & 
        (data['Gtm'] == '') & 
        (data['Date'] == '') & 
        (data['Team'] == '') & 
        (data['at'] == '') & 
        (data['Opp'] == '')
    ].drop(columns=['Gcar', 'Gtm', 'Date', 'Team', 'at', 'Opp'])
    reg_season = data.iloc[:totals.index[0]] 

    ### calculate games missed and played
    missed_reg_seas_played = reg_season[reg_season['GS'] != '*'] ### this ###
    total_reg_seas_missed_games = len(missed_reg_seas_played)
    reg_seas_played = reg_season[reg_season['GS'] == '*'] ### this ###

    ### because 3p are 0, their percentage is also gonna be 0 or not reported
    ### therefore fill with 0 for any col of %
    reg_seas_played[reg_seas_played['3P%'] == '']['3P']
    reg_seas_played[reg_seas_played['3P'] == '0'].shape

    for col in [c for c in reg_seas_played if '%' in c]:
        for row in reg_seas_played.index:
            if reg_seas_played.at[row, col] == '':
                reg_seas_played.at[row, col] = 0

    reg_seas_played = reg_seas_played.drop(columns=['at', 'GS'])

    reg_seas_played['Date'] = pd.to_datetime(reg_seas_played['Date']).dt.strftime('%Y%m%d')

    result = [1 if r[0] == 'W' else 0 for r in reg_seas_played['Result']]
    team_score = [
        re.findall(
            r'(?<= )\d+(?=\s*-\s*)', 
            reg_seas_played['Result'][i])[0] for i in reg_seas_played['Result'].index
    ]
    opp_score = [
        re.findall(
            r'(?<=-)\s*\d+', 
            reg_seas_played['Result'][i])[0] for i in reg_seas_played['Result'].index
        ]

    reg_seas_played['Result'] = result
    reg_seas_played['Team Score'] = team_score
    reg_seas_played['Opp Score'] = opp_score

    ### convert total mins played to sec
    def time_to_secs(time_str):
        minutes, seconds = map(int, time_str.split(":"))
        total_seconds = minutes * 60 + seconds  

        return total_seconds

    reg_seas_played['MP'] = reg_seas_played['MP'].apply(time_to_secs)

    ### calculate what percent of the score was attributed by player
    reg_seas_played['Percent Score'] = round(
        reg_seas_played['PTS'].astype(int) / reg_seas_played['Team Score'].astype(int), 
        2
    )

    ### convert the data to float values
    to_convert = [
        'FG','FGA','FG%','3P','3PA','3P%','2P','2PA','2P%','eFG%','FT','FTA',
        'FT%','ORB','DRB','TRB','AST','STL','BLK','TOV','PF','PTS','GmSc',
        '+/-', 'Team Score', 'Opp Score'
    ]
    reg_seas_played[to_convert] = reg_seas_played[to_convert].astype(float)

    urls = []
    for row in reg_seas_played.index:
        curr_data = reg_seas_played.loc[row, :]
        curr_team = curr_data['Team']
        curr_game_date = curr_data['Date']
        game_url = f'https://www.basketball-reference.com/boxscores/{curr_game_date}0{curr_team}.html'

        urls.append(game_url)

    reg_seas_played['URL'] = urls

    data = soup.select('span[itemprop="name"]')

    names = []
    for i in data:
        names.append(i.get_text(strip=True))

    name = names[3]

    reg_seas_played['Player_NAME'] = name

    ### ERROR: WILL TIMEOUT REQUESTS ###

    # team_start = []
    # opp_start = []
    # start = []

    # reg_seas_played = reg_seas_played.sort_values('Date', ascending=False)
    # last_game_played = reg_seas_played.iloc[0]

    # team = last_game_played['Team']
    # opp = last_game_played['Opp']
    # url = last_game_played['URL']

    # game_data = get_game_data(team, opp, url)

    # team_starters = list(game_data[0].head()['player'])
    # opp_starters = list(game_data[1].head()['player'])

    # did_start = name in team_starters


    # reg_seas_played['team_starting_five'] = team_start
    # reg_seas_played['opp_starting_five'] = opp_start
    # reg_seas_played['starter'] = start

    ### ERROR: WILL TIMEOUT REQUESTS ###

    ### Add opp as dummy variable
    nba_teams = [
        "ATL", "BOS", "BRK", "CHI", "CLE", "DAL", "DEN", "DET", 
        "GSW", "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", 
        "MIN", "NOP", "NYK", "OKC", "ORL", "PHI", "PHO", "POR", 
        "SAC", "SAS", "TOR", "UTA", "WAS", "CHO"
    ]

    for team in nba_teams:
        reg_seas_played[team] = 0

    curr_team = reg_seas_played['Team'].iloc[0]
    reg_seas_played = reg_seas_played.drop(columns=curr_team)

    for row in reg_seas_played.index:
        curr_data = reg_seas_played.loc[row]
        opp_team = curr_data['Opp']
        reg_seas_played.at[row, opp_team] = 1
    
    reg_seas_played = (
        reg_seas_played
        .drop(
            columns=['Gtm', 'Gcar']
        )
        .reset_index(drop=True)
    )

    # Base stats
    rebounds = reg_seas_played['TRB']
    assists = reg_seas_played['AST']
    points = reg_seas_played['PTS'] * 0.5
    turnovers = reg_seas_played['TOV'] * -1
    steals = reg_seas_played['STL']
    blocks = reg_seas_played['BLK']

    # Double/Triple doubles
    cats = ['PTS', 'TRB', 'AST', 'STL', 'BLK']
    counts = (reg_seas_played[cats] >= 10).sum(axis=1)

    reg_seas_played['double_double'] = (counts >= 2).astype(int)        # 1 point bonus
    reg_seas_played['triple_double'] = (counts >= 3).astype(int) * 2    # 2 point bonus

    # Build fantasy score
    reg_seas_played['fantasy_score'] = (
        rebounds
        + assists
        + points
        + steals
        + blocks
        + turnovers
        + reg_seas_played['double_double']
        + reg_seas_played['triple_double']
    )

    # Optional: round for neatness
    reg_seas_played['fantasy_score'] = reg_seas_played['fantasy_score'].round(2)

    return missed_reg_seas_played, reg_seas_played 

# --------------------------------------------------------------------------- #

from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression, Lasso, Ridge

def fantasy_score_model(player_data):
    '''
    Build an optimal model for a player

    Args:
        player_data - Dataframe (ex call get_player_data()[1])

    Returns:
        sklearn linear_model
        model r^2 score
    '''

    X = player_data.drop(columns=[
        'Date', 'Opp', 'fantasy_score', 'URL', 'Player_NAME', 'TRB', 'AST', 'PTS', 
        'TOV', 'STL', 'BLK', 'double_double', 'triple_double'
    ]).values
    y = player_data[['fantasy_score']].values

    scaler = StandardScaler()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    linreg = LinearRegression()
    linreg.fit(X_train_scaled, y_train)

    linreg_score = linreg.score(X_test_scaled, y_test)

    alpha_vals = [0.1, 1.0, 10.0, 100.0, 1000.0]

    ridge_scores = []
    for alpha in alpha_vals:
        ridge = Ridge(alpha=alpha)
        ridge.fit(X_train_scaled, y_train)
        ridge_scores.append(ridge.score(X_test_scaled, y_test))

    lasso_scores = []
    for alpha in alpha_vals:
        ridge = Lasso(alpha=alpha)
        ridge.fit(X_train_scaled, y_train)
        lasso_scores.append(ridge.score(X_test_scaled, y_test))

    scores_list = [[LinearRegression(), np.nan, linreg_score]] + \
    [[Ridge(), alpha, score] for alpha, score in zip(alpha_vals, ridge_scores)] + \
    [[Lasso(), alpha, score] for alpha, score in zip(alpha_vals, lasso_scores)]

    model_selection_df = (
        pd
        .DataFrame(scores_list, columns=['model', 'alpha', 'score'])
        .sort_values('score', ascending=False)
    )

    final_model_data = model_selection_df.iloc[0]
    final_model = final_model_data['model']
    final_alpha = final_model_data['alpha']

    if final_alpha != np.nan:
        final_model.set_params(alpha=final_alpha)

    final_model.fit(X_train_scaled, y_train)

    return final_model, final_model_data['score']