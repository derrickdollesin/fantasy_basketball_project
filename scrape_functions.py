import re
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup

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

###############################################################################
#                              Player Functions                               #
###############################################################################

### Available Functions
###
# player_avg_data(player_username)
# player_sum_data(player_username)
# player_per36_data(player_username)
# player_advanced_data(player_username)
# player_season_data(player_username)
###

###############################################################################
#                               Team Functions                                #
###############################################################################

### Available Functions
###
# all_team_historical_data()
# team_historical_data(team)
# team_per36_data(team, year)
# team_avg_data(team, year)
# team_season_data(team, year)
###

###############################################################################
#                              Season Functions                               #
###############################################################################

### Available Functions
###
# season_data()
###

class Scrape_Functions:

    def __init__(self):
        self.teams = [
            "ATL", "BOS", "NJN", "CHI", "CLE", "DAL", "DEN", "DET", 
            "GSW", "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", 
            "MIN", "NOH", "NYK", "OKC", "ORL", "PHI", "PHO", "POR", 
            "SAC", "SAS", "TOR", "UTA", "WAS", "CHA"
        ]

# --------------------------------------------------------------------------- #

    @staticmethod
    def player_avg_data(player_username):

        soup = get_soup(
            f'https://www.basketball-reference.com/players/{player_username[0]}/{player_username}.html'
        )

        table = soup.select_one(
            'table[data-soc-sum-table-type="PlayerPerGame"][data-soc-sum-phase-type="reg"]'
        )

        cols = [
            col.get_text(strip=True) for col in table.select_one('tr').select('th')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        reg_season = pd.DataFrame(rows, columns=cols)

        reg_season = reg_season[
            reg_season.apply(lambda x: x != '')
        ].dropna(how='all', axis=0).fillna('')

        table = soup.select_one(
            'table[data-soc-sum-table-type="PlayerPerGame"][data-soc-sum-phase-type="post"]'
        )

        cols = [
            col.get_text(strip=True) for col in table.select_one('tr').select('th')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        playoff_season = pd.DataFrame(rows, columns=cols)

        playoff_season = playoff_season[
            playoff_season.apply(lambda x: x != '')
        ].dropna(how='all', axis=0).fillna('')
            
        reg_season['Season Type'] = 'Regular'
        playoff_season['Season Type'] = 'Playoff'

        data = pd.concat([reg_season, playoff_season], axis=0)
        data = data.reset_index(drop=True)

        return data

    # --------------------------------------------------------------------------- #

    @staticmethod
    def player_sum_data(player_username):

        soup = get_soup(
            f'https://www.basketball-reference.com/players/{player_username[0]}/{player_username}.html'
        )

        table = soup.select_one(
            'table[data-soc-sum-table-type="PlayerTotals"][data-soc-sum-phase-type="reg"]'
        )

        cols = [
            col.get_text(strip=True) for col in table.select_one('tr').select('th')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        reg_season = pd.DataFrame(rows, columns=cols)

        reg_season = reg_season[
            reg_season.apply(lambda x: x != '')
        ].dropna(how='all', axis=0).fillna('')

        table = soup.select_one(
            'table[data-soc-sum-table-type="PlayerTotals"][data-soc-sum-phase-type="post"]'
        )

        cols = [
            col.get_text(strip=True) for col in table.select_one('tr').select('th')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        playoff_season = pd.DataFrame(rows, columns=cols)

        playoff_season = playoff_season[
            playoff_season.apply(lambda x: x != '')
        ].dropna(how='all', axis=0).fillna('')
            
        reg_season['Season Type'] = 'Regular'
        playoff_season['Season Type'] = 'Playoff'

        data = pd.concat([reg_season, playoff_season], axis=0)
        data = data.reset_index(drop=True)

        return data

    # --------------------------------------------------------------------------- #

    @staticmethod
    def player_per36_data(player_username):
        
        soup = get_soup(
            f'https://www.basketball-reference.com/players/{player_username[0]}/{player_username}.html'
        )

        table = soup.select_one(
            'table[data-soc-sum-table-type="PlayerPerMinute"][data-soc-sum-phase-type="reg"]'
        )

        cols = [
            col.get_text(strip=True) for col in table.select_one('tr').select('th')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        reg_season = pd.DataFrame(rows, columns=cols)

        reg_season = reg_season[
            reg_season.apply(lambda x: x != '')
        ].dropna(how='all', axis=0).fillna('')

        table = soup.select_one(
            'table[data-soc-sum-table-type="PlayerPerMinute"][data-soc-sum-phase-type="post"]'
        )

        cols = [
            col.get_text(strip=True) for col in table.select_one('tr').select('th')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        playoff_season = pd.DataFrame(rows, columns=cols)

        playoff_season = playoff_season[
            playoff_season.apply(lambda x: x != '')
        ].dropna(how='all', axis=0).fillna('')
            
        reg_season['Season Type'] = 'Regular'
        playoff_season['Season Type'] = 'Playoff'

        data = pd.concat([reg_season, playoff_season], axis=0)
        data = data.reset_index(drop=True)

        return data

    # --------------------------------------------------------------------------- #

    @staticmethod
    def player_advanced_data(player_username):
        soup = get_soup(
            f'https://www.basketball-reference.com/players/{player_username[0]}/{player_username}.html'
        )

        table = soup.select_one(
            'table[data-soc-sum-table-type="Advanced"][data-soc-sum-phase-type="reg"]'
        )

        cols = [
            col.get_text(strip=True) for col in table.select_one('tr').select('th')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        reg_season = pd.DataFrame(rows, columns=cols)

        reg_season = reg_season[
            reg_season.apply(lambda x: x != '')
        ].dropna(how='all', axis=0).fillna('')

        table = soup.select_one(
            'table[data-soc-sum-table-type="Advanced"][data-soc-sum-phase-type="post"]'
        )

        cols = [
            col.get_text(strip=True) for col in table.select_one('tr').select('th')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        playoff_season = pd.DataFrame(rows, columns=cols)

        playoff_season = playoff_season[
            playoff_season.apply(lambda x: x != '')
        ].dropna(how='all', axis=0).fillna('')
            
        reg_season['Season Type'] = 'Regular'
        playoff_season['Season Type'] = 'Playoff'

        data = pd.concat([reg_season, playoff_season], axis=0)
        data = data.reset_index(drop=True)

        return data

    # --------------------------------------------------------------------------- #

    @staticmethod
    def player_shooting_data(player_username):

        soup = get_soup(
            f'https://www.basketball-reference.com/players/{player_username[0]}/{player_username}.html'
        )

        table = soup.select_one(
            'table[data-soc-sum-table-type="Shooting"][data-soc-sum-phase-type="reg"]'
        )

        cols = [
            col.get_text(strip=True) for col in table.select_one('tr').select('th')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        reg_season = pd.DataFrame(rows, columns=cols)

        reg_season = reg_season[
            reg_season.apply(lambda x: x != '')
        ].dropna(how='all', axis=0).fillna('')

        table = soup.select_one(
            'table[data-soc-sum-table-type="Shooting"][data-soc-sum-phase-type="post"]'
        )

        cols = [
            col.get_text(strip=True) for col in table.select_one('tr').select('th')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        playoff_season = pd.DataFrame(rows, columns=cols)

        playoff_season = playoff_season[
            playoff_season.apply(lambda x: x != '')
        ].dropna(how='all', axis=0).fillna('')
            
        reg_season['Season Type'] = 'Regular'
        playoff_season['Season Type'] = 'Playoff'

        data = pd.concat([reg_season, playoff_season], axis=0)
        data = data.reset_index(drop=True)

        return data

    # --------------------------------------------------------------------------- #

    @staticmethod
    def player_season_data(player_username, season):    
        
        soup = get_soup(
            f'https://www.basketball-reference.com/players/{player_username[0]}/{player_username}/gamelog/{season}/'
        )
        
        table = soup.select_one('table#player_game_log_reg')

        if not table:
            print("No Data Found")
            return

        header_cells = table.select('thead tr')[-1].select('th[scope="col"]')
        cols = [c.get_text(strip=True) for c in header_cells]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue
            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        reg_season = pd.DataFrame(rows, columns=cols)
        reg_season['Season Type'] = 'Regular'

        ### Playoffs

        table = soup.select_one('table#player_game_log_post')

        if not table:
            print('No Playoff Data Found')

            reg_season = reg_season.drop(columns='')

            reg_season['TS'] = reg_season['Result'].apply(
                lambda x: re.findall(' [0-9]*', x)[0]
            ).str.replace(' ', '')
            reg_season['OS'] = reg_season['Result'].apply(
                lambda x: re.findall('-[0-9]*', x)[0]
            ).str.replace('-', '')
            reg_season['Result'] = reg_season['Result'].apply(lambda x: x[0])

            return reg_season
        
        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue
            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        post_season = pd.DataFrame(rows, columns=cols)
        post_season['Season Type'] = 'Playoff'

        data = pd.concat([reg_season, post_season], axis=0)
        data = data.drop(columns='')

        data['TS'] = data['Result'].apply(
            lambda x: re.findall(' [0-9]*', x)[0]
        ).str.replace(' ', '')
        data['OS'] = data['Result'].apply(
            lambda x: re.findall('-[0-9]*', x)[0]
        ).str.replace('-', '')
        data['Result'] = data['Result'].apply(lambda x: x[0])

        return data

    # --------------------------------------------------------------------------- #

    @staticmethod
    def all_team_historical_data():
        soup = get_soup('https://www.basketball-reference.com/teams/')

        teams = soup.select_one('table#teams_active')

        cols = [
            col.get_text(strip=True) for col in teams.select('th[scope="col"]')
        ]

        NUM_COLS = len(cols)
        rows = []
        for tr in teams.select('tbody tr'):
            if 'left' in tr.select_one('th')['class'] \
            and 'class' in tr.attrs \
            and 'thead' in tr['class']:
                continue
            
            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)
            
        teams = pd.DataFrame(rows, columns=cols)

        return teams

    # --------------------------------------------------------------------------- #

    def team_historical_data(self, team):

        if team not in self.teams:
            print(f'Team {team} Not Found')
            return 

        soup = get_soup(f'https://www.basketball-reference.com/teams/{team}/')

        table = soup.select_one(f'table#{team}')

        if not table:
            print(f'No Data Found ({team})')
            return

        cols = [
            col.get_text(strip=True) for col in table.select('th[scope="col"]')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)
            
        return pd.DataFrame(rows, columns=cols)

    # --------------------------------------------------------------------------- #

    def team_season_data(self, team, year):

        if team not in self.teams:
            print(f'Team {team} Not Found')
            return 
        
        soup = get_soup(
            f'https://www.basketball-reference.com/teams/{team}/{year}.html'
        )

        table = soup.select_one('table#roster')

        if not table:
            print(f'No Data Found ({team})')
            return

        cols = [
            col.get_text(strip=True) for col in table.select('th[scope="col"]')
        ]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue
            
            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        return pd.DataFrame(rows, columns=cols)

    # --------------------------------------------------------------------------- #

    def team_avg_data(self, team, year):

        if team not in self.teams:
            print(f'Team {team} Not Found')
            return

        soup = get_soup(
            f'https://www.basketball-reference.com/teams/{team}/{year}.html'
        )
        
        table = soup.select_one('table#per_game_stats')

        if not table:
            print(f'No Data Found ({team})')
            return 

        cols = [col.get_text(strip=True) for col in table.select('th[scope="col"]')]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue
            
            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        return pd.DataFrame(rows, columns=cols)

    # --------------------------------------------------------------------------- #

    def team_per36_data(self, team, year):

        if team not in self.teams:
            print(f'Team {team} Not Found')
            return
        
        soup = get_soup(
            f'https://www.basketball-reference.com/teams/{team}/{year}.html'
        )
        
        table = soup.select_one('table#per_minute_stats')

        if not table:
            print(f'No Data Found ({team})')
            return 

        cols = [col.get_text(strip=True) for col in table.select('th[scope="col"]')]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tbody tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue
            
            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        return pd.DataFrame(rows, columns=cols)

    # --------------------------------------------------------------------------- #

    @staticmethod
    def season_data():
        soup = get_soup(
            f'http://basketball-reference.com/leagues/'
        )
        
        table = soup.select_one('table#stats')

        if not table:
            print('No Data Found')
            return

        cols = [col.get_text(strip=True) for col in table.select('th[class="sort_default_asc center"]')]
        NUM_COLS = len(cols)

        rows = []
        for tr in table.select('tr'):
            if 'class' in tr.attrs and 'thead' in tr['class']:
                continue

            cells = tr.find_all(['th', 'td'])
            row = [c.get_text(strip=True) for c in cells]
            if len(row) < NUM_COLS:
                row += [''] * (NUM_COLS - len(row))
            elif len(row) > NUM_COLS:
                row = row[:NUM_COLS]
            rows.append(row)

        return pd.DataFrame(rows, columns=cols)

    # --------------------------------------------------------------------------- #
