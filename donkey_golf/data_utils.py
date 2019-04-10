import sqlite3
import pandas as pd

from donkey_golf import config

secrets = config.DGConfig()

def scrape_espn_leaderboard():
    dict = {}
    # Creating leaderboard & rankings dataframes
    leaderboard = pd.read_html('http://www.espn.com/golf/leaderboard',header=3)[0]

    leaderboard.columns = leaderboard.columns.str.lower().str.replace(' ','_')

    if leaderboard.columns.tolist() == ['player', 'tee_time']:
        dict['pre_tourney'] = leaderboard
    else:
        # Getting rid of 1,500+ unnecessary columns ESPN brings in & a few columns from rankings df
        keep_cols_leaderboard = ['pos','player','to_par','r1','r2','r3','r4','tot']

        leaderboard = leaderboard[keep_cols_leaderboard]

        #Adding underscore to column names & changing to lowercase for querying
        leaderboard.rename(columns={'tot': 'total_strokes'},
                                inplace=True)

        dict['in_progress'] = leaderboard

    return dict

def scrape_world_rankings_data():
    rankings = pd.read_html('http://www.owgr.com/ranking?pageNo=1&pageSize=All&country=All', header=0)[0]

    keep_cols_rankings = ['This Week', 'Last week', 'End 2018', 'Name', 'Events Played (Actual)']
    rankings = rankings[keep_cols_rankings]


    rankings.rename(columns= {
                            'This Week': 'current_rank',
                            'Last week':'lw_rank',
                            'End 2018':'ly_rank',
                            'Name':'player',
                            'Events Played (Actual)':'events_played'},
                            inplace=True)

    return rankings

def load_table_to_db(df, tablename):
    conn = sqlite3.connect('site.db')
    c = conn.cursor()
    # Inserting leaderboard info into sql
    df.to_sql(tablename, conn, if_exists="replace", index=False)

def run_sql(sql):
    conn = sqlite3.connect('site.db')
    return pd.read_sql_query(sql
                  , conn)

def data_load_leaderboard():
    result = scrape_espn_leaderboard()
    if result.get('pre_tourney').shape[0] > 0:
        print('pre_tourney load happening')
        load_table_to_db(result.get('pre_tourney'), 'pre_tourney')
    elif result.get('in_progress').shape[0] > 0:
        print('leaderboard load happening')
        load_table_to_db(scrape_espn_leaderboard(), 'leaderboard')

def data_load_rankings():
    load_table_to_db(scrape_world_rankings_data(), 'rankings')

def pull_available_players():
    conn = sqlite3.connect(secrets.db_location)
    return pd.read_sql('SELECT * FROM pre_tourney', conn)
