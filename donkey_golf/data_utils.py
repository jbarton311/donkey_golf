import sqlite3
import pandas as pd

def scrape_espn_leaderboard():
    # Creating leaderboard & rankings dataframes
    leaderboard = pd.read_html('http://www.espn.com/golf/leaderboard',header=3)[0]

    leaderboard.columns = leaderboard.columns.str.lower().str.replace(' ','_')
    
    # Getting rid of 1,500+ unnecessary columns ESPN brings in & a few columns from rankings df
    keep_cols_leaderboard = ['pos','player','to_par','r1','r2','r3','r4','tot']
    
    leaderboard = leaderboard[keep_cols_leaderboard]
    
    #Adding underscore to column names & changing to lowercase for querying
    leaderboard.rename(columns={'tot': 'total_strokes'}, 
                            inplace=True)
    
    return leaderboard

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
    conn = sqlite3.connect('donkey_database.db')
    c = conn.cursor()
    # Inserting leaderboard info into sql
    df.to_sql(tablename, conn, if_exists="replace")
    
def run_sql(sql):
    conn = sqlite3.connect('donkey_database.db')
    return pd.read_sql_query(sql
                  , conn)    

def data_load_leaderboard():
    load_table_to_db(scrape_espn_leaderboard(), 'leaderboard')
    
def data_load_rankings():
    load_table_to_db(scrape_world_rankings_data(), 'rankings')
