import sqlite3
import pandas as pd

from donkey_golf import config
import oyaml

conf = config.DGConfig()


def scrape_espn_leaderboard():
    dict = {}
    # Creating leaderboard & rankings dataframes
    leaderboard = pd.read_html('http://www.espn.com/golf/leaderboard',header=3)[0]

    leaderboard.columns = leaderboard.columns.str.lower().str.replace(' ','_')
    print(leaderboard.columns)
    if leaderboard.columns.tolist() == ['player', 'tee_time']:
        dict['pre_tourney'] = leaderboard
    else:
        # Getting rid of 1,500+ unnecessary columns ESPN brings in & a few columns from rankings df
        keep_cols_leaderboard = ['pos','player','to_par','thru','r1','r2','r3','r4','tot']

        leaderboard = leaderboard[keep_cols_leaderboard]

        #Adding underscore to column names & changing to lowercase for querying
        leaderboard.rename(columns={'tot': 'total_strokes'},
                                inplace=True)

        leaderboard['tourney_id'] = conf.tourney_id
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
                            'Name':'player_raw',
                            'Events Played (Actual)':'events_played'},
                            inplace=True)

    # Some of the players names are messed up
    # read correct mappings from YAML file and apply them
    yaml_dict = oyaml.load(open(conf.yaml_loc))
    player_map = yaml_dict.get('world_rankings')
    rankings['player_cleaned'] = rankings['player_raw'].map(player_map)

    rankings['player'] = rankings['player_cleaned'].combine_first(rankings['player_raw'])
    return rankings

def load_table_to_db(df, tablename):
    conn = sqlite3.connect('/home/greenman225/Code/github/donkey_golf/donkey_golf/site.db')
    c = conn.cursor()
    # Inserting leaderboard info into sql
    df.to_sql(tablename, conn, if_exists="replace", index=False)

def run_sql(sql):
    conn = sqlite3.connect('/home/greenman225/Code/github/donkey_golf/donkey_golf/site.db')
    return pd.read_sql_query(sql
                  , conn)

def data_load_leaderboard():
    result = scrape_espn_leaderboard()
    if 'pre_tourney' in result.keys():
        if result.get('pre_tourney').shape[0] > 0:
            print('pre_tourney load happening')
            load_table_to_db(result.get('pre_tourney'), 'pre_tourney')
    elif 'in_progress' in result.keys():
        if result.get('in_progress').shape[0] > 0:
            print('leaderboard load happening')
            load_table_to_db(result.get('in_progress'), 'leaderboard')

def data_load_rankings():
    load_table_to_db(scrape_world_rankings_data(), 'rankings')

def pull_available_players():
    conn = sqlite3.connect(conf.db_location)
    sql = '''SELECT a.*, b.current_rank, b.events_played FROM pre_tourney a
            LEFT JOIN rankings b ON a.player=b.player
            ORDER BY current_rank
            '''
    df = pd.read_sql(sql, conn)

    df['rank'] = df['current_rank'].rank(ascending=True, method='min')
    df['tier'] = 'Tier 2'
    df.loc[df['rank'] <= 12, 'tier'] = 'Tier 1'

    return df

def users_team(user_id):
    '''
    Pulls leaderboard data for everyone on a given users team
    '''

    user_lb_sql = f'''SELECT * from teams a
                    INNER JOIN leaderboard b
                        ON a.golfer = b.player
                    WHERE id = {user_id}'''

    df_user_lb = run_sql(user_lb_sql)

    if df_user_lb.shape[0] > 0:
        return df_user_lb
    else:
        user_sql = f'''
        SELECT * FROM teams
        WHERE id = {user_id}
        AND tourney_id = {conf.tourney_id}
        '''

        df_team_results = run_sql(user_sql)
        return df_team_results

def pull_tourney_leaderboard(user_id):
    '''
    Pulls leaderboard data for the tourney
    '''
    sql = '''
    SELECT * from leaderboard
    '''

    df = run_sql(sql)

    user_df = users_team(user_id)

    final_df = df.merge(user_df[['golfer']],
        how='left',
        left_on=['player'],
        right_on=['golfer'])

    final_df.loc[final_df['golfer'].notnull(), 'on_team'] = 'Yes'

    return final_df
