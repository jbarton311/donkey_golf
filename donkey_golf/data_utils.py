import sqlite3
import pandas as pd
import logging

from donkey_golf import config
import oyaml

import logging

# create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)



conf = config.DGConfig()

yaml_sql_dict = oyaml.load(open(conf.yaml_sql_loc))

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
    yaml_dict = oyaml.load(open(conf.yaml__world_rankings_loc))
    player_map = yaml_dict.get('world_rankings')
    rankings['player_cleaned'] = rankings['player_raw'].map(player_map)

    rankings['player'] = rankings['player_cleaned'].combine_first(rankings['player_raw'])
    return rankings

def load_table_to_db(df, tablename):
    '''
    Takes a df and tablename and loads it to our database
    '''
    with sqlite3.connect(conf.db_location) as conn:
        df.to_sql(tablename, conn, if_exists="replace", index=False)

def run_sql(sql):
    '''
    Takes a SQL and connects to database to execute passed SQL
    '''
    with sqlite3.connect(conf.db_location) as conn:
        return pd.read_sql_query(sql ,conn)

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
    '''
    Will pull world rankings and load it to the database
    '''
    load_table_to_db(scrape_world_rankings_data(), 'rankings')

def pull_available_players():
    '''
    Pulls a list of players to be available for the draft
    '''
    sql = yaml_sql_dict.get('pull_available_players')

    df = run_sql(sql)

    df['rank'] = df['current_rank'].rank(ascending=True, method='min')
    df['tier'] = 'Tier 2'
    df.loc[df['rank'] <= 12, 'tier'] = 'Tier 1'

    return df

def users_team(user_id):
    '''
    Pulls leaderboard data for everyone on a given users team
    '''

    user_lb_sql = yaml_sql_dict.get('users_team')

    # Sub in users ID into the SQL
    user_lb_sql = user_lb_sql.format(user_id)

    df_user_lb = run_sql(user_lb_sql)

    # CHANGE BACK
    if df_user_lb.shape[0] > 0:
        return df_user_lb
    else:
        sql_pre_tourney = yaml_sql_dict.get('users_team_pre_tourney')
        sql_pre_tourney = sql_pre_tourney.format(user_id, conf.tourney_id)

        df_team_results = run_sql(sql_pre_tourney)
        return df_team_results

def pull_tourney_leaderboard(user_id):
    '''
    Pulls leaderboard data for the tourney
    '''

    sql = yaml_sql_dict.get('pull_tourney_leaderboard')
    df = run_sql(sql)


    user_df = users_team(user_id)

    final_df = df.merge(user_df[['golfer']],
        how='left',
        left_on=['player'],
        right_on=['golfer'])

    final_df.loc[final_df['golfer'].notnull(), 'on_team'] = 'Yes'

    return final_df

def aggregate_team_score():
    '''
    Pull each users aggregate score for the current tourney
    '''
    sql = yaml_sql_dict.get('aggregate_team_score')
    df = run_sql(sql)

    # Convert to par field to an integer so we can aggregate
    df['user_score'] = df['to_par'].str.replace('+','').str.replace('E','0')
    df['user_score'] = df['user_score'].astype(int)

    # Get an overall score for each user
    team_score_df = df.groupby(['id','username'])['user_score'].sum().reset_index()
    team_score_df.sort_values('user_score', inplace=True)

    team_score_df['rank'] = team_score_df['user_score'].rank(ascending=True, method='min').astype(int)

    return team_score_df

def determine_ties_in_scoreboard(df):
    '''
    Takes a scoreboard df and determines rank and handles ties
    '''
    df_ties = df.copy()

    # Need to create a better rank that handles ties
    df_ties['new_rank'] = df_ties['rank'].copy()

    df_ties = df_ties.groupby(['rank'])['id'].count().reset_index()
    df_ties.loc[df_ties['id'] > 1, 'new_rank'] = "T"

    df_ties.loc[df_ties['new_rank'].notnull(), 'final_rank'] = 'T' + df_ties['rank'].astype(str)
    df_ties.loc[df_ties['new_rank'].isnull(), 'final_rank'] = df_ties['rank'].astype(str)

    return df_ties[['rank','final_rank']]

def pull_scoreboard():
    '''
    Pulls aggregate game score and then adds in tie info
    '''
    df = aggregate_team_score()
    tie_df = determine_ties_in_scoreboard(df)

    df = df.merge(tie_df,
        how='left',
        on='rank')

    return df
