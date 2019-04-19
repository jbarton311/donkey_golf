import pandas as pd
import logging
import oyaml
from sqlalchemy import create_engine
import psycopg2

from donkey_golf import config

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

# Bringing in config and SQL dict
conf = config.DGConfig()
yaml_sql_dict = oyaml.load(open(conf.yaml_sql_loc))

engine = create_engine(conf.db_url)

def scrape_espn_leaderboard():
    dict = {}
    # Creating leaderboard & rankings dataframes
    leaderboard = pd.read_html('http://www.espn.com/golf/leaderboard',header=3)[0]

    leaderboard.columns = leaderboard.columns.str.lower().str.replace(' ','_')

    leaderboard['tourney_id'] = conf.tourney_id
    print(leaderboard.columns)
    if leaderboard.columns.tolist() == ['player', 'tee_time','tourney_id']:
        dict['pre_tourney'] = leaderboard

    elif leaderboard.columns.tolist() == ['pos',
                                         'player',
                                         'to_par',
                                         'r1',
                                         'r2',
                                         'r3',
                                         'r4',
                                         'tot',
                                         'earnings',
                                         'fedex_pts',
                                         'tourney_id']:
        leaderboard.rename(columns={'tot': 'total_strokes'},
                                inplace=True)

        leaderboard['thru'] = 'F'

        dict['in_progress'] = leaderboard
    else:
        # Getting rid of 1,500+ unnecessary columns ESPN brings in & a few columns from rankings df
        #keep_cols_leaderboard = ['pos','player','to_par','thru','r1','r2','r3','r4','tot','tourney_id']
        #leaderboard = leaderboard[keep_cols_leaderboard]

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
    #engine = create_engine(conf.db_url)
    with engine.connect() as conn:
        df.to_sql(tablename, conn, if_exists='replace', index=False)

    #engine.dispose()
    #with sqlite3.connect(conf.db_location) as conn:
    #df.to_sql(tablename, conn, if_exists="replace", index=False)

def run_sql(sql):
    '''
    Takes a SQL and connects to database to execute passed SQL
    '''
    #engine = create_engine(conf.db_url)
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    #engine.dispose()

    return df
    #engine = create_engine(config.DGConfig.db_url)
    #return pd.read_sql_query(sql ,engine)

    #with sqlite3.connect(conf.db_location) as conn:
#        return pd.read_sql_query(sql ,conn)

def data_load_leaderboard():
    result = scrape_espn_leaderboard()
    if 'pre_tourney' in result.keys():
        if result.get('pre_tourney').shape[0] > 0:
            print('pre_tourney load happening')
            load_table_to_db(result.get('pre_tourney'), 'pre_tourney')
    elif 'in_progress' in result.keys():
        if result.get('in_progress').shape[0] > 0:
            print('leaderboard load happening')
            df = result.get('in_progress')
            df = add_donkey_game_score(df)
            df = add_player_left_indicator(df)
            load_table_to_db(df, 'leaderboard')

def add_donkey_game_score(df):
    '''
    Will add a column called donkey score that should
    convert the to_par string to an integer
    '''
    df.loc[df['to_par'] == 'E', 'donkey_score'] = 0
    df.loc[df['to_par'].str[0:1] == '+', 'donkey_score'] = df['to_par'].str[1:]
    df.loc[df['to_par'].str[0:1] == '-', 'donkey_score'] = df['to_par']

    # IF they don't have a score of E or one that starts with a '+' or '-'
    # let's assume they didn't make it to the weekend
    df = add_score_for_cut_players(df)

    return df

def add_score_for_cut_players(df):
    '''
    Determines the score for cut players
    and adds a missed_cut field
    '''
    cut_score = df.loc[df['donkey_score'].notnull(), 'donkey_score'].astype(int).max()
    cut_score = cut_score + 1

    df['missed_cut'] = 0
    df.loc[df['donkey_score'].isnull(), 'missed_cut'] = 1

    df.loc[df['donkey_score'].isnull(), 'donkey_score'] = cut_score

    return df

def add_player_left_indicator(df):
    '''
    This should add a column 'player_left'
    indicating if a player is still in progress
    '''

    df['player_left'] = 1

    df.loc[df['missed_cut'] == 1, 'player_left'] = 0

    df.loc[(df['missed_cut'] == 0) &
          (df['r4'] != '--' ),
          'player_left'] = 0

    return df


def data_load_rankings():
    '''
    Will pull world rankings and load it to the database
    '''
    load_table_to_db(scrape_world_rankings_data(), 'rankings')

def login_checker(email):
    '''
    Pulls the user info for the login page
    '''

    sql = yaml_sql_dict.get('login_check')
    sql = sql.format(email)
    df = run_sql(sql)

    return df

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

def pull_tourney_leaderboard(user_id=None):
    '''
    Pulls leaderboard data for the tourney
    '''

    sql = yaml_sql_dict.get('pull_tourney_leaderboard')
    df = run_sql(sql)

    if user_id:
        user_df = users_team(user_id)

        df = df.merge(user_df[['golfer']],
            how='left',
            left_on=['player'],
            right_on=['golfer'])

        df.loc[df['golfer'].notnull(), 'on_team'] = 'Yes'

    return df

def aggregate_user_scores():
    '''
    Pull each users aggregate score for the current tourney
    '''
    sql = yaml_sql_dict.get('aggregate_team_score')
    df = run_sql(sql)

    df.sort_values('donkey_score', inplace=True)

    df['rank'] = df['donkey_score'].rank(ascending=True, method='min').astype(int)

    return df

def determine_ties_in_scoreboard(df):
    '''
    Takes a scoreboard df and determines rank and handles tiesself.

    It's nice to have T2 on the scoreboard instead of multiple "2"s
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
    df = aggregate_user_scores()
    tie_df = determine_ties_in_scoreboard(df)

    df = df.merge(tie_df,
        how='left',
        on='rank')

    return df

def calculate_cut_line():
    '''
    Pull the cut line from the tourney leaderboard table

    Returns a dict if round 2
    Else it returns None
    '''
    cur_round = determine_current_round()

    if cur_round == 'r2':
        sql = yaml_sql_dict.get('calculate_cut_line')
        df = run_sql(sql)
        # Grab the cut_str
        cut_str = df['pos'][0].lower()
        cut_dict = {}

        # Not sure what will happen if cut is even...
        if cut_str[-1:] == 'E':
            cut_dict['cut_str'] = 'E'
            cut_dict['cut_in'] = 0
        else:
            # Just grab the last two characters - this should be the cut score
            cut_str = cut_str[-2:]
            # Put both string and int representations into a dict
            cut_dict['cut_str'] = cut_str
            cut_dict['cut_int'] = int(cut_str.replace('+', '').replace('E', '0'))

        return cut_dict
    else:
        return None

def determine_current_round():
    '''
    Figure out which round is in progress. Will be helpful for cut
    calculations among other things
    '''
    df = pull_tourney_leaderboard()

    if 'fedex_pts' in df.columns:
        return 'tourney_over'
    else:
        rounds = ['r1','r2','r3','r4']
        for round_num in rounds:
            if df.loc[df[round_num] == '--'].shape[0] > 1:
                return round_num
