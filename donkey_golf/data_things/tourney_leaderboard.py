import oyaml
import pandas as pd
import logging
import psycopg2

from sqlalchemy import create_engine
from datetime import datetime

from .base_class import BaseClass

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

class LoadLeaderboard(BaseClass):

    def __init__(self):
        BaseClass.__init__(self)

    def scrape_espn_leaderboard(self, url):
        '''
        Scrape the main leaderboard table from the web
        '''
        data_dict = {}
        # Creating leaderboard & rankings dataframes
        leaderboard = pd.read_html(url,header=3)[0]
        leaderboard.columns = leaderboard.columns.str.lower().str.replace(' ','_')

        leaderboard['tourney_id'] = self.tourney_id

        self.data = leaderboard.copy()

    def determine_tourney_status(self):
        '''
        Look at the leaderboard data and determine current tourney
        status from that data
        '''
        if 'tee_time' in self.data.columns:
            self.tourney_status = 'pre_tourney'
        elif 'fedex_pts' in self.data.columns or 'earnings' in self.data.columns:
            self.tourney_status = 'finished'

        elif 'r4' in self.data.columns or 'tot' in self.data.columns:
            self.tourney_status = 'in_progress'
        else:
            self.tourney_status = 'cant_determine'

        logger.info(f"Tourney current status: {self.tourney_status}")

    def determine_current_round(self):
        '''
        Figure out which round is in progress. Will be helpful for cut
        calculations among other things
        '''
        logger.info("Determining the current round")
        df = self.data.copy()

        df = df.loc[df['missed_cut'] == 0]

        current_round = None

        if 'fedex_pts' in df.columns:
            current_round = 'finished'
        else:
            rounds = ['r1','r2','r3','r4']
            for round_num in rounds:
                if df.loc[df[round_num] == '--'].shape[0] > 1:
                    current_round = round_num
        logger.info(f"Determined the current round to be {current_round}")

        return current_round

    def add_donkey_game_score(self):
        '''
        Will add a column called donkey score that should
        convert the to_par string to an integer
        '''
        logger.info("Calculating the donkey_score column")
        self.data.loc[self.data['to_par'] == 'E', 'donkey_score'] = 0
        self.data.loc[self.data['to_par'].str[0:1] == '+', 'donkey_score'] = self.data['to_par'].str[1:]
        self.data.loc[self.data['to_par'].str[0:1] == '-', 'donkey_score'] = self.data['to_par']

        # IF they don't have a score of E or one that starts with a '+' or '-'
        # let's assume they didn't make it to the weekend
        self.add_score_for_cut_players()

    def add_score_for_cut_players(self):
        '''
        Determines the score for cut players
        and adds a missed_cut field
        '''
        logger.info("Determining score for cut players and adding missed_cut indicator")
        cut_score = self.data.loc[self.data['donkey_score'].notnull(), 'donkey_score'].astype(int).max()
        cut_score = cut_score + 1

        self.data['missed_cut'] = 0
        self.data.loc[self.data['donkey_score'].isnull(), 'missed_cut'] = 1

        self.data.loc[self.data['donkey_score'].isnull(), 'donkey_score'] = cut_score

    def add_player_left_indicator(self):
        '''
        This should add a column 'player_left'
        indicating if a player is still in progress
        '''
        logger.info("Determine if the player is active and if he has been cut or not")
        self.data['player_left'] = 1

        self.data.loc[self.data['missed_cut'] == 1, 'player_left'] = 0

        self.data.loc[(self.data['missed_cut'] == 0) &
              (self.data['r4'] != '--' ),
              'player_left'] = 0
        self.data.loc[self.data['today'] == '-', 'player_left'] = 0
        self.data.loc[self.data['thru'] == 'F', 'player_left'] = 0

    def add_golfer_team_count(self):
        '''
        This should add a column 'team_count'
        indicating the amount of teams a player is on
        '''
        self.golfer_team_count()

        logger.debug("Adding golfer team count")
        self.data = self.data.merge(self.golfer_count_by_team,
            how='left',
            on='player')

        self.data['team_count'] = self.data['team_count'].fillna(0.0).astype(int)

    def golfer_team_count(self):
        '''
        Pull each users aggregate score for the current tourney
        '''
        sql = self.yaml_sql_dict.get('golfer_team_count')
        df = self.run_sql(sql)

        self.golfer_count_by_team = df

    def prepare_active_data(self):
        '''
        If we know the data is from after the tourney has started,
        there will be manipulations we need to do
        '''
        logger.info("Running prepare_in_progress_data")
        self.clean_active_cols()
        self.add_donkey_game_score()
        self.add_player_left_indicator()
        self.add_golfer_team_count()
        self.data['load_date'] = datetime.today()
        self.data['donkey_score'] = self.data['donkey_score'].astype(int)

    def clean_active_cols(self):
        '''
        Clean function for the active tourney data
        '''
        if 'unnamed:_1' in self.data.columns:
            self.data = self.data.drop('unnamed:_1', axis=1)

    def prepare_pre_tourney_data(self):
        '''
        This will be used if we need to make any updated to the pre_tourney data
        '''
        pass

    def clean_data(self):
        '''
        Depending on the status of the tourney (and what .data will look like)
        we want to prepare/cleanse the data
        '''
        logger.info("Running clean_data function")
        if self.tourney_status in ['in_progress','finished']:
            self.prepare_active_data()
        elif self.tourney_status == 'pre_tourney':
            self.prepare_pre_tourney_data()

    def load_leaderboard_to_db(self):
        '''
        Take self.data and load it into the appropriate table based
        on the tourney_status
        '''
        if self.tourney_status == 'pre_tourney':
            logger.info("Loading data into the pre_tourney table")
            self.load_table_to_db(self.data, 'pre_tourney')
        else:
            logger.info("Loading data into the leaderboard table")
            self.load_table_to_db(self.data, 'leaderboard')

    def load_tourney_status(self):
        '''
        Load tourney status
        '''

        self.load_status_sql = self.yaml_sql_dict.get('load_tourney_status')
        self.load_status_sql = self.load_status_sql.format(self.tourney_status)
        run_this = self.execute_any_sql(self.load_status_sql)

    def run(self):
        # Get the leaderboard data
        self.scrape_espn_leaderboard(self.url_lb)

        # Determine the tourney status (set an attribute)
        self.determine_tourney_status()

        # Clean data - smart enough to determine pre_tourney or in-progress data
        self.clean_data()

        self.load_leaderboard_to_db()
        self.load_tourney_status()

class PullLeaderboard(BaseClass):

    def __init__(self, **kwargs):
        BaseClass.__init__(self, **kwargs)
        self.kwargs = kwargs
        if kwargs.get('user_id'):
            self.user_id = kwargs.get('user_id')
        else:
            logger.warning("NEED TO PASS A USER_ID")
            self.user_id = None

    def pull_pre_tourney_data(self):
        '''
        Pulls pre_tourney data for the tourney
        '''

        self.sql_pre_tourney = self.yaml_sql_dict.get('pull_available_players')
        self.df_pre_tourney = self.run_sql(self.sql_pre_tourney)

        # Fix some current_rank formatting
        self.df_pre_tourney['current_rank'] = self.df_pre_tourney['current_rank'].fillna(9999).astype(int).astype(str)
        self.df_pre_tourney['current_rank'] = self.df_pre_tourney['current_rank'].replace('9999', '-')

    def pull_tourney_leaderboard(self):
        '''
        Pulls leaderboard data for the tourney
        '''

        self.sql = self.yaml_sql_dict.get('pull_tourney_leaderboard')
        self.data = self.run_sql(self.sql)

        self.pull_users_team()

        if self.user_id and self.tourney_status in ['in_progress','finished']:
            # If the user_df comes back with records, add on_team field
            if not self.user_df.empty:
                logger.info("OUTER JOIN")
                self.data = self.data.merge(self.user_df[['player','id']],
                    how='left',
                    on=['player'])

                self.data['on_team'] = 'No'
                self.data.loc[self.data['id'].notnull(), 'on_team'] = 'Yes'

                self.data = self.data.drop('id', axis=1)

        self.data['team_count'] = self.data['team_count'].astype(int)

    def pull_users_team(self):
        '''
        Pulls leaderboard data for everyone on a given users team
        '''
        if self.tourney_status == 'pre_tourney':
            logger.info("Pulling users pre_tourney team")
            self.sql_user = self.yaml_sql_dict.get('users_team_pre_tourney')
            self.sql_user = self.sql_user.format(self.user_id, self.conf.tourney_id)
            self.user_df = self.run_sql(self.sql_user)
        else:
            logger.info("Pulling users active tourney team")
            self.sql_user = self.yaml_sql_dict.get('users_team')
            # Sub in users ID into the SQL
            self.sql_user = self.sql_user.format(self.user_id, self.tourney_id)
            self.user_df = self.run_sql(self.sql_user)

    def determine_current_round(self):
        '''
        Figure out which round is in progress. Will be helpful for cut
        calculations among other things
        '''
        logger.info("Determining the current round")
        df = self.data.copy()

        df = df.loc[df['missed_cut'] == 0]

        current_round = None

        if 'fedex_pts' in df.columns:
            current_round = 'finished'
        else:
            rounds = ['r1','r2','r3','r4']
            for round_num in rounds:
                if df.loc[df[round_num] == '--'].shape[0] > 1:
                    current_round = round_num
                    return current_round
        logger.info(f"Determined the current round to be {current_round}")

        return current_round

    def calculate_cut_line(self):
        '''
        Pull the cut line from the tourney leaderboard table

        Returns a dict if round 2
        Else it returns None
        '''
        logger.info("Running calculate cut line")
        cur_round = self.determine_current_round()

        if cur_round == 'r2':
            sql = self.yaml_sql_dict.get('calculate_cut_line')
            df = self.run_sql(sql)
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
            logger.info("Dont need to worry about the cut line now")
            return None

    def calc_refresh_date(self):
        '''
        Add attributes to determine when the tourney data was refreshed
        '''
        logger.info("Determing data refresh times")

        if self.tourney_status == 'pre_tourney':
            logger.info("Tourney hasnt started yet - no relevant data in database ")
        else:
            self.refresh_timedelta = self.data['load_date'].min()
            self.refresh_minutes = (datetime.today() - self.refresh_timedelta).seconds // 60

            # Create a string to display refresh date
            if self.refresh_minutes == 0:
                self.refresh_string = "Refreshed <1 minute ago"
            elif self.refresh_minutes == 1:
                self.refresh_string = "Refreshed 1 minute ago"
            else:
                self.refresh_string =  f"Refreshed {self.refresh_minutes} minutes ago"

    def run(self):
        self.pull_tourney_leaderboard()
        #self.pull_users_team()
        self.calc_refresh_date()
