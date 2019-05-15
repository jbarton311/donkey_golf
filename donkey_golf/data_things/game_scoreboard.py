import oyaml
import pandas as pd
import logging
import psycopg2

from sqlalchemy import create_engine

from donkey_golf import config
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

from .tourney_leaderboard import BaseClass

class GameScoreboard(BaseClass):
    def __init__(self):
        BaseClass.__init__(self)

    def aggregate_user_scores(self):
        '''
        Pull each users aggregate score for the current tourney
        '''
        logger.info("Pulling aggregated team score for scoreboard")
        sql = self.yaml_sql_dict.get('aggregate_team_score')
        df = self.run_sql(sql)

        df.sort_values('donkey_score', inplace=True)

        df['rank'] = df['donkey_score'].rank(ascending=True, method='min').astype(int)

        self.data = df.copy()

    def determine_ties_in_scoreboard(self):
        '''
        Takes a scoreboard df and determines rank and handles tiesself.

        It's nice to have T2 on the scoreboard instead of multiple "2"s
        '''
        logger.info("Determining where ties exist in the donkey scoreboard")
        df_ties = self.data.copy()

        # Need to create a better rank that handles ties
        df_ties['new_rank'] = df_ties['rank'].copy()

        df_ties = df_ties.groupby(['rank'])['id'].count().reset_index()
        df_ties.loc[df_ties['id'] > 1, 'new_rank'] = "T"

        df_ties.loc[df_ties['new_rank'].notnull(), 'final_rank'] = 'T' + df_ties['rank'].astype(str)
        df_ties.loc[df_ties['new_rank'].isnull(), 'final_rank'] = df_ties['rank'].astype(str)

        self.df_ties = df_ties[['rank','final_rank']]

    def add_ties_to_data(self):
        logger.info("Adding ties to scoreboard data")
        self.data = self.data.merge(self.df_ties,
                                   how='left',
                                   on='rank')

    def pre_tourney_drafted_teams(self):
        '''
        Pull a list of teams that have drafted pre-tourney
        '''
        logger.info("Pulling pre tourney draft info")
        self.sql_pre_tourney = self.yaml_sql_dict.get('locked_in_teams')
        self.df_drafted_teams = self.run_sql(self.sql_pre_tourney)

    def run(self):
        if self.tourney_status != 'pre_tourney':
            self.aggregate_user_scores()
            self.determine_ties_in_scoreboard()
            self.add_ties_to_data()
        else:
            logger.info("Not pulling any scoreboard data since its pre_tourney")
            self.pre_tourney_drafted_teams()
