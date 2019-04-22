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

class DraftDay(BaseClass):
    def __init__(self):
        BaseClass.__init__(self)

    def pull_available_players(self):
        '''
        Pulls a list of players to be available for the draft
        '''
        logger.info("Pulling a list of available players for the draft")
        sql = self.yaml_sql_dict.get('pull_available_players')

        df = self.run_sql(sql)

        df['rank'] = df['current_rank'].rank(ascending=True, method='min')
        df['tier'] = 'Tier 2'
        df.loc[df['rank'] <= 12, 'tier'] = 'Tier 1'

        self.data = df.copy()

    def run(self):
        self.pull_available_players()
