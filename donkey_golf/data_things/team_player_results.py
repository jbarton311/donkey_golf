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

class TeamPlayerResults(BaseClass):

    def pull_data(self):
        '''
        Pulls full team results by player
        '''
        logger.info("Pulling full team results by player")
        sql = self.yaml_sql_dict.get('scoreboard_users_team')

        df = self.run_sql(sql)

        self.data = df.copy()

    def clean_data(self):
        self.data['donkey_score'] = self.data['donkey_score'].astype(int)
        self.data = self.data.sort_values('donkey_score', ascending=True)

    def run(self):
        self.pull_data()
        self.clean_data()
