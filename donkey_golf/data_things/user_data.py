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

class UserData(BaseClass):
    def __init__(self, **kwargs):
        logger.debug("ABOUT TO SET yaml_sql_dict")
        self.conf = config.DGConfig()
        self.yaml_sql_dict = oyaml.load(open(self.conf.yaml_sql_loc), Loader=oyaml.FullLoader)
        
        self.kwargs = kwargs
        if kwargs.get('user_id'):
            self.user_id = kwargs.get('user_id')
        else:
            logger.warning("NEED TO PASS A USER_ID")
            self.user_id = None

    def pull_data(self):
        '''
        Pulls user info
        '''
        logger.info("Pulling user info")
        self.sql = self.yaml_sql_dict.get('user_info')
        self.sql = self.sql.format(self.user_id)
        df = self.run_sql(self.sql)
        self.data = df.copy()

    def run(self):
        self.pull_data()
