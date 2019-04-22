import oyaml
import pandas as pd
import logging
import psycopg2

from sqlalchemy import create_engine

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

class BaseClass():

    def __init__(self, **kwargs):
        logger.debug("ABOUT TO SET yaml_sql_dict")
        self.conf = config.DGConfig()
        self.yaml_sql_dict = oyaml.load(open(self.conf.yaml_sql_loc), Loader=oyaml.FullLoader)
        self.tourney_id = self.determine_current_tourney_id()
        self.determine_current_url()

    def determine_current_url(self):
        '''
        Returns the specific URL we want to scrape from
        '''
        self.url_lb = 'http://www.espn.com/golf/leaderboard/_/tournamentId/{}'.format(self.tourney_id)

    def run_sql(self, sql):
        '''
        Takes a SQL and connects to database to execute passed SQL
        '''
        logger.info("Executing run_sql")
        #engine = create_engine(self.conf.db_url)
        with psycopg2.connect(self.conf.db_url) as conn:
        #with engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        conn.close()

        return df

    def load_table_to_db(self, df, tablename):
        '''
        Takes a df and tablename and loads it to our database
        '''
        logger.info(f"Loading {tablename} to database")
        engine = create_engine(self.conf.db_url)

        with engine.connect() as conn:
            df.to_sql(tablename, conn, if_exists='replace', index=False)

    def determine_current_tourney_id(self):
        '''
        Pull each users aggregate score for the current tourney
        '''
        sql = self.yaml_sql_dict.get('determine_current_tourney_id')
        df = self.run_sql(sql)

        tourney_id =  df['value'][0].strip()
        logger.info(f"Current tourney ID: {tourney_id}")

        return tourney_id
