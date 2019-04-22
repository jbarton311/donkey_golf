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

class LoadWorldRankings(BaseClass):
    def __init__(self):
        BaseClass.__init__(self)

    def scrape_world_rankings_data(self):
        logger.info("Scraping world rankings data")
        rankings = pd.read_html('http://www.owgr.com/ranking?pageNo=1&pageSize=All&country=All', header=0)[0]

        keep_cols_rankings = ['This Week', 'Last week', 'End 2018', 'Name', 'Events Played (Actual)']
        rankings = rankings[keep_cols_rankings]

        self.data = rankings.copy()

    def clean_data(self):
        logger.info("Cleaning data")
        self.data.rename(columns= {
                                'This Week': 'current_rank',
                                'Last week':'lw_rank',
                                'End 2018':'ly_rank',
                                'Name':'player_raw',
                                'Events Played (Actual)':'events_played'},
                                inplace=True)
        self.fix_player_names()
        self.limit_to_top_players()

    def fix_player_names(self):
        logger.info("Fixing some players names that dont align between systems")

        # Some of the players names are messed up
        # read correct mappings from YAML file and apply them
        yaml_dict = oyaml.load(open(self.conf.yaml_world_rankings_loc), Loader=oyaml.FullLoader)
        player_map = yaml_dict.get('world_rankings')

        self.data['player_cleaned'] = self.data['player_raw'].map(player_map)
        self.data['player'] = self.data['player_cleaned'].combine_first(self.data['player_raw'])

    def limit_to_top_players(self):
        logger.info("Trimming world rankings to only top 400")
        self.data = self.data.loc[self.data['current_rank'] <= 400]

    def run(self):
        self.scrape_world_rankings_data()
        self.clean_data()
        self.load_table_to_db(self.data, 'world_rankings')
