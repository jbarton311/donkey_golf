import oyaml
import pandas as pd
import logging
import psycopg2
import requests

from sqlalchemy import create_engine
from bs4 import BeautifulSoup

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

class TournamentInfo(BaseClass):
    def __init__(self):
        BaseClass.__init__(self)

    def pull_current_tourney_data(self):
        source = requests.get(self.url_lb).text
        espn = BeautifulSoup(source, 'lxml')
        tournament_info = espn.find('div', class_='Leaderboard__Header')
        tourney_data = {}
        course_data = tournament_info.find('div', class_= 'Leaderboard__Course__Location__Detail n8 clr-gray-04').text
        tourney_data['tourney_name'] = [tournament_info.find('h1', class_='headline__h1 Leaderboard__Event__Title').text]
        tourney_data['tourney_dates'] = [tournament_info.find('span', class_='Leaderboard__Event__Date n7').text]
        tourney_data['course_name'] = [tournament_info.find('div', class_= 'Leaderboard__Course__Location n8 clr-gray-04').text]
        tourney_data['course_par'] = [course_data[3:5]]
        tourney_data['course_yardage'] = [course_data[-4:]]
        tournament_winner_data = tournament_info.find('div', class_= 'n7 clr-gray-04')
        winner = []
        for node in tournament_winner_data.find_all('span', class_= 'clr-gray-05 mr2 label'):
            winner.append(node.next_sibling)
        tourney_data['tourney_purse'] = [winner[0]]
        tourney_data['tourney_defending_champ'] = [winner[1]]
        self.data_dict = tourney_data

    def convert_to_dataframe(self):
        self.data = pd.DataFrame(self.data_dict)
        self.data['tourney_id'] = self.tourney_id

    def run(self):
        self.pull_current_tourney_data()
        self.convert_to_dataframe()
        self.load_table_to_db(self.data, 'tourney_info')
