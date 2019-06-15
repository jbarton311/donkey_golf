import oyaml
import pandas as pd
import logging
import psycopg2

from sqlalchemy import create_engine
from datetime import datetime

from .base_class import BaseClass
from .tourney_leaderboard import PullLeaderboard

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

class SideAction():
    def __init__(self):
        pull = PullLeaderboard(user_id=28)
        pull.run()
        self.df = pull.data
        self.df = self.df[['pos','player','to_par','today','thru','player_country']]

    def bet_1_and_5(self):
        '''
        USA vs. Everyone
        First Time Major Winner
        '''
        self.bet1 = self.df.head(6)
        self.bet1['bet'] = 'Ben O'
        self.bet1.loc[self.bet1['player_country'].str.contains('usa.png'), 'bet'] = 'King of the Donks'

    def bet_2(self):
        '''
        Koepka vs. DJ
        '''
        self.bet2 = self.df.loc[self.df['player'].isin(['Brooks Koepka','Dustin Johnson'])]
        self.bet2['bet'] = 'Ben O'
        self.bet2.loc[self.bet2['player'] == 'Brooks Koepka', 'bet'] = 'King of the Donks'

    def bet_3(self):
        '''
        Fitzpatrick vs. Tier 3
        '''
        self.bet3 = self.df.loc[self.df['player'].isin(['Matthew Fitzpatrick','Jordan Spieth'])]
        self.bet3['bet'] = 'Ben O'
        self.bet3.loc[self.bet3['player'] == 'Jordan Spieth', 'bet'] = 'King of the Donks'

    def bet_4(self):
        '''
        Rory vs. Spieth
        '''
        self.bet4 = self.df.loc[self.df['player'].isin(['Rory McIlroy','Jordan Spieth'])]
        self.bet4['bet'] = 'Ben O'
        self.bet4.loc[self.bet4['player'] == 'Rory McIlroy', 'bet'] = 'King of the Donks'

    def aggregate_results(self):
        winners = []

        winners.append(self.bet1.reset_index().iloc[0]['bet'])
        winners.append(self.bet2.reset_index().iloc[0]['bet'])
        winners.append(self.bet3.reset_index().iloc[0]['bet'])
        winners.append(self.bet4.reset_index().iloc[0]['bet'])

        result = {}
        result['King of the Donks'] = winners.count('King of the Donks')
        result['Ben O'] = winners.count('Ben O')
        self.result = result
        
    def run(self):
        self.bet_1_and_5()
        self.bet_2()
        self.bet_3()
        self.bet_4()
        self.aggregate_results()
