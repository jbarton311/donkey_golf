import pandas as pd
import logging
import bs4
import requests
import re

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

class LoadPlayerInfo(BaseClass):

    def scrape_page(self):
        url = 'https://www.espn.com/golf/leaderboard'
        response = requests.get(url)
        logger.info(f"Response from website: {response}")

        self.soup = bs4.BeautifulSoup(response.text, "html.parser")

    def extract_main_table(self):
        # Grab the main leaderboard table
        table = self.soup.find('table', {'class': 'Table2__table-scroller Table2__right-aligned Table2__table'})

        # Grab all of the rows in a list
        self.rows = table.find_all('tr')

    def extract_data(self):

        # Dict to store all results
        self.output = {}

        # Loop thru all rows in table and pull data points
        # Don't want first row
        for player in self.rows[1:]:
            try:
                # Create clean dict to store results for each row
                new_dict = {}

                # Extract ESPN player ID from player URL
                url = player.find_all('a', href=True)[0]['href']
                player_id = re.sub("[^0-9]", "", url)


                new_dict['player_page'] = url

                # Grab the URL for the country flag
                new_dict['player_country'] = player.find_all('img')[0]['src']

                # Grab the player name
                new_dict['player_name'] = player.find_all('td')[2].text

                # Add results to dictionary
                self.output[player_id] = new_dict
            except:
                logger.info("BAD ROW in BS4 ESPN table scrape - could just be the cut row")

    def generate_output_data(self):
        self.data = pd.DataFrame.from_dict(self.output, orient='index',
                        columns=['player_name','player_page', 'player_country']).reset_index()

        self.data = self.data.rename(columns={'index':'player_id'})

    def run(self):
        self.scrape_page()
        self.extract_main_table()
        self.extract_data()
        self.generate_output_data()
        self.load_table_to_db(self.data, 'player_info')
