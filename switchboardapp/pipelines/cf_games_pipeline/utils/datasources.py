import datetime
import pandas as pd
import requests
import bs4 as bs
from google.cloud import bigquery
import logging








class DataSource:
    def __init__(self) -> None:
        self.source = None
        self.format = None
        self.df = pd.DataFrame()
        self.db_engine = None
        self.table_name = None
        self.dtypes = []
        self.scheduled = None #Boolean flag used by pipeline to determine if data should be pulled or not
        self.overwrite = None #date passed to Delete query for manually scheduled data pulls, avoids duplicate entries
        self.APIkey = False
        self.testitem = None
        self.dataset = '''portfolio-project-353016.ALL.'''

    def schedule(self):
        '''
        Method should include logic specific to child DataSource object.
        '''
        # used for determine if data should be ingested
        # Returns True/False
        self.db_engine = bigquery.Client('portfolio-project-353016')
        # Child DataSource objects will have specific queries to determine the boolean value to return

    def extract(self):
        '''
        Method should include logic specific to child DataSource object.
        '''
        return

    def load(self):
        if not self.schedule():#if manually scheduled
            delete_tbl = f'''DELETE FROM `{self.dataset}{self.table_name}` WHERE Date = '{self.overwrite}' '''
            self.db_engine.query(delete_tbl)
            logging.info(f'Removed any duplicate data from {self.table_name}, table cleaned for landing new pull')
        #land in appropriate tables
        loadjob = bigquery.LoadJobConfig(schema=self.dtypes)
        loadjob.write_disposition = 'WRITE_APPEND'
        loadjob.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        self.db_engine.load_table_from_dataframe(self.df, f'''{self.dataset}{self.table_name}''', loadjob).result()
        logging.info(f'''{type(self).__name__} loaded into {self.table_name}''' )

    def truncate(self):
        query = f'''TRUNCATE TABLE {self.dataset}{self.table_name}'''
        result = self.db_engine.query(query).result()
        logging.info(f'''{type(self).__name__} - {result}''')

    def getreq(self, url):
        r = requests.get(url=url, 
        headers={'User-Agent': '''Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36''','referer':'https://www.google.com/'})
        try:
            r.raise_for_status()
        except Exception as ex:
            logging.error(getattr(ex, 'message', repr(ex)))
        return r

    def test(self):
        self.schedule()
        self.extract()
        if self.df.shape[0] > 0:
            print(self.df.head()) 
        elif self.testitem:
            print(self.testitem)
        else:
            print('testitem/df failed to generate')







class CFGames(DataSource):#Table containing meta game data Columns: year, opponent, url(for game id)
    def __init__(self, massgrab=False, overwrite=False) -> None:
        super().__init__()
        self.source = 'https://www.espn.com'
        self.massgrab = massgrab
        self.overwrite = overwrite
        self.format = 'webscraped html'
        self.table_name = 'Power_5_Games'
        self.dtypes = [
            bigquery.SchemaField('YEAR', 'STRING'),
            bigquery.SchemaField('GameDate', 'STRING'),
            bigquery.SchemaField('GameID', 'STRING'),
            bigquery.SchemaField('HomeTeam', 'STRING'),
            bigquery.SchemaField('AwayTeam', 'STRING'),
            bigquery.SchemaField('GameURL', 'STRING'),
        ]
        self.dataset = '''portfolio-project-353016.collegeFootball.'''

    def schedule(self):
        super().schedule()
        if not self.massgrab and not self.overwrite and not self.scheduled:
            query = f"""SELECT table_id FROM `{self.dataset}__TABLES__`"""
            tbls = self.db_engine.query(query).result().to_dataframe()
            tbls = tbls['table_id'].tolist()
            if self.table_name in tbls:
                tdy = datetime.datetime.today()
                if tdy.month in [8,9,10,11,12,1] and tdy.weekday() == 0:#if its college football season and a monday then auto pull latest game data
                    logging.info(f'''{type(self).__name__} scheduled''')
                    return True
                else:
                    logging.info(f'''{type(self).__name__} not scheduled''')
                    return False
            else:
                self.massgrab = True
                logging.info(f'''{self.table_name} not found in dataset tables, scheduling pull''')
                return True #schedule pull if table doesn't exist in database           
        else:
            logging.info(f'''{type(self).__name__} scheduled manually''')
            return True

    def extract(self):
        allgames = []
        query = f"""SELECT * FROM {self.dataset}{self.table_name}"""
        p5schls = self.db_engine.query(query).result().to_dataframe()
        power5teams = [{'id': i['ID'], 'path': i['URL']} for i in p5schls.to_dict('records')]

        #pulling max year from the team page is easy, just using good ol TN since it doesn't matter which team is used
        path = f'''{self.source}/college-football/team/schedule/_/id/2633'''
        r = self.getreq(path)
        maxyr = int(max([y.get_text() for y in bs.BeautifulSoup(r.text, 'html.parser').find_all('option', class_="dropdown__option")]))
        years = []
        #2010-2022 if mass grab otherwise just current year
        if not self.massgrab:
            yr = maxyr
        else:
            yr = 2010
        while yr <= maxyr:
            years.append(str(yr))
            yr += 1

        for team in power5teams:
            for year in years:
                path = f'''{self.source}/college-football/team/schedule/_/id/{team['id']}/season/{year}'''
                r = self.getreq(path)
                soup = bs.BeautifulSoup(r.text, 'html.parser').find_all('tr')
                for s in soup:
                    game = {
                            "YEAR": year, 
                            "GameDate": '',
                            "GameID": '', 
                            "HomeTeam": '', 
                            "AwayTeam": '',
                            "GameURL": ''
                            }
                    #pulls in game id url
                    gmid = s.find('span', class_="ml4")
                    if gmid:
                        game["GameDate"] = s.td.span.get_text()
                        gmurl = gmid.find('a', class_="AnchorLink").get('href')
                        game["GameURL"] = gmurl
                        game["GameID"] = gmurl.split("gameId/")[1]
                        if s.find('span', class_="pr2").get_text() == 'vs':
                            game["HomeTeam"] = team['path']
                            game["AwayTeam"] = s.find('a', class_="AnchorLink").get('href')
                        else:
                            game["HomeTeam"] = s.find('a', class_="AnchorLink").get('href')
                            game["AwayTeam"] = team['path']
                        allgames.append(game)
        self.df = self.df.from_records(allgames)
        self.df = self.df.drop_duplicates()
        if not self.overwrite:
            self.df = self.clean_and_append(self.df, years)

    def clean_and_append(self, df, years):
        query = f"""SELECT * FROM {self.dataset}{self.table_name} WHERE YEAR IN {tuple(years) if len(years) > 1 else f'''('{years[0]}')'''}"""
        qdf = self.db_engine.query(query).result().to_dataframe()
        df = pd.concat([df, qdf], ignore_index=True)
        df = df.drop_duplicates(keep=False)
        return df

    def load(self):
        loadjob = bigquery.LoadJobConfig(schema=self.dtypes)
        if self.overwrite:
            self.truncate()
            loadjob.write_disposition = 'WRITE_TRUNCATE'
        else:
            loadjob.write_disposition = 'WRITE_APPEND'
        loadjob.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        self.db_engine.load_table_from_dataframe(self.df, f'''{self.dataset}{self.table_name}''', loadjob).result()
        logging.info(f'''{type(self).__name__} loaded into {self.table_name}''' )




