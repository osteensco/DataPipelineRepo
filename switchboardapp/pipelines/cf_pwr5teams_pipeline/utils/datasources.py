import datetime
import pandas as pd
import requests
import bs4 as bs
from google.cloud import bigquery
import logging
import re






# parent objects

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

















class CFTeamsAll(DataSource):
    def __init__(self) -> None:
        super().__init__()
        self.source = 'https://www.espn.com/college-football/'
        self.format = 'webscraped html'
        self.table_name = 'Power_5_Schools'
        self.dtypes = [
            bigquery.SchemaField('Conference', 'STRING'),
            bigquery.SchemaField('Team', 'STRING'),
            bigquery.SchemaField('URL', 'STRING'),
            bigquery.SchemaField('ID', 'STRING')
        ]

        self.dataset = '''portfolio-project-353016.collegeFootball.'''

    def schedule(self):
        super().schedule()
        
        query = f"""SELECT table_id FROM `{self.dataset}__TABLES__`"""
        tbls = self.db_engine.query(query).result().to_dataframe()
        tbls = tbls['table_id'].tolist()
        if self.table_name in tbls:
            query = f'''SELECT DATE(TIMESTAMP_MILLIS(last_modified_time)) AS dt FROM `{self.dataset}__TABLES__` where table_id = '{self.table_name}' '''
            result = self.db_engine.query(query).result().to_dataframe()
            result = result['dt'].tolist()[0]
            if datetime.datetime(result.year+1, result.month, result.day) <= datetime.datetime.today():#schedule to run annually
                logging.info(f'''{type(self).__name__} scheduled''')
                return True
            else:
                logging.info(f'''{type(self).__name__} not scheduled''')
                return False
        else:
            logging.info(f'''{self.table_name} not found in dataset tables, scheduling pull''')
            return True #schedule pull if table doesn't exist in database


    def extract(self):
        r = self.getreq(f'{self.source}teams')
        soup = bs.BeautifulSoup(r.text, 'html.parser')
        p5 = [
            'ACC', 
            'Big 12', 
            'Pac-12', 
            'Big Ten', 
            'SEC'
            ]#conferences we want data from
        #generate list of conference tables
        parentsoup = soup.find_all('div', class_="mt7")
        allteams = []
        for s in parentsoup:
            conf = s.find('div', class_="headline headline pb4 n8 fw-heavy clr-gray-01").get_text()
            if conf in p5:
                confteams = []
                #loop through teams in conf
                teams = s.find_all('div', class_="pl3")
                for t in teams:
                #find_all teams.get_text and urls
                    tm = {
                        'Conference': conf, 
                        'Team': t.find('h2', class_="di clr-gray-01 h5").get_text(), 
                        'URL': t.find('a', class_="AnchorLink").get('href'),
                        'ID': [m.group(1) for m in [re.search('_/id/(.+?)/', t.find('a', class_="AnchorLink").get('href'))] if m][0]
                        }
                    confteams.append(tm)
                #add completed confrence to p5 dictionary
                allteams += confteams                
        self.df = self.df.from_records(allteams)

    def load(self):
        loadjob = bigquery.LoadJobConfig(schema=self.dtypes, write_disposition='WRITE_TRUNCATE')
        self.truncate()
        self.db_engine.load_table_from_dataframe(self.df, f'''{self.dataset}{self.table_name}''', loadjob).result()
        logging.info(f'''Replaced table, {self.table_name} now up to date''')







