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














class GeoData(DataSource):
    def __init__(self) -> None:
        super().__init__()
        self.source = '''https://www.unitedstateszipcodes.org'''
        self.format = 'webscraped html'
        self.table_name = 'US_Zips_Counties'
        self.dtypes = [
            bigquery.SchemaField('ZIP_Code', 'STRING'),
            bigquery.SchemaField('County', 'STRING'),
            bigquery.SchemaField('State', 'STRING')
        ]
        self.df = pd.DataFrame(columns=['ZIP_Code', 'County', 'State'], dtype=str)
        self.States = [
            'AL', 'AR', 'AZ', 'CA', 'CO', 'CT',
            'DE', 'FL', 'GA', 'IA', 'ID', 'IL',
            'IN', 'KS', 'KY', 'LA', 'MA', 'MD',
            'ME', 'MI', 'MN', 'MO', 'MS', 'MT',
            'NC', 'ND', 'NE', 'NH', 'NJ', 'NM',
            'NV', 'NY', 'OH', 'OK', 'OR', 'PA',
            'RI', 'SC', 'SD', 'TN', 'TX', 'UT',
            'VA', 'VT', 'WA', 'WI', 'WV', 'WY'
        ]

    def schedule(self):
        super().schedule()
        
        query = """SELECT table_id FROM `portfolio-project-353016.ALL.__TABLES__`"""
        tbls = self.db_engine.query(query).result().to_dataframe()
        tbls = tbls['table_id'].tolist()
        if self.table_name in tbls:
            query = f'''SELECT MAX(Date_Pulled) AS dt FROM `{self.dataset}{self.table_name}` '''
            result = self.db_engine.query(query).result().to_dataframe()
            result = result['dt'].tolist()[0]
            if result.year < datetime.date.today().year:#schedule to run every new year
                logging.info(f'''{type(self).__name__} scheduled''')
                return True
            else:
                logging.info(f'''{type(self).__name__} not scheduled''')
                return False
        else:
            print(f'''{self.table_name} not found in dataset tables, scheduling pull''')
            return True #schedule pull if table doesn't exist in database

    def extract(self):
        for state in self.States:
            r = self.getreq(f'''{self.source}/{state.lower()}/#zips-list''')
            soup = bs.BeautifulSoup(r.text, 'html.parser')
            zips = []
            counties = []#these lists will contain the data for each column, since we loop it will maintain correct order for rows
            sts = []
            ZipSoup = soup.find_all('div', class_="col-xs-12 prefix-col1")#list of dirty zip data
            CountySoup = soup.find_all('div', class_="col-xs-12 prefix-col4")#list of dirty county data
            DirtySoup = [ZipSoup, CountySoup]
            for each in DirtySoup:
                for x in each:
                    if each.index(x) != 0: #each[0] is the header, we want to bypass this
                        x = x.get_text()#beautiful soup method, returns html data as text
                        x = x.translate({ord(char): None for char in ['\r', '\n', '\t']})#clean html formatting text
                        if each is ZipSoup:#put data in correct list
                            zips.append(x)
                        elif each is CountySoup:#put data in correct list
                            x = x.removesuffix(" County")
                            counties.append(x)
                    else:#bypass header
                        continue
            if len(counties) != len(zips):#these should always equal, otherwise will end program and print below info
                logging.error(f'''Shape Error found for state {state}''')
                logging.info(f'''{len(counties)} rows for counties''')
                logging.info(f'''{len(zips)} rows for zips''')
            else:
                for i in range(len(counties)):#we're just adding the current State we're on for each row that exists
                    sts.append(state)
                table = {'ZIP_Code': zips, 'County': counties, 'State': sts}#attach our data to column names
                df1 = pd.DataFrame(data=table, dtype=str)#put in dataframe to easily append
                df1['Date_Pulled'] = datetime.date.today()#add date column
                self.df = pd.concat([self.df, df1], ignore_index=True)#add to our dataframe
                logging.info(f'''{state} Zips and Counties scraped successfully!''')

    def load(self):
        loadjob = bigquery.LoadJobConfig(schema=self.dtypes, write_disposition='WRITE_TRUNCATE')
        self.truncate()
        self.db_engine.load_table_from_dataframe(self.df, f'''{self.dataset}{self.table_name}''', loadjob).result()
        logging.info(f'''Replaced table, {self.table_name} now up to date''')



