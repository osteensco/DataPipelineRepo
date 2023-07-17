import pandas as pd
import requests
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














class WebsiteEndpoint(DataSource):
    def __init__(self, payload) -> None:
        super().__init__()
        self.source = '''www.scottosteen.com'''
        self.format = 'json'
        self.table_name = 'Portfolio_Website_Traffic'
        self.dtypes = [
            bigquery.SchemaField('TimeStamp', 'TIMESTAMP'),
            bigquery.SchemaField('PlainTextTS', 'STRING'),
            bigquery.SchemaField('ID', 'STRING'),
            bigquery.SchemaField('Session', 'STRING'),
            bigquery.SchemaField('Page', 'STRING'),
            bigquery.SchemaField('Referrer', 'STRING'),
            bigquery.SchemaField('Device', 'STRING'),
            bigquery.SchemaField('Language', 'STRING')
        ]
        self.payload = payload

    def schedule(self):
        super().schedule()
        return True

    def extract(self):
        logging.info(f'''{type(self).__name__} Payload: \n{self.payload}''' )
        self.df = pd.read_json(self.payload)
        
    def load(self):
        #land in appropriate tables
        loadjob = bigquery.LoadJobConfig(schema=self.dtypes)
        loadjob.write_disposition = 'WRITE_APPEND'
        loadjob.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
        self.db_engine.load_table_from_dataframe(self.df, f'''{self.dataset}{self.table_name}''', loadjob).result()
        logging.info(f'''{type(self).__name__} loaded into {self.table_name}''' )






