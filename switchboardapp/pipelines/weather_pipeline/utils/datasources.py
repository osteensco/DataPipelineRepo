import datetime
import pandas as pd
import requests
import time
from google.cloud import bigquery
from google.api_core import exceptions as gbq
import logging
import sys






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










#data source objects        


class WeatherData(DataSource):
    def __init__(self, states) -> None:
        super().__init__()
        self.source = '''http://api.weatherapi.com/v1/history.json'''
        self.format = 'json'
        self.states = states #list of state abbreviation strings, all caps
        self.weather_data_col = ['totalprecip_in']#columns from weather data we want to use
        self.table_name = 'Daily_Weather'
        self.dtypes = [
                        bigquery.SchemaField('ZIP_Code', 'STRING'),
                        bigquery.SchemaField('Date', 'DATE')        
                    ] + [bigquery.SchemaField(i, 'FLOAT') for i in self.weather_data_col]
        self.yesterday = datetime.date.today() - datetime.timedelta(days=1)#get yesterdays date (yyyy-mm-dd)
        self.APIkey = True


    def retrieve_last_pull(self):
        query = """SELECT table_id FROM `portfolio-project-353016.ALL.__TABLES__`"""
        tbls = self.db_engine.query(query).result().to_dataframe()
        tbls = tbls['table_id'].tolist()
        if self.table_name in tbls:
            query = f'''SELECT MAX(Date) AS dt FROM `{self.dataset}{self.table_name}` '''
            result = self.db_engine.query(query).result().to_dataframe()
            result = result['dt'].tolist()[0]
            return result
        else:
            return None

    def retrieve_monthly_req(self):
        curr_month = datetime.date.today().month
        curr_year = datetime.date.today().year
        #determine if a pull has been made this month or not
        if self.last_pull:
            if (self.last_pull.month < curr_month
            or self.last_pull.year < curr_year):    
            #if last pull is not this month or this year (account for year change):
                logging.info(f'{self.table_name} first pull of the month, monthly request limit reset, may want to verify manually')
                return 1000000   
            else:
                #query database to determine requests made month to date
                query = f'''SELECT COUNT(*) AS cnt FROM `{self.dataset}{self.table_name}` WHERE EXTRACT(MONTH FROM Date) = {curr_month}'''
                result = self.db_engine.query(query).result().to_dataframe()
                #subtract from monthly limit
                result = result['cnt'].tolist()[0]
                reqs = 1000000 - result - len(self.zipcodes)
                #return number of requests available
                return reqs
        else:
            return 1000000

    def retrieve_zips(self, st):
        #query database geo data, return list of zip codes based on self.states
        query = f'''SELECT ZIP_Code FROM `portfolio-project-353016.ALL.US_Zips_Counties` WHERE State = '{st}' '''
        result = self.db_engine.query(query).result().to_dataframe()
        result = result['ZIP_Code'].tolist()
        return result

    def schedule(self):
        super().schedule()
        #determine last data ingestion
        # with self.db_engine.connect() as connection:
        self.last_pull = self.retrieve_last_pull()
        if self.scheduled:#if already scheduled by override, return False for logic check in load method
            if self.df.shape[0] > 0:#exit ramp for logic in load method
                return False
            self.overwrite = self.yesterday
            self.zipcodes = []
            for state in self.states:
                self.zipcodes += self.retrieve_zips()
            #determine if enough requests are available for another pull
            self.requests = self.retrieve_monthly_req()
            if self.requests > 0:
                logging.info(f'''enough requests for {type(self).__name__}, continuing''')
            else:
                logging.warning(f'''Not enough {type(self).__name__} requests available for month, new data will not be pulled.''')
                self.scheduled = False #override manual schedule if not enough requests available
        else:
            #determine if it was run for yesterday's data
            if not self.last_pull or self.last_pull < self.yesterday:
                #grab list of zipcodes to pass to api calls
                self.zipcodes = []
                for state in self.states:
                    try:
                        self.zipcodes += self.retrieve_zips(state)
                    except gbq.NotFound:
                        logging.info(f'''{type(self).__name__} not scheduled, US_Zips_Counties table missing''')
                        return False
                #determine if enough requests are available for another pull
                self.requests = self.retrieve_monthly_req()
                if self.requests > 0:
                    logging.info(f'''{type(self).__name__} scheduled''')
                    return True
                else:
                    logging.warning(f'''Not enough {type(self).__name__} requests available for month, new data will not be pulled.''')
                    return False
            else:
                logging.info(f'''{type(self).__name__} not scheduled, data already up to date''')
                return False

    def extract(self):
        logging.info(f"""Requesting API for {len(self.zipcodes)} zip codes\n""")
        counter = 0
        for zip in self.zipcodes:
            sys.stdout.flush()
            try:
                result = requests.get(url=f'''{self.source}?key={self.APIkey}&q={zip}&dt={self.yesterday}''')#create response obj
                result.raise_for_status()
            except requests.exceptions.HTTPError as err:
                logging.error(f'''{err}''')
                logging.info(f'''{zip} is an invalid zipcode according to weatherapi, skipping...''')
                counter += 1
                #data pull progress display
                progress = round(((counter / len(self.zipcodes)) * 100),2)
                sys.stdout.write((f'''{datetime.datetime.now().isoformat()} - WeatherData Progress: {progress}%\
                                                        \r'''
                ))
                continue
            except requests.exceptions.Timeout:
                logging.warning("timeout occured, trying again after 30 sec...")
                time.sleep(30)
                try:#try again to account for connection issue
                    result = requests.get(url=f'''{self.source}?key={self.APIkey}&q={zip}&dt={self.yesterday}''')#create response obj
                    result.raise_for_status()
                except requests.exceptions.RequestException as e:
                    logging.info(f'''timeout error occured for zip {zip}, skipping...''')
                    logging.error(e)
                    continue
            except requests.exceptions.TooManyRedirects:
                logging.error("Too many redirects, url may need to be updated")
                logging.info(f'''url to check: {self.source}?key={self.APIkey}&q={zip}&dt={self.yesterday}''')
                continue
            except requests.exceptions.RequestException as e:
                logging.info(f'''error occured for zip {zip}''')
                logging.error(e)
                continue
            #add to df
            self.df = self.clean_and_append(result.json(), zip)
            counter += 1
            #data pull progress display
            progress = round(((counter / len(self.zipcodes)) * 100),2)
            sys.stdout.write((f'''{datetime.datetime.now().isoformat()} - WeatherData Progress: {progress}%\
                                                    \r'''
            ))

    def clean_and_append(self, json_dict, zip):#specific for weather data
        #dictionary address for weather data
        #other available fields in "astro" and "hour" (replaces "day")
        json_1 = json_dict["forecast"]["forecastday"][0]["day"]
        #add and trim fields
        json_1["ZIP_Code"] = zip
        json_1["Date"] = self.yesterday
        keep_col = ["ZIP_Code", "Date"] + self.weather_data_col#determine cols to keep
        cleaned_json_result = {k: [json_1[k]] for k in json_1 if k in keep_col}#remove unwanted fields
        #place in df, set datatypes, and append
        result_df = pd.DataFrame(cleaned_json_result)
        dtypes = {"ZIP_Code": 'str', "Date": 'datetime64'} | {i: 'float' for i in self.weather_data_col}
        result_df = result_df.astype(dtypes)
        return pd.concat([self.df, result_df])



