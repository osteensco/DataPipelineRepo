import datetime
import logging
import pandas as pd
import requests
import time
import bs4 as bs
from sqlalchemy import create_engine, types
import boto3
import base64
from botocore.exceptions import ClientError
import json


#Notes:
    #use engine.connect() to open db connection https://docs.sqlalchemy.org/en/14/core/connections.html
    #next steps:
        #build schedule method for geodata
            #add date_pulled column for geodata
        #set up amazon secrets, write retrieval method
            #secret is key-value paired, pipeline will ingest and store as self.secrets
        #add field to geodata table containing date or create separate table to track pull dates?
        #once schedule method works, test feeding geodata through pipeline









class DataSource:
    def __init__(self) -> None:
        self.source = None
        self.format = None
        self.df = pd.DataFrame()
        self.db_engine = None
        self.table_name = None
        self.dtypes = {}
        self.destination = None

    def schedule(self):
        # used for determine if data should be ingested
        # Returns True/False
        # Create SQLAlchemy engine for connection to MySQL Database
        self.db_engine = create_engine(f'''mysql://{self.secret[2]}:{self.secret[3]}@{self.secret[0]}/{self.secret[1]}''')
        # Child DataSource objects will have specific queries to determine the boolean value to return

    def extract(self):
        pass

    def clean(self):
        pass

    def load(self):

        #land in appropriate tables
        self.df.to_sql(self.table_name, self.db_engine, if_exists='append', index=False, dtype=self.dtypes)




        
class WeatherData(DataSource):
    def __init__(self, states) -> None:
        super().__init__()
        self.source = '''http://api.weatherapi.com/v1/history.json'''
        self.format = 'json'
        self.states = states
        self.weather_data_col = ['totalprecip_in']#columns from weather data we want to use
        self.table_name = 'Daily_Weather'
        self.dtypes = {i: types.FLOAT for i in self.weather_data_col}
        self.yesterday = datetime.date.today() - datetime.timedelta(days=1)#get yesterdays date (yyyy-mm-dd)
        self.last_pull = self.retrieve_last_pull()
        self.requests = self.retrieve_monthly_req()
        self.APIkey = None
        self.zipcodes = self.retrieve_zips()

    def retrieve_last_pull(self):
        #query database to determine when last pull was
        pass

    def retrieve_monthly_req(self):
        #query database to determine requests made month to date
        pass

    def retrieve_zips(self):
        #query database geo data, return list of zip codes based on self.states
        pass

    def schedule(self):
        super().schedule()
    #get current date
    #check last run for each source, then determine if run should be scheduled
        #read table data source lands in to find last run
        #datesubtract to determine if another run should be scheduled
    #identify by boolean value if data source should be fed through pipeline
        #returns True/False
        return None

    def check_req_limit(self):
        if datetime.date.today().strftime("""%d""") == "01":
        #Replace with: if month(self.last_pull) > current month
            #determine if a pull has been made this month or not
            #if not:
            self.requests = 0
            logging.info('first of the month, monthly request limit reset, may want to verify manually')
        else:
            pass

    def extract(self):
        for zip in self.zipcodes:
            try:
                result = requests.get(url=f'''{self.source}?key={self.APIkey}&q={zip}&dt={self.yesterday}''')#create response obj
                result.raise_for_status()
            except requests.exceptions.HTTPError:
                logging.info(f'''{zip} is an invalid zipcode according to weatherapi, skipping...''')
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

            self.df = self.clean_and_append(result.json, zip)

    def clean_and_append(self, json_dict, zip):#specific for weather data
        json_1 = json_dict["forecast"]["forecastday"][0]["day"]
        cleaned_json_result = {k:json_1[k] for k in json_1 if k != 'condition'}
        cleaned_json_result["ZIP Code"] = zip
        cleaned_json_result["Date"] = self.yesterday
        result_df = pd.DataFrame(cleaned_json_result, index=[0]).astype('str')#need to pass index because it's single row dict
        result_df[self.weather_data_col] = result_df[self.weather_data_col].astype('float')
        return self.df.append(result_df)





class GeoData(DataSource):
    def __init__(self) -> None:
        super().__init__()
        self.source = '''https://www.unitedstateszipcodes.org/'''
        self.format = 'webscraped html to json'
        self.table_name = 'US_Zips_Counties'
        self.dtypes = {'ZIP Code': types.String, 'County': types.String, 'State': types.String}
        self.df = pd.DataFrame(columns=['ZIP Code', 'County', 'State'], dtype=str)
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
    #get current date
    #check last run for each source, then determine if run should be scheduled
        #read table data source lands in to find last run
        #datesubtract to determine if another run should be scheduled
    #identify by boolean value if data source should be fed through pipeline
        #returns True/False
        return None

    def extract(self):
        for state in self.States:
            r = requests.get(url=f'''https://www.unitedstateszipcodes.org/{state.lower()}/#zips-list''', 
            headers={'''User-Agent''': '''Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36'''})
            try:
                r.raise_for_status()
            except Exception as ex:
                logging.error(getattr(ex, 'message', repr(x)))
                continue
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
                table = {'ZIP Code': zips, 'County': counties, 'State': sts}#attach our data to column names
                df1 = pd.DataFrame(data=table, dtype=str)#put in dataframe to easily append
                df1['Date Pulled'] = datetime.date.today()#add date column
                self.df = self.df.append(df1)#add to our dataframe
                logging.info(f'''{state} Zips and Counties scraped successfully!''')






class Pipeline:
    def __init__(self, sources) -> None:
        self.timestamp = datetime.datetime.now()
        self.data_objs = sources
        # self.init_log()
        #logs land in a table in rds database as well
        self.secrets = self.retrieve_secrets()
        # self.run()

    def init_log(self):
        logging.basicConfig(filename=self.log, filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

    def retrieve_secrets(self):#attain secrets from aws secret manager
        secret_name = "pipeline"
        # Create a Secrets Manager client
        session = boto3.session.Session()
        with session.client(service_name='secretsmanager') as client:
        # error info see https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            try:
                get_secret_value_response = client.get_secret_value(
                    SecretId=secret_name
                )
            except ClientError as e:
                if e.response['Error']['Code'] == 'DecryptionFailureException':
                    # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                    # Deal with the exception here, and/or rethrow at your discretion.
                    raise e
                elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                    # An error occurred on the server side.
                    # Deal with the exception here, and/or rethrow at your discretion.
                    raise e
                elif e.response['Error']['Code'] == 'InvalidParameterException':
                    # You provided an invalid value for a parameter.
                    # Deal with the exception here, and/or rethrow at your discretion.
                    raise e
                elif e.response['Error']['Code'] == 'InvalidRequestException':
                    # You provided a parameter value that is not valid for the current state of the resource.
                    # Deal with the exception here, and/or rethrow at your discretion.
                    raise e
                elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                    # We can't find the resource that you asked for.
                    # Deal with the exception here, and/or rethrow at your discretion.
                    raise e
            else:
                # Decrypts secret using the associated KMS key.
                # Depending on whether the secret is a string or binary, one of these fields will be populated.
                if 'SecretString' in get_secret_value_response:
                    secret = get_secret_value_response['SecretString']
                else:
                    secret = base64.b64decode(get_secret_value_response['SecretBinary'])
                secret = json.loads(secret)#convert to dictionary

        #assign secrets to appropriate attributes
        for obj in self.data_objs:
            #if statements to assign api keys to appropriate data objects
            if obj.table_name == 'Daily_Weather':
                obj.APIKey = secret['weatherapi']

        self.hostname = secret['dbhost']
        self.dbname = secret['dbname']
        self.uname = secret['dbuser']
        self.pwd = secret['dbpass']

        return [self.hostname, self.dbname, self.uname, self.pwd]

    def run(self):
        for data in self.data_objs:
            # pass secrets to object for db connection
            data.secrets = self.secrets
            #open connection to db, query db to determine if data source is scheduled to be ingested
            if data.schedule():
                data.extract()
                data.load()




if __name__ == '__main__':


    # data = [
    #     GeoData()

    #     ]


    # aws_rds_data_pipeline = Pipeline(data)
    print('Complete')