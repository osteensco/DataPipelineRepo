import datetime
import logging
import pandas as pd
import requests
import time
import bs4 as bs
from sqlalchemy import create_engine, types


class DataSource:
    def __init__(self) -> None:
        self.source = None
        self.format = None
        self.df = pd.DataFrame()
        self.table_name = None
        self.dtype = {}
        self.ingested_data = None
        self.destination = None
        self.scheduled = self.schedule()

    def schedule(self):
        #get current date
        #check last run for each source, then determine if run should be scheduled
            #read table data source lands in to find last run
            #datesubtract to determine if another run should be scheduled
        #identify by boolean value if data source should be fed through pipeline
            #returns True/False
        return None

    def extract(self):
        pass

    def clean(self):
        pass

    def load(self, secret):#move inside pipeline?
        #attain secrets from aws secret manager
        secret_hostname = ''
        secret_db = ''
        secret_uname = ''
        secret_pass = ''
        #connect to aws rds database
        hostname=secret_hostname
        dbname=secret_db
        uname=secret_uname
        pwd=secret_pass

        # Create SQLAlchemy engine for connection to MySQL Database
        engine = create_engine(f'''mysql://{uname}:{pwd}@{hostname}/{dbname}''')

        #land in appropriate tables
        self.df.to_sql(self.table_name, engine, if_exists='append', index=False, dtype=self.dtype)


    def pull(self):
        pass
        
class WeatherData(DataSource):
    def __init__(self, states) -> None:
        super().__init__()
        self.source = '''http://api.weatherapi.com/v1/history.json'''
        self.format = 'json'
        self.states = states
        self.weather_data_col = ['totalprecip_in']#columns from weather data we want to use
        self.yesterday = datetime.date.today() - datetime.timedelta(days=1)#get yesterdays date (yyyy-mm-dd)
        self.last_pull = self.retrieve_last_pull()
        self.requests = self.retrieve_monthly_req()
        self.APIkey = self.retrieve_API_key()
        self.zipcodes = self.retrieve_zips()

    def retrieve_last_pull(self):
        #query database to determine when last pull was
        pass

    def retrieve_monthly_req(self):
        #query database to determine requests made month to date
        pass

    def retrieve_API_key(self):
        pass

    def retrieve_zips(self):
        #query database geo data, return list of zip codes based on self.states
        pass

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

    def pull(self):
        self.extract()




class GeoData(DataSource):
    def __init__(self) -> None:
        super().__init__()
        self.source = '''https://www.unitedstateszipcodes.org/'''
        self.format = 'webscraped html to json'
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
        #read state abbreviations from database?

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
                self.df = self.df.append(df1)#add to our dataframe
                logging.info(f'''{state} Zips and Counties scraped successfully!''')






class Pipeline:
    def __init__(self, sources) -> None:
        self.timestamp = datetime.datetime.now()
        self.data_objs = sources
        #logging
        #self.log = 'pipelinelog.txt'
        #self.creds = obtain secret from aws secrets
        self.init_log()
        #logs land in a table in rds database as well
        self.run()

    def init_log(self):
        logging.basicConfig(filename=self.log, filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

    def run(self):
        for data in self.data_objs:
            if data.scheduled:
                data.pull()
                data.load(self.creds)




if __name__ == '__main__':
    pass