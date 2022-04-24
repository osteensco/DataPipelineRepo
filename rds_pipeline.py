import datetime
import logging
import pandas as pd
import requests
import time




class DataSource:
    def __init__(self) -> None:
        self.source = None
        self.format = None
        self.df = pd.DataFrame()
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

    def load(self, secret):
        #attain secrets from aws secret manager
        #connect to aws rds database
        #land in appropriate tables
        pass

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
        #read state abbreviations from database?

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