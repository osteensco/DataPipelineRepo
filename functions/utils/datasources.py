import datetime
import json
import pandas as pd
import requests
import time
import bs4 as bs
from google.cloud import bigquery
from google.api_core import exceptions as gbq
import logging
import sys
import re


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
        # used for determine if data should be ingested
        # Returns True/False
        self.db_engine = bigquery.Client('portfolio-project-353016')
        # Child DataSource objects will have specific queries to determine the boolean value to return

    def extract(self):
        pass

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
        query = f'''TRUNCATE TABLE `{self.dataset}{self.table_name}`'''
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
        return False
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
            r.raise_for_status()
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
            if len(counties) != len(zips):#these should always equal, and not be 0
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
        query = f"""SELECT * FROM `{self.dataset}{self.table_name}`"""
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
        query = f"""SELECT * FROM `{self.dataset}{self.table_name}` WHERE YEAR IN {tuple(years) if len(years) > 1 else f'''('{years[0]}')'''}"""
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





class CFGameTeamStats(DataSource):
    '''
    Note:\n
    \t This object is dependant on data pulled from GFGames object.\n
    \t If no data is in Power_5_Games table then no data will be pulled by this object.
    '''
    def __init__(self, overwrite=False) -> None:
        super().__init__()
        self.source = 'https://www.espn.com/college-football/matchup?gameId='
        self.overwrite = overwrite
        self.format = 'webscraped html'
        self.table_name = 'Game_Team_Stats'
        self.dtypes = [
            bigquery.SchemaField('GameID', 'STRING'),
            bigquery.SchemaField('Team', 'STRING'),
            bigquery.SchemaField('isHome', 'BOOL'),
            bigquery.SchemaField('Points', 'STRING'),
            bigquery.SchemaField('FirstDowns', 'STRING'),
            bigquery.SchemaField('ThirdDownEff', 'STRING'),
            bigquery.SchemaField('FourthDownEff', 'STRING'),
            bigquery.SchemaField('TotalYds', 'STRING'),
            bigquery.SchemaField('PassYds', 'STRING'),
            bigquery.SchemaField('PassCompAtt', 'STRING'),
            bigquery.SchemaField('PassIntThrown', 'STRING'),
            bigquery.SchemaField('RushYds', 'STRING'),
            bigquery.SchemaField('RushAtt', 'STRING'),
            bigquery.SchemaField('Penalties', 'STRING'),
            bigquery.SchemaField('FumblesLost', 'STRING'),
            bigquery.SchemaField('PossTime', 'STRING'),
        ]
        self.dataset = '''portfolio-project-353016.collegeFootball.'''

    def schedule(self):
        super().schedule()
        if not self.overwrite and not self.scheduled:
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
        allgamestats = []
        if not self.overwrite:
        #query will join stats table to game table to generate a list of game ids that we dont stat for one or both teams
            query = f"""
                With CurrStatsCTE AS (
                    SELECT
                        GameID,
                        COUNT(DISTINCT Team) AS cntcheck
                    FROM `{self.dataset}{self.table_name}`
                    GROUP BY 1
                    )
                SELECT
                    GameID
                FROM (
                    SELECT
                        GameID,
                        cntcheck
                    FROM `{self.dataset}Power_5_Games`
                    LEFT JOIN CurrStatsCTE
                    USING(GameID)
                    )
                WHERE cntcheck < 2 
                OR cntcheck IS NULL
            """
        else:
            #if overwriting everything we just want to pull
            query = f"""
                SELECT
                    GameID
                FROM `{self.dataset}Power_5_Games`
            """
        gms = self.db_engine.query(query).result().to_dataframe()
        gameids = [i for i in gms['GameID']]

        for id in gameids:
            game = {
                'home': {
                    'GameID': id,
                    'Team': '',
                    'isHome': True,
                    'Points': '', 
                    '1st Downs': '',
                    '3rd down efficiency': '',
                    '4th down efficiency': '',
                    'Total Yards': '',
                    'Passing': '',
                    'Comp-Att': '',
                    'Interceptions thrown': '',
                    'Rushing': '',
                    'Rushing Attempts': '',
                    'Penalties': '',
                    'Fumbles lost': '',
                    'Possession': ''
                    }, 
                'away': {
                    'GameID': id,
                    'Team': '',
                    'isHome': False,
                    'Points': '',
                    '1st Downs': '',
                    '3rd down efficiency': '',
                    '4th down efficiency': '',
                    'Total Yards': '',
                    'Passing': '',
                    'Comp-Att': '',
                    'Interceptions thrown': '',
                    'Rushing': '',
                    'Rushing Attempts': '',
                    'Penalties': '',
                    'Fumbles lost': '',
                    'Possession': ''
                    }
                }
            teams = game.keys()       
            path = f'''{self.source}{id}'''
            r = self.getreq(path) 
            #bail out if espn blocks us
            try:
                r.raise_for_status()
                bsobj = bs.BeautifulSoup(r.text, 'html.parser')
            except:
                pass#if page does not exist pass to next try/except blocks to autofill unavail data
            #grab all stats from table, score and team name not in table
            try:
            #sometimes espn is missing team stats for a game, page exists but table does not - we mark these as "unavail" so they are easily queried
            #we want a record of the game, who played, and the score
                soup = bsobj.find('section', id="main-container").find('div', id="gamepackage-matchup").find('tbody').find_all('tr')
                for s in soup:    
                    stat = s.find_all('td')
                    st = stat[0].get_text().strip()#stat[0] is stat name
                    if st in game['away'].keys():
                        game['away'][st] = stat[1].get_text().translate({ord(char): None for char in ['\n', '\t']})#stat[1] is away teams stats
                        game['home'][st] = stat[2].get_text().translate({ord(char): None for char in ['\n', '\t']})#stat[2] is home teams stats
            except (AttributeError, UnboundLocalError):
                for tm in teams:
                    for k in [key for key in game[tm].keys() if key not in ['Team', 'Points', 'GameID', 'isHome']]:
                        game[tm][k] = 'unavail'
            #grab team name, score, add completed dictionary to our list
            for t in teams:
                try:
                    sp = bsobj.find('div', class_=f'team {t}')
                    game[t]['Team'] = f"""{sp.find('span', class_="long-name").get_text()} {sp.find('span', class_="short-name").get_text()}"""
                    game[t]['Points'] = sp.find('div', class_='score-container').find('div', class_=f"""score icon-font-{'after' if t=='away' else 'before'}""").get_text()
                    allgamestats.append(game[t])
                except (AttributeError, UnboundLocalError) as er:
                    game[t]['Team'] = t
                    game[t]['Points'] = 'unavail'
                    logging.error(f'''{getattr(er, 'message', repr(er))} \n path: {path}, t: {t}, game: {game}''')
                    allgamestats.append(game[t])
        #place everything in a dataframe
        self.df = self.df.from_records(allgamestats)
        self.df = self.mapfields(self.df)

    def mapfields(self, df):
        fieldmap = {
            '1st Downs': 'FirstDowns',
            '3rd down efficiency': 'ThirdDownEff',
            '4th down efficiency': 'FourthDownEff',
            'Total Yards': 'TotalYds',
            'Passing': 'PassYds',
            'Comp-Att': 'PassCompAtt',
            'Interceptions thrown': 'PassIntThrown',
            'Rushing': 'RushYds',
            'Rushing Attempts': 'RushAtt',
            'Fumbles lost': 'FumblesLost',
            'Possession': 'PossTime'
        }
        df=df.rename(index=str, columns=fieldmap)
        return df

    def load(self):
        self.testitem = self.df.shape[0]
        if self.df.shape[0] > 0:
            loadjob = bigquery.LoadJobConfig(schema=self.dtypes)
            if self.overwrite:
                self.truncate()
                loadjob.write_disposition = 'WRITE_TRUNCATE'
            else:
                loadjob.write_disposition = 'WRITE_APPEND'
            loadjob.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            self.db_engine.load_table_from_dataframe(self.df, f'''{self.dataset}{self.table_name}''', loadjob).result()
            logging.info(f'''{type(self).__name__} loaded into {self.table_name}''' )
        else:
            logging.info('''No new data to pull''')





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








if __name__ == '__main__':
#add any tests here
    run = CFGameTeamStats()
    run.test()
    # run.load()


