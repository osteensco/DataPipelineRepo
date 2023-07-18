import datetime
import pandas as pd
import requests
import bs4 as bs
from google.cloud import bigquery
import logging








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


