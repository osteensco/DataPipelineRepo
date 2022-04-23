import datetime
import logging







class DataSource:
    def __init__(self, source, dataformat) -> None:
        self.source = source
        self.format = dataformat
        self.df = None
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

    def extract(self):#will super().__init__() run extract for child class? I think so
        pass

    def clean(self):
        pass

    def load(self, secret):
        #attain secrets from aws secret manager
        #connect to aws rds database
        #land in appropriate tables
        pass
        
class WeatherData(DataSource):
    def __init__(self) -> None:
        super().__init__()

class GeoData(DataSource):
    def __init__(self) -> None:
        super().__init__()


class Pipeline:
    def __init__(self, sources) -> None:
        self.timestamp = datetime.datetime.now()
        self.data_objs = self.schedule(sources)
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
            data.extract()
            data.clean()
            data.load(self.creds)




if __name__ == '__main__':
    pass