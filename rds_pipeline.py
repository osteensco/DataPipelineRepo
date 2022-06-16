import datetime
import logging
from google.cloud import bigquery
from datasources import GeoData, WeatherData



#Notes:
    
    #next steps:

        #testing and adding try/excepts
        #create git branching for continuous development
        #look at github actions for deployment automation


#gcloud auth application-default login










class Pipeline:
    def __init__(self, sources, forcedupdatesources=[]) -> None:
        self.timestamp = datetime.datetime.now()
        self.data_objs = sources
        self.override_scheduling = forcedupdatesources
        self.dtype_convert = {
                            'FLOAT': bigquery.enums.SqlTypeNames.FLOAT,
                            'STRING': bigquery.enums.SqlTypeNames.STRING,
                            'INT': bigquery.enums.SqlTypeNames.INTEGER,
                            'DATE': bigquery.enums.SqlTypeNames.DATE,
                            'BOOLEAN': bigquery.enums.SqlTypeNames.BOOLEAN
                            }
        self.init_log()
        #add method so that logs land in a table in database as well
        self.run()

    def init_log(self):
        if logging.getLogger().hasHandlers():
            logging.getLogger().setLevel(logging.INFO)
        else:    
            logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    def retrieve_secrets(self):#attain API Keys
        allobjs = self.data_objs + self.override_scheduling
        for obj in allobjs:
            #Assign APIkey if one is needed
            if obj.APIkey:
                query = f"""SELECT API_KEY FROM `portfolio-project-353016.APIKEYS.KEYS` WHERE TBL_NM = '{obj.table_name}' """
                obj.APIkey = obj.db_engine.query(query).result().to_dataframe()
            else:
                continue


    def manual_schedule(self):#identify data sources that should bypass schedule method
        if self.override_scheduling:
            for data in self.override_scheduling:
                data.scheduled = True
                self.data_objs.append(data)
                logging.info(f'''{type(data).__name__} manual pull, scheduled''')
        else:
            pass

    def schedule(self):#schedule pulls from DataSource objects
        for data in self.data_objs:
            if data not in self.override_scheduling:
                data.scheduled = data.schedule()
            else:
                #if overriding scheduling, don't assign result
                #method still needs to be called to query data needed for pull
                data.schedule()

    def run(self):
        self.manual_schedule()
        self.schedule()
        self.retrieve_secrets()
        for data in self.data_objs:
            if data.scheduled:
                data.extract()
                data.load(self.dtype_convert)





def run_pipeline():
    data = [
        GeoData(),
        WeatherData(['GA'])
        ]

    manual = [
        
    ]

    Pipeline(sources=data, forcedupdatesources=manual)
    print('Complete')

if __name__ == '__main__':

    run_pipeline()

