import datetime
import logging






class Pipeline:
    def __init__(self, sources, forcedupdatesources=[]) -> None:
        self.timestamp = datetime.datetime.now()
        self.data_objs = sources
        self.override_scheduling = forcedupdatesources
        self.init_log()
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
                obj.APIkey = obj.db_engine.query(query).result().to_dataframe()['API_KEY'].tolist()[0]
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
                data.load()





class SQL(Pipeline):
    def __init__(self, sources, forcedupdatesources=[]) -> None:
        super().__init__(sources, forcedupdatesources)

    def run(self): 
        self.manual_schedule()
        self.schedule()
        for query in self.data_objs:
            if query.scheduled:
                query.run()





def run_pipeline(data, manual):


    Pipeline(sources=data, forcedupdatesources=manual)
    print('Complete')


#TODO
##refactor to better handle testing before pushing builds to production
##differentiate ingestion, transfer, and transform (sql) pipelines? datasources instead?
    ####use SwitchBoard framework for orchestration
    ####SQL datasource that will just run a given query via cloud function. use stored procedures when there's no need to provide table addresses via dependency injection.



##SwitchBoard framework:
    ####PubSub triggers GCF endpoints.
    ####Trigger endpoints will trigger SwitchBoard GCF via HTTP.
    ####SwitchBoard contains graph data structure that will trigger appropriate pipelines via HTTP.
        ###SwitchBoard will reference .json file in cloud storage for any additional dependencies that should be considered.
        ###When a pipeline is triggered a 200 response is returned immediately to identify a successful trigger.
        ###Any failures will exist in logs of pipeline GCF.
    ####On completion of pipeline GCF, SwitchBoard will be triggered to communicate successful run.
        ###SwitchBoard will update .json file in cloud storage once successful pipeline run is communicated to it.










# def run_sql_pipeline(data, manual):


#     SQL(sources=data, forcedupdatesources=manual)
#     print('Complete')


if __name__ == '__main__':
    pass

