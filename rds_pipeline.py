import datetime
import logging
from sqlalchemy import types
import boto3
import base64
from botocore.exceptions import ClientError
import json
from datasources import GeoData, WeatherData
import sys


#Notes:
    #https://docs.sqlalchemy.org/en/14/core/connections.html
    
    #next steps:
        #finish lambda deployment
        #testing and adding try/excepts
        #create git branching for continuous development
        #determine means of deployment













class Pipeline:
    def __init__(self, sources, forcedupdatesources=[]) -> None:
        self.timestamp = datetime.datetime.now()
        self.data_objs = sources
        self.override_scheduling = forcedupdatesources
        self.dtype_convert = {
                            'FLOAT': types.FLOAT,
                            'STRING': types.String,
                            'INT': types.INTEGER,
                            'DATE': types.DATE,
                            'BOOLEAN': types.Boolean
                            }
        self.init_log()
        #add method so that logs land in a table in rds database as well
        self.secrets = self.retrieve_secrets()
        self.run()

    def init_log(self):
        if logging.getLogger().hasHandlers():
            logging.getLogger().setLevel(logging.INFO)
        else:    
            logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    def retrieve_secrets(self):#attain secrets from aws secret manager
        secret_name = "pipeline"
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager')
        # error info see https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
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
        allobjs = self.data_objs + self.override_scheduling
        for obj in allobjs:
            #if statements to assign api keys to appropriate data objects
            if obj.table_name == 'Daily_Weather':
                obj.APIkey = secret['weatherapi']

        self.hostname = secret['dbhost']
        self.dbname = secret['dbname']
        self.uname = secret['dbuser']
        self.pwd = secret['dbpass']

        return [self.hostname, self.dbname, self.uname, self.pwd]

    def manual_schedule(self):#identify data sources that should bypass schedule method
        if self.override_scheduling:
            for data in self.override_scheduling:
                data.scheduled = True
                self.data_objs.append(data)
                logging.info(f'''{type(data).__name__} manual pull, scheduled''')
        else:
            pass

    def schedule(self):
        #open connection to db, query db to determine if data source is scheduled to be ingested
        #also passes secrets to object for db connection
        for data in self.data_objs:
            if data not in self.override_scheduling:
                data.scheduled = data.schedule(self.secrets)
            else:
                #if overriding scheduling, we don't assign result
                #method still needs to be called to query data needed for pull
                data.schedule(self.secrets)

    def run(self):
        self.manual_schedule()
        self.schedule()
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
