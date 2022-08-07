from rds_pipeline import run_pipeline
from datasources import GeoData, WeatherData, WebsiteEndpoint
import base64



def weather_pipeline(event, context):
    data = [
    WeatherData(['GA'])
    ]

    manual = [
        
    ]
    
    run_pipeline(data, manual)



def uslocations_pipeline(event, context):
    data = [
    GeoData()
    ]

    manual = [
        
    ]

    run_pipeline(data, manual)



def websitehits_pipeline(event, context):
    data = [
    WebsiteEndpoint(base64.b64decode(event['data']).decode('utf-8'))
    ]

    manual = [
        
    ]

    run_pipeline(data, manual)




