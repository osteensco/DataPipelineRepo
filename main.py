from rds_pipeline import run_pipeline
from datasources import GeoData, WeatherData, WebsiteEndpoint




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
    WebsiteEndpoint(event.data)
    ]

    manual = [
        
    ]

    run_pipeline(data, manual)




