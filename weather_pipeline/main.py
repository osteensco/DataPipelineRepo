from objs.rds_pipeline import run_pipeline
from objs.datasources import WeatherData




def weather_pipeline(event, context):
    data = [
    WeatherData(['GA'])
    ]

    manual = [
        
    ]
    
    run_pipeline(data, manual)



