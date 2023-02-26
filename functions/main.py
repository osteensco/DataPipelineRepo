from rds_pipeline import run_pipeline
from datasources import (
GeoData, 
WeatherData, 
WebsiteEndpoint, 
CFTeamsAll, 
CFGames, 
CFGameTeamStats
)
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
    payload = base64.b64decode(event['data']).decode('utf-8')

    data = [
    WebsiteEndpoint(payload)
    ]

    manual = [
        
    ]

    run_pipeline(data, manual)



def cf_pwr5teams_pipeline(event, context):
    data = [
    CFTeamsAll()
    ]

    manual = [

    ]

    run_pipeline(data, manual)



def cf_games_pipeline(event, context):
    data = [
    CFGames()
    ]

    manual = [

    ]

    run_pipeline(data, manual)



def cf_gamestats_pipeline(event, context):
    data = [
    CFGameTeamStats()
    ]

    manual = [

    ]

    run_pipeline(data, manual)