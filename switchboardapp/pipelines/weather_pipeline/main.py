
import os
from dotenv import load_dotenv
from switchboard_py import http_trigger, Caller
from utils import (
run_pipeline,
WeatherData
)






@http_trigger
def main(request):
    
    data = [
    WeatherData(['GA'])
    ]

    manual = [
        
    ]
    
    run_pipeline(data, manual)

    load_dotenv('sb_endpoint.env')
    sb_endpoint = os.environ.get('SWITCHBOARD')
    caller_name = 'weather_pipeline'
    caller_type = 'pipeline_completetion'
    payload = {'datafield': 'some data'}

    send_conf_message = Caller(sb_endpoint, caller_name, caller_type, payload)
    send_conf_message.invoke()
    


