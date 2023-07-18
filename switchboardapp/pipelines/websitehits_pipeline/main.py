
import os
from dotenv import load_dotenv
from switchboard_py import http_trigger, Caller
from utils import (
run_pipeline,
WebsiteEndpoint
)






@http_trigger
def main(request):

    request_body = request.get_json()

    data = [
    WebsiteEndpoint(request_body)
    ]

    manual = [
        
    ]

    # run_pipeline(data, manual)
    print(data[0].payload)
    
    load_dotenv('sb_endpoint.env')
    sb_endpoint = os.environ.get('SWITCHBOARD')
    caller_name = 'pipeline_boilerplate'
    caller_type = 'pipeline_completetion'
    payload = {'datafield': 'some data'}

    send_conf_message = Caller(sb_endpoint, caller_name, caller_type, payload)
    send_conf_message.invoke()



