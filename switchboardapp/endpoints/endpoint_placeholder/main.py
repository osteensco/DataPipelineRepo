
import os
from dotenv import load_dotenv
from switchboard_py import http_trigger, Caller

@http_trigger
def main(request):
    
    #do some data stuff

    load_dotenv('sb_endpoint.env')
    sb_endpoint = os.environ.get('SWITCHBOARD')
    caller_name = 'endpoint_boilerplate'
    caller_type = 'cron'
    payload = request_body = request.get_json()

    call_switchboard = Caller(sb_endpoint, caller_name, caller_type, payload)
    call_switchboard.invoke()
    