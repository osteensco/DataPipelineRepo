
import os
from dotenv import load_dotenv
from switchboard_py import http_trigger, Caller
import base64



@http_trigger
def main(event, context):

    load_dotenv('sb_endpoint.env')
    sb_endpoint = os.environ.get('SWITCHBOARD')
    caller_name = 'portfolio_site_traffic'
    caller_type = 'webhook'
    payload = base64.b64decode(event['data']).decode('utf-8')

    call_switchboard = Caller(sb_endpoint, caller_name, caller_type, payload)
    call_switchboard.invoke()
    