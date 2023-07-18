
import os
from dotenv import load_dotenv
from switchboard_py import http_trigger, Caller



@http_trigger
def main(event, context):

    load_dotenv('sb_endpoint.env')
    sb_endpoint = os.environ.get('SWITCHBOARD')
    caller_name = 'daily'
    caller_type = 'cron'
    payload = None

    call_switchboard = Caller(sb_endpoint, caller_name, caller_type, payload)
    call_switchboard.invoke()
    