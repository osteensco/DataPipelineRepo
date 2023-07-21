
import os
from dotenv import load_dotenv
from switchboard_py import http_trigger, SwitchBoard, connect_to_bucket, GCP
import json




bucket = connect_to_bucket(GCP)

@http_trigger
def main(request):

    load_dotenv('destinationMap.env')
    payload = request.get_json()
    print(payload)
    destinationMap = os.environ.get('DESTINATIONMAP')
    destinationMap = json.loads(destinationMap)

    sb = SwitchBoard(GCP, bucket, payload, destinationMap)
    sb.run()



