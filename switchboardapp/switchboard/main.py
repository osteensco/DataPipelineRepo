
import os
from dotenv import load_dotenv
from switchboard_py import http_trigger, SwitchBoard, connect_to_bucket, GCP





bucket = connect_to_bucket(GCP)

@http_trigger
def main(request):

    load_dotenv('destinationMap.env')
    payload = request.get_json()
    print(payload)
    destinationMap = os.environ.get('DESTINATIONMAP')

    sb = SwitchBoard(GCP, bucket, payload, destinationMap)
    sb.run()


