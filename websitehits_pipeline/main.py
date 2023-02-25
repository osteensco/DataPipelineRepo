from objs.rds_pipeline import run_pipeline
from objs.datasources import WebsiteEndpoint
import base64






def websitehits_pipeline(event, context):
    payload = base64.b64decode(event['data']).decode('utf-8')

    data = [
    WebsiteEndpoint(payload)
    ]

    manual = [
        
    ]

    run_pipeline(data, manual)



