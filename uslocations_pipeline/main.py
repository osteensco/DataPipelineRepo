from objs.rds_pipeline import run_pipeline
from objs.datasources import GeoData






def uslocations_pipeline(event, context):
    data = [
    GeoData()
    ]

    manual = [
        
    ]

    run_pipeline(data, manual)



