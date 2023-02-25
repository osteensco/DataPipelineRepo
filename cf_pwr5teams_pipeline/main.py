from objs.rds_pipeline import run_pipeline
from objs.datasources import CFTeamsAll





def cf_pwr5teams_pipeline(event, context):
    data = [
    CFTeamsAll()
    ]

    manual = [

    ]

    run_pipeline(data, manual)


