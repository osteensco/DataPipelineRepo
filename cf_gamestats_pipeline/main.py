from objs.rds_pipeline import run_pipeline
from objs.datasources import CFGameTeamStats






def cf_gamestats_pipeline(event, context):
    data = [
    CFGameTeamStats()
    ]

    manual = [

    ]

    run_pipeline(data, manual)

