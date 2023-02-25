from objs.rds_pipeline import run_pipeline
from objs.datasources import CFGames






def cf_games_pipeline(event, context):
    data = [
    CFGames()
    ]

    manual = [

    ]

    run_pipeline(data, manual)

