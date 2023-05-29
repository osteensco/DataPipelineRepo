import base64


class SwitchBoard():
    def __init__(self, data) -> None:
        self.data = base64.b64decode(data['data']).decode('utf-8')
        self.caller = self.data['caller']
        self.statusController = self.grabStatus()
        self.destinationMap = {
            'cron': [],
            'webhook': [],

        }



    def grabStatus(self):
        return


    def grabDestination(self):
        return





##SwitchBoard framework:
    ####PubSub triggers GCF endpoints.
    ####Trigger endpoints will trigger SwitchBoard GCF via HTTP.
    ####SwitchBoard contains graph data structure that will trigger appropriate pipelines via HTTP.
        ###SwitchBoard will reference .json file in cloud storage for any additional dependencies that should be considered.
        ###When a pipeline is triggered a 200 response is returned immediately to identify a successful trigger.
        ###Any failures will exist in logs of pipeline GCF.
    ####On completion of pipeline GCF, SwitchBoard will be triggered to communicate successful run.
        ###SwitchBoard will update .json file in cloud storage once successful pipeline run is communicated to it.




def call(event, context):

    SwitchBoard(event)


