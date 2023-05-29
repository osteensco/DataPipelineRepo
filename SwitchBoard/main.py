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


    def receiveCall(self):
        # provide response object
        return

    def grabStatus(self):
        # read in appropriate json object in cloud storage
        return

    def grabDestination(self):
        # determine the correct http endpoint to call from self.destinationMap
        # determine if all dependency conditions are met based on statusController data
        return

    def forwardCall(self):
        # send request(s) to identified http endpoint(s)
        # pass along any relevant data
        return

    def receiveConfirmation(self):
        # called when caller is a completed pipeline function
        return
    
    def updateStatus(self):
        # update appropriate json object in cloud storage
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

    # need to setup SwitchBoard library
        ## git+https://github.com/osteensco/[SwitchBoard].git
        ## pip install -e
            ## this will install the library in "editable mode" allowing changes to the library to be reflected automatically


def call(event, context):

    SwitchBoard(event)


