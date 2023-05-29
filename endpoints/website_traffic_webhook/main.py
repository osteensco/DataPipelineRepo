import requests





class Caller():
    def __init__(self, switchboard, body) -> None:
        self.switchboard = switchboard
        self.body = body
    
    def place_call(self):
        response = requests.post(self.switchboard, json=self.body)

        if response.status_code == 200:
            print('Switchboard trigger request successful!')
        else:
            print('Request failed with status code:', response.status_code)


def call_switchboard(event, context):
    switchboard = ''
    body = {
        'caller': 'cron'
    }

    trigger = Caller(switchboard, body)
    trigger.place_call()


