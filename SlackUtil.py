import requests
import json
from common.get_api_data import *
url = app_properties['slack_url']

def sendMessage(message):
  slack_msg = {};
  slack_msg['text'] = message;
  myResponse = requests.post(url,json.dumps(slack_msg))
