import requests
import json

url = 'https://hooks.slack.com/services/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'

def sendMessage(message):
  slack_msg = {};
  slack_msg['text'] = message;
  myResponse = requests.post(url,json.dumps(slack_msg))
