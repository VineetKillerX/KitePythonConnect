import requests
import json

url = 'https://hooks.slack.com/services/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'

def sendMessage(message):
  slack_msg = {};
  slack_msg['text'] = message;
  myResponse = requests.post(url,json.dumps(slack_msg))
