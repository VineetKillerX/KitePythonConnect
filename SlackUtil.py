import requests
import json

url = 'https://hooks.slack.com/services/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'

def sendMessage(message):
  slack_msg = {};
  slack_msg['text'] = message;
  myResponse = requests.post(url,json.dumps(slack_msg))
