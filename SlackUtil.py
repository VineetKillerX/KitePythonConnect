import requests
import json

url = 'https://hooks.slack.com/services/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'

def sendMessage(message):
  slack_msg = {};
  slack_msg['text'] = message;
  myResponse = requests.post(url,json.dumps(slack_msg))
