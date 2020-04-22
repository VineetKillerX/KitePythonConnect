import requests
import json

import common.application as ap
from common.get_api_data import *

app_properties = ap.app_properties
url = app_properties['slack_url']
trades_url = app_properties['trades_slack_url']


def sendMessage(message):
    slack_msg = {};
    slack_msg['text'] = message;
    requests.post(url, json.dumps(slack_msg))


def sendTrades(message):
    slack_msg = {};
    slack_msg['text'] = message;
    requests.post(trades_url, json.dumps(slack_msg))
