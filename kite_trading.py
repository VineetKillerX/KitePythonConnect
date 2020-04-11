import logging
import os
import pandas as pd
from common import indicators
import sys
from datetime import datetime, timedelta
from common.get_api_data import *
logging.basicConfig(level=logging.INFO)
import common.application as ap
import pytz
from simulator import place_order as simulator
app_properties = ap.app_properties
token_mappings = ap.token_mappings
csv_mapping = ap.csv_mapping


api_key = app_properties['api_key']
api_secret = app_properties['api_secret']
token = str(sys.argv[1])
os.makedirs(token, exist_ok=True)
file_name = "trades.csv"
tz = pytz.timezone('Asia/Kolkata')
logger = logging.getLogger('algo_tester')
#global define
profit = 0.019
stop_loss = 0.095
historical_data = ""
supertrend = ""
rsi = 0.00
rsi_slope = 0.00
wma20 = 0.00
wma5 = 0.00
wma14 = 0.00
last_close = 0.00
last_price = get_price(token, 0.00)
holding=""
order_id=""
swing=False

def getDateTime():
    datetime_obj_hour_fwd = datetime.now(tz)
    return datetime_obj_hour_fwd


datetime_obj_hour_fwd = getDateTime() + timedelta(hours=1)
from_date = str(datetime_obj_hour_fwd - timedelta(days=14)).split(" ")[0]
to_date = str(datetime_obj_hour_fwd).split(" ")[0]

def create_log(action):
    log = str(order_id) + ',' + str(supertrend) + ',' + str(rsi) + ',' + str(rsi_slope) + ',' + str(
                wma20) + ',' + str(wma5) + ',' + str(wma14) + ',' + str(last_close) + ',' + str(
                current_price) + ',' + str(action) + ',' + str(holding)
    return log

def write_log(action,name=file_name):
    global profit ,stop_loss ,last_min ,historical_data ,supertrend ,rsi ,rsi_slope ,wma20 ,wma5 ,wma14 ,last_close,last_price ,order_id, holding
    current_price = get_price(token, last_close)
    log = ''
    if action == 'BUY' or action == 'SELL':
        simulator.place_order(action, token_mappings[token], 1000)
        if order_id == '':
            datetime_obj = getDateTime()
            order_id = str(datetime_obj.hour) + str(datetime_obj.minute) + str(datetime_obj.second)
            holding = 'up' if action == 'BUY' else 'down'
            log=create_log(action)
        else:
            holding = ''
            log=create_log(action)
            order_id = ''
    name = token + "/" + name
    f = open(name, 'a')
    if log == '':
        log=create_log(action)
    f.write(log)
    f.close()


def place_order(signal):
    if supertrend == 'up' and 60 <= rsi <= 80 and rsi_slope > 0.5 and wma5 >= wma20 and holding == '' and order_id == '' and signal == '':
        write_log('BUY')
    elif supertrend == 'down' and 20 <= rsi <= 40 and rsi_slope < -0.5 and wma5 <= wma20 and holding == '' and order_id == '' and signal == '':
        write_log('SELL')
    elif holding != '' and order_id != '' and signal == '' and swing:
        write_log('SELL') if holding == 'up' else write_log('BUY')
    elif holding != '' and order_id != '' and signal == '':
        write_log('HOLD')
    elif holding != '' and order_id != '' and signal != '':
        write_log(signal)
    else:
        write_log('NONE')


def stopper():
    global last_price
    current_price = get_price(token, last_price)
    datetime_obj = getDateTime()
    if holding == 'up':
        temp_profit = (current_price - last_price) / last_price
        temp_loss = (last_price - current_price) / last_price
        if temp_profit >= profit:
            place_order('SELL')
        elif temp_loss > stop_loss:
            place_order('BUY')
        if datetime_obj.hour == 15 and datetime_obj.minute > 28:
            place_order('SELL')
    elif holding == 'down':
        temp_profit = (last_price - current_price) / last_price
        temp_loss = (current_price - last_price) / last_price
        if temp_profit >= profit:
            place_order('SELL')
        elif temp_loss > stop_loss:
            place_order('BUY')
        if datetime_obj.hour == 15 and datetime_obj.minute > 28:
            place_order('SELL')
    last_price = current_price
  
        
def get_history_data():
        global historical_data ,supertrend ,rsi ,rsi_slope ,wma20 ,wma5 ,wma14 ,last_close,last_price,swing 
        swing=False
        historical_data = get_data(token, from_date, to_date, "5minute", historical_data)
        df = pd.DataFrame(historical_data)
        df = indicators.SuperTrend(df, 4, 2)
        df = indicators.RSI(df)
        df = indicators.vwma(df, 20)
        df = indicators.vwma(df, 5)
        df = indicators.vwma(df, 14)
        df['RSI_Trend'] = df['RSI_8'].diff()
        tail_dict = df.tail(1).to_dict()
        supertrend = tail_dict['STX_4_2']
        rsi = tail_dict['RSI_8']
        rsi_slope = tail_dict['RSI_Trend']
        wma20 = tail_dict['VWMA_20']
        wma5 = tail_dict['VWMA_5']
        wma14 = tail_dict['VWMA_14']
        last_close = tail_dict['close']
        swing=is_swing(df)


def is_swing(df):
    temp_df=df.iloc[-3:]
    if holding=='up':
        high_values=list(temp_df['high'])
        if (high_values[2] < high_values[0] <high_values[1]):
            return True
    else:
        low_values=list(temp_df['low'])
        if (low_values[1] < low_values[0] < low_values[2] ):
            return True

        
            

def trade():
    signal=''
    last_min = -1
    while True:
        datetime_obj = getDateTime()
        if (datetime_obj.hour >= 15 and datetime_obj.minute > 30) or (
                datetime_obj.hour <= 9 and datetime_obj.minute <= 10):
            raise Exception('Market Not Tradable at this moment')
        minutes = int(str(datetime_obj).split(".")[0].split(":")[1])
        if minutes % 5 == 0 and (last_min == -1 or (minutes != last_min and last_min != -1)):
            get_history_data()
            if minutes == 15 and datetime_obj.hour == 9:
                low = tail_dict['low']
                high = tail_dict['high']
                if (abs(high-low)/last_close) >= 0.01:
                    time.sleep(5*60)
            place_order(signal)
            last_min=minutes
        else:
            stopper()
        time.sleep(1)


if __name__ == "__main__":
    name = token + "/trades.csv"
    global order_id,holding
    try:
        with open(name, 'r') as f:
            lines = f.read().splitlines()
            last_line = lines[-1]
            order_id = last_line.split(",")[0]
            holding = last_line.split(",")[10]
            trade()
    except:
        trade()
