import os
import sys
from datetime import datetime, timedelta
import pandas as pd
from common import indicators
from common.get_api_data import *
logging.basicConfig(level=logging.INFO)
import common.application as ap
import pytz
from common.SlackUtil import sendMessage, sendTrades

app_properties = ap.app_properties
token_mappings = ap.token_mappings
price_mappings = ap.price_mappings
csv_mapping = ap.csv_mapping

api_key = app_properties['api_key']
api_secret = app_properties['api_secret']
token = str(sys.argv[1])
os.makedirs(token, exist_ok=True)
file_name = "trades.csv"
tz = pytz.timezone('Asia/Kolkata')
logger = logging.getLogger('algo_tester')
# global define
profit = 0.01
stop_loss = 0.01
historical_data = ""
supertrend = ""
rsi = 0.00
rsi_slope = 0.00
wma20 = 0.00
wma5 = 0.00
wma14 = 0.00
sma20 = 0.00
vwap = 0.00
last_close = 0.00
last_high = 0.00
last_low = 0.00
last_price = get_price(token, 0.00)
current_price = 0.00
holding = ""
order_id = ""
swing = False
flag = ''
current_profit = 0.00
current_loss = 0.00
last_data_time = ''


def getDateTime():
    datetime_obj_hour_fwd = datetime.now(tz)
    return datetime_obj_hour_fwd


datetime_obj_hour_fwd = getDateTime() + timedelta(hours=1)
from_date = str(datetime_obj_hour_fwd - timedelta(days=14)).split(" ")[0]


def create_log(action):
    current_time = str(getDateTime()).split(".")[0]
    log = str(order_id) + ',' + str(supertrend) + ',' + str(rsi) + ',' + str(rsi_slope) + ',' + str(
        wma20) + ',' + str(wma5) + ',' + str(wma14) + ',' + str(last_close) + ',' + str(
        current_price) + ',' + str(action) + ',' + str(holding) + ',' + str(last_price) + ',' + str(
        current_time) + ',' + str(price_mappings[token]) + ',' + str(last_data_time) + ',' + str(
        current_profit) + ',' + str(
        current_loss) + ',' + str(sma20) + ',' + str(vwap) + ',' + str(swing) + "\n"
    return log


def write_log(action, name=file_name):
    global profit, stop_loss, historical_data, supertrend, rsi, rsi_slope, wma20, wma5, wma14, last_close, last_price, order_id, holding, current_price, price_status
    log = ''
    current_price = get_price(token, last_close)
    if action == 'BUY' or action == 'SELL':
        slack_msg = action + " : " + str(token_mappings[token]) + " @ " + str(current_price)
        if order_id == '':
            datetime_obj = getDateTime()
            order_id = str(datetime_obj.hour) + str(datetime_obj.minute) + str(datetime_obj.second)
            holding = 'up' if action == 'BUY' else 'down'
            last_price = current_price
            log = create_log(action)
        else:
            holding = ''
            slack_msg = slack_msg + ' for Order Id : ' + str(order_id) + ' in : ' + str(flag)
            sendTrades(slack_msg)
            log = create_log(action)
            order_id = ''
        sendMessage(slack_msg)
    name = token + "/" + name
    f = open(name, 'a')
    if log == '':
        log = create_log(action)
    f.write(log)
    f.close()


def place_order(signal):
    if supertrend == 'up' and 60 <= rsi <= 80  and rsi_slope > 0 and last_close > vwap and holding == '' and order_id == '' and signal == '':
        write_log('BUY')
    elif supertrend == 'down' and 20 <= rsi <= 40 and rsi_slope < 0 and last_close < vwap and holding == '' and order_id == '' and signal == '':
        write_log('SELL')
    elif holding != '' and order_id != '' and signal == '':
        write_log('HOLD')
    elif holding != '' and order_id != '' and signal != '':
        write_log(signal)
    else:
        write_log('NONE')


def stopper():
    global last_price, flag, current_profit, current_loss
    flag = ''
    current_price = get_price(token, last_price)
    datetime_obj = getDateTime()
    if holding == 'up':
        temp_profit = (current_price - last_price) / last_price
        temp_loss = (last_price - current_price) / last_price
        current_profit = temp_profit
        current_loss = temp_loss
        if temp_profit >= profit:
            flag = 'Profit'
            place_order('SELL')
        elif temp_loss > stop_loss:
            flag = 'Loss'
            place_order('SELL')
        if datetime_obj.hour == 15 and datetime_obj.minute > 20:
            flag = 'Market Close : Profit/Loss'
            place_order('BUY')
    elif holding == 'down':
        temp_profit = (last_price - current_price) / last_price
        temp_loss = (current_price - last_price) / last_price
        current_profit = temp_profit
        current_loss = temp_loss
        if temp_profit >= profit:
            flag = 'Profit'
            place_order('BUY')
        elif temp_loss > stop_loss:
            flag = 'Loss'
            place_order('BUY')
        if datetime_obj.hour == 15 and datetime_obj.minute > 20:
            flag = 'Market Close : Profit/Loss'
            place_order('BUY')


def get_history_data():
    global historical_data, supertrend, rsi, rsi_slope, wma20, wma5, wma14, last_close, last_price, swing, last_high, last_low, last_data_time, sma20, vwap
    swing = False
    to_date = str(getDateTime() - timedelta(minutes=2)).split(".")[0]
    historical_data = get_data(token, from_date, to_date,"5minute", historical_data)
    df = pd.DataFrame(historical_data)
    df = indicators.SuperTrend(df, 4, 2)
    df = indicators.RSI(df)
    df = indicators.vwma(df, 20)
    df = indicators.vwma(df, 5)
    df = indicators.vwma(df, 14)
    df = indicators.SMA(df, 'close', 'SMA20', 20)
    df['RSI_Trend'] = df['RSI_8'].diff()
    tail_dict = df.tail(1).to_dict('list')
    supertrend = tail_dict['STX_4_2'][0]
    rsi = tail_dict['RSI_8'][0]
    sma20 = tail_dict['SMA20'][0]
    rsi_slope = tail_dict['RSI_Trend'][0]
    wma20 = tail_dict['VWMA_20'][0]
    wma5 = tail_dict['VWMA_5'][0]
    wma14 = tail_dict['VWMA_14'][0]
    last_close = tail_dict['close'][0]
    last_low = tail_dict['low'][0]
    last_high = tail_dict['high'][0]
    last_data_time = tail_dict['date'][0]
    swing = is_swing(df)
    from_date_current_data = str(getDateTime()).split(" ")[0]
    current_data = get_data(token, from_date_current_data, to_date,"5minute", historical_data)
    df_current = pd.DataFrame(current_data)
    df_current = indicators.VWMA(df_current)
    tail_dict = df_current.tail(1).to_dict('list')
    vwap = tail_dict['VWAP'][0]


def is_swing(df):
    temp_df = df.iloc[-3:]
    if holding == 'up':
        high_values = list(temp_df['high'])
        if high_values[2] < high_values[0] < high_values[1]:
            return True
    else:
        low_values = list(temp_df['low'])
        if low_values[1] < low_values[0] < low_values[2]:
            return True


def trade():
    signal = ''
    last_min = -1
    while True:
        datetime_obj = getDateTime()
        if (datetime_obj.hour >= 15 and datetime_obj.minute > 21) or (
                datetime_obj.hour <= 9 and datetime_obj.minute <= 18):
            raise Exception('Market Not Tradable at this moment')
        minutes = int(str(datetime_obj).split(".")[0].split(":")[1])
        if minutes % 5 == 0 and (last_min == -1 or (minutes != last_min and last_min != -1)):
            get_history_data()
            place_order(signal)
            last_min = minutes
        else:
            stopper()
        time.sleep(1)


def init():
    global order_id, holding, last_price
    name = token + "/trades.csv"
    try:
        with open(name, 'r') as f:
            lines = f.read().splitlines()
            last_line = lines[-1]
            order_id = last_line.split(",")[0]
            holding = last_line.split(",")[10]
            last_price = float(last_line.split(",")[11])
            trade()
    except:
        trade()


if __name__ == "__main__":
    init()
