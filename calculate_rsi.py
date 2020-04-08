import logging
import os
import pandas as pd
import indicators
import sys
from datetime import datetime,timedelta
logging.basicConfig(level=logging.INFO)
import common.application as ap
app_properties=ap.app_properties
csv_mapping=ap.csv_mapping
from common.get_api_data import *
api_key = app_properties['api_key']
api_secret = app_properties['api_secret']
token=str(sys.argv[1])
os.makedirs(token,exist_ok=True)
file_name="holdings"
import pytz
tz = pytz.timezone('Asia/Kolkata')
rs1_trend_log="rsi_trend.csv"
logger = logging.getLogger('algo_tester')

def getDateTime():
  datetime_obj_hour_fwd=datetime.now(tz)
  return datetime_obj_hour_fwd

datetime_obj_hour_fwd=getDateTime()+timedelta(hours=1)
from_date=str(datetime_obj_hour_fwd-timedelta(days=60)).split(" ")[0]
to_date=str(datetime_obj_hour_fwd).split(" ")[0]
super_trend_prop1={"range":7,"mult":3}
super_trend_prop2={"range":7,"mult":2}
rsi_prop={"range":14}
historical_data = {}
historical_data = kite.historical_data(token,from_date,to_date,"minute")
df = pd.DataFrame(historical_data)
df['transaction_date'] = pd.to_datetime(df.date, errors='coerce').dt.strftime('%Y-%m-%d')
df['transaction_hour'] = pd.to_datetime(df.date, errors='coerce').dt.strftime('%H')
df['transaction_minute'] = pd.to_datetime(df.date, errors='coerce').dt.strftime('%M')
df = indicators.RSI(df,base='close',period=14)
historical_data = kite.historical_data(token,from_date,to_date,"3minute")
df_3m = pd.DataFrame(historical_data)
df_3m['transaction_date'] = pd.to_datetime(df_3m.date, errors='coerce').dt.strftime('%Y-%m-%d')
df_3m['transaction_hour'] = pd.to_datetime(df_3m.date, errors='coerce').dt.strftime('%H')
df_3m['transaction_minute'] = pd.to_datetime(df_3m.date, errors='coerce').dt.strftime('%M')
df_3m = indicators.RSI(df_3m,base='close',period=14)
historical_data = kite.historical_data(token,from_date,to_date,"5minute")
df_5m = pd.DataFrame(historical_data)
df_5m['transaction_date'] = pd.to_datetime(df_5m.date, errors='coerce').dt.strftime('%Y-%m-%d')
df_5m['transaction_hour'] = pd.to_datetime(df_5m.date, errors='coerce').dt.strftime('%H')
df_5m['transaction_minute'] = pd.to_datetime(df_5m.date, errors='coerce').dt.strftime('%M')
df_5m = indicators.RSI(df_5m,base='close',period=14)
historical_data = kite.historical_data(token,from_date,to_date,"15minute")
df_15m = pd.DataFrame(historical_data)
df_15m['transaction_date'] = pd.to_datetime(df_15m.date, errors='coerce').dt.strftime('%Y-%m-%d')
df_15m['transaction_hour'] = pd.to_datetime(df_15m.date, errors='coerce').dt.strftime('%H')
df_15m['transaction_minute'] = pd.to_datetime(df_15m.date, errors='coerce').dt.strftime('%M')
df_15m = indicators.RSI(df_15m,base='close',period=14)
historical_data = kite.historical_data(token,from_date,to_date,"hour")
df_1h = pd.DataFrame(historical_data)
df_1h['transaction_date'] = pd.to_datetime(df_1h.date, errors='coerce').dt.strftime('%Y-%m-%d')
df_1h['transaction_hour'] = pd.to_datetime(df_1h.date, errors='coerce').dt.strftime('%H')
df_1h['transaction_minute'] = pd.to_datetime(df_1h.date, errors='coerce').dt.strftime('%M')
df_1h = indicators.RSI(df_1h,base='close',period=14)
historical_data = kite.historical_data(token,from_date,to_date,"day")
df_1d = pd.DataFrame(historical_data)
df_1d['transaction_date'] = pd.to_datetime(df_1d.date, errors='coerce').dt.strftime('%Y-%m-%d')
df_1d['transaction_hour'] = pd.to_datetime(df_1d.date, errors='coerce').dt.strftime('%H')
df_1d['transaction_minute'] = pd.to_datetime(df_1d.date, errors='coerce').dt.strftime('%M')
df_1d = indicators.RSI(df_1d,base='close',period=14)
new_df = pd.merge(df,df_3m,how='left',left_on=['transaction_date','transaction_hour','transaction_minute'],right_on=['transaction_date','transaction_hour','transaction_minute'])
new_df = new_df.rename(columns={"date_x": "date", "open_x": "open","high_x": "high", "low_x": "low","close_x": "close", "volume_x": "volume","RSI_14_x":"RSI_14_minute","RSI_14_y":"RSI_14_3minute"})
new_df = new_df[['date','open','high','low','close','volume','transaction_date','transaction_hour','transaction_minute','RSI_14_minute','RSI_14_3minute']]
new_df = pd.merge(new_df,df_5m,how='left',left_on=['transaction_date','transaction_hour','transaction_minute'],right_on=['transaction_date','transaction_hour','transaction_minute'])
new_df = new_df.rename(columns={"date_x": "date", "open_x": "open","high_x": "high", "low_x": "low","close_x": "close", "volume_x": "volume","RSI_14":"RSI_14_5minute"})
new_df = new_df[['date','open','high','low','close','volume','transaction_date','transaction_hour','transaction_minute','RSI_14_minute','RSI_14_3minute','RSI_14_5minute']]
new_df = pd.merge(new_df,df_15m,how='left',left_on=['transaction_date','transaction_hour','transaction_minute'],right_on=['transaction_date','transaction_hour','transaction_minute'])
new_df = new_df.rename(columns={"date_x": "date", "open_x": "open","high_x": "high", "low_x": "low","close_x": "close", "volume_x": "volume","RSI_14":"RSI_14_15minute"})
new_df = new_df[['date','open','high','low','close','volume','transaction_date','transaction_hour','transaction_minute','RSI_14_minute','RSI_14_3minute','RSI_14_5minute','RSI_14_15minute']]
new_df = pd.merge(new_df,df_1h,how='left',left_on=['transaction_date','transaction_hour','transaction_minute'],right_on=['transaction_date','transaction_hour','transaction_minute'])
new_df = new_df.rename(columns={"date_x": "date", "open_x": "open","high_x": "high", "low_x": "low","close_x": "close", "volume_x": "volume","RSI_14":"RSI_14_1hour"})
new_df = new_df[['date','open','high','low','close','volume','transaction_date','transaction_hour','transaction_minute','RSI_14_minute','RSI_14_3minute','RSI_14_5minute','RSI_14_15minute','RSI_14_1hour']]
new_df = pd.merge(new_df,df_1d,how='left',left_on=['transaction_date','transaction_hour','transaction_minute'],right_on=['transaction_date','transaction_hour','transaction_minute'])
new_df = new_df.rename(columns={"date_x": "date", "open_x": "open","high_x": "high", "low_x": "low","close_x": "close", "volume_x": "volume","RSI_14":"RSI_14_1Day"})
new_df = new_df[['date','open','high','low','close','volume','transaction_date','transaction_hour','transaction_minute','RSI_14_minute','RSI_14_3minute','RSI_14_5minute','RSI_14_15minute','RSI_14_1hour','RSI_14_1Day']]
new_df = new_df.ffill()
new_df['%Change'] = (new_df['close'].diff() / new_df['close']) * 100
new_df['Market_Time'] = new_df.transaction_hour.str.cat(new_df.transaction_minute)
new_df.to_csv('rsi_trend_axis.csv',index=False,header=True)
