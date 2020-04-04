import logging
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import indicators
from datetime import datetime,timedelta 
import time
import multiprocessing
logging.basicConfig(level=logging.INFO)
import common.application as ap
app_properties=ap.app_properties
from common.get_api_data import *
api_key = app_properties['api_key']
api_secret = app_properties['api_secret']
file_name="holdings_log.csv"
import pytz
tz = pytz.timezone('Asia/Kolkata')
rs1_trend_log="rsi_trend.csv"


def run(request_token):
	jobs = []
	tokens = ['1510401']
	for instrument_token in tokens:
		process = multiprocessing.Process(target=trade,args=(instrument_token,))
		jobs.append(process)
	for j in jobs:
		j.start()
	for j in jobs:
		j.join()




def trade(token):
  print("token is started ",token)
  global kite
  profit = 0.02
  stop_loss = 0.01
  last_price = 0.00
  last_min=-1
  datetime_obj_hour_fwd=getDateTime()+timedelta(hours=1)
  from_date=str(datetime_obj_hour_fwd-timedelta(days=14)).split(" ")[0]
  to_date=str(datetime_obj_hour_fwd).split(" ")[0]
  historical_data_rsi=""
  historical_data=""
  holding=""
  while True:
    datetime_obj=getDateTime()
    min=int(str(datetime_obj).split(".")[0].split(":")[1])
    if(min%5==0 and (last_min==-1 or (min!=last_min and last_min!=-1))):
      if(last_min==-1 or (datetime_obj_hour_fwd<=datetime_obj and min==15)):
        historical_data_rsi = get_data(token,from_date,to_date,"hour",historical_data_rsi)
        historical_data_rsi_df=pd.DataFrame(historical_data_rsi)
        df_rsi = indicators.RSI(historical_data_rsi_df,'close',14)
        latest_data = df_rsi.tail(1)
        rsi = float(latest_data['RSI_14'].get(latest_data['RSI_14'].index.start))
      last_min=min
      historical_data = get_data(token,from_date,to_date,"5minute",historical_data)
      df = pd.DataFrame(historical_data)
      df = indicators.SuperTrend(df,7,3,['open','high','low','close'])
      latest_data = df.tail(1)
      signal = str(latest_data['STX_7_3'].get(latest_data['STX_7_3'].index.start))
      write_log(str(rsi)+","+str(signal)+","+str(datetime_obj)+"\n",rs1_trend_log)
      if (datetime_obj.hour == 15 and datetime_obj.minute>15):
        profit = 0.01
        stop_loss = 0.005
      else:
        (holding,last_price)=place_orders(signal,token,holding,last_price)
    else:
      holding=stoper(token,last_price,profit,stop_loss,datetime_obj,holding)
    time.sleep(1)		



def getDateTime():
  datetime_obj_hour_fwd=datetime.now(tz)
  return datetime_obj_hour_fwd



def place_orders(signal,token,holding,last_price):
	if(signal=='down' and holding!='down' and rsi<50):
		price =get_price(token,last_price)
		write_log("Sell"+","+str(token)+","+str(price)+",supertrend,"+str(getDateTime())+"\n")
		holding = 'down'
		last_price = price
	elif (signal == 'up' and holding != 'up' and rsi>50):
		price =get_price(token,last_price)
		write_log("Buy"+","+str(token)+","+str(price)+",supertrend,"+str(getDateTime())+"\n")
		holding = 'up'
		last_price = price
	return	(holding,last_price)





def stoper(token,last_price,profit,stop_loss,datetime_obj,holding):
  price =get_price(token,last_price)
  if (holding=='up'):
    temp_profit = (price-last_price)/last_price
    temp_loss = (last_price-price)/last_price
    if(temp_profit>=profit):
      holding = ''
      write_log("Sell"+","+str(token)+","+str(price)+",profit,"+str(getDateTime())+"\n")
    elif(temp_loss>stop_loss):
      holding = ''
      write_log("Sell"+","+str(token)+","+str(price)+",stoploss,"+str(getDateTime())+"\n")	
    if (datetime_obj.hour == 15 and datetime_obj.minute>28):
      holding = ''
      write_log("Sell"+","+str(token)+","+str(price)+",market_close,"+str(getDateTime())+"\n")
  elif (holding=='down'):
    temp_profit = (last_price-price)/last_price
    temp_loss = (price-last_price)/last_price
    if(temp_profit>=profit):
      holding = ''
      write_log("Buy"+","+str(token)+","+str(price)+",profit,"+str(getDateTime())+"\n")
    elif(temp_loss>stop_loss):
      holding = ''
      write_log("Buy"+","+str(token)+","+str(price)+",stoploss,"+str(getDateTime())+"\n")
    if (datetime_obj.hour == 15 and datetime_obj.minute>28):
      holding = ''
      write_log("Buy"+","+str(token)+","+str(price)+",market_close,"+str(getDateTime())+"\n")
  return holding  




			
def write_log(log,file_name=file_name):
  f=open(file_name,'a')	
  f.write(log)	
  f.close()	
  				
