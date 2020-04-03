import logging
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import indicators
from datetime import datetime 
import time
import multiprocessing
from datetime import timedelta
logging.basicConfig(level=logging.INFO)
import common.application as ap
from common.create_kite_session import *
app_properties=ap.app_properties
kite=get_session()
api_key = app_properties['api_key']
api_secret = app_properties['api_secret']
import pytz
tz = pytz.timezone('Asia/Kolkata')
file_name="holdings.csv"

def run(request_token):
	jobs = []
	tokens = ['1510401','408065','633601','1270529','779521','340481','738561','348929','877057']
	for instrument_token in tokens:
		process = multiprocessing.Process(target=trade,args=(instrument_token,))
		jobs.append(process)
	for j in jobs:
		j.start()
	for j in jobs:
		j.join()

def trade(token):
	print("token is started ",token)
	profit = 0.02
	stop_loss = 0.01
	last_price = 0.00
	holding = ''
	last_min=-1
	datetime_obj_hour_fwd=datetime.now(tz)
	from_date=str(datetime_obj_hour_fwd-timedelta(days=7)).split(" ")[0]
	to_date=str(datetime_obj_hour_fwd).split(" ")[0]
	while True:
		datetime_obj=datetime.now(tz)
		f=open(file_name,'a')
		min=int(str(datetime_obj).split(".")[0].split(":")[1])
		if(min%5==0 and (last_min==-1 or (min!=last_min and last_min!=-1))):
			last_min=min
			historical_data = kite.historical_data(instrument_token=token,from_date=from_date,to_date=to_date,interval='5minute')
			df = pd.DataFrame(historical_data)
			df = indicators.SuperTrend(df,7,3,['open','high','low','close'])
			df_rsi = indicators.RSI(df,'close',14)
			latest_data = df.tail(1)
			signal = str(latest_data['STX_7_3'].get(latest_data['STX_7_3'].index.start))
			rsi = float(latest_data['RSI_14'].get(latest_data['RSI_14'].index.start))
			if (datetime_obj.hour == 15 and datetime_obj.minute>15):
				profit = 0.01
				stop_loss = 0.005
			else:
				if (signal=='down' and holding!='down' and rsi<50):
					quote = kite.quote(token)
					price = quote[token]['last_price']
					write_log("Sell"+","+str(token)+","+str(price)+",supertrend,"+str(datetime.now(tz))+"\n")
					holding = 'down'
					last_price = price
				elif (signal == 'up' and holding != 'up' and rsi>50):
					quote = kite.quote(token)
					price = quote[token]['last_price']
					write_log("Buy"+","+str(token)+","+str(price)+",supertrend,"+str(datetime.now(tz))+"\n")
					holding = 'up'
					last_price = price
		else:
			quote = kite.quote(token)
			price = quote[token]['last_price']
			if (holding=='up'):
				temp_profit = (price-last_price)/last_price
				temp_loss = (last_price-price)/last_price
				if(temp_profit>=profit):
					holding = ''
					write_log("Sell"+","+str(token)+","+str(price)+",profit,"+str(datetime.now(tz))+"\n")
				elif(temp_loss>stop_loss):
					holding = ''
					write_log("Sell"+","+str(token)+","+str(price)+",stoploss,"+str(datetime.now(tz))+"\n")	
				if (datetime_obj.hour == 15 and datetime_obj.minute>28):
					holding = ''
					write_log("Sell"+","+str(token)+","+str(price)+",market_close,"+str(datetime.now(tz))+"\n")
			elif (holding=='down'):
				temp_profit = (last_price-price)/last_price
				temp_loss = (price-last_price)/last_price
				if(temp_profit>=profit):
					holding = ''
					write_log("Buy"+","+str(token)+","+str(price)+",profit,"+str(datetime.now(tz))+"\n")
				elif(temp_loss>stop_loss):
					holding = ''
					write_log("Buy"+","+str(token)+","+str(price)+",stoploss,"+str(datetime.now(tz))+"\n")
				if (datetime_obj.hour == 15 and datetime_obj.minute>28):
					holding = ''
					write_log("Buy"+","+str(token)+","+str(price)+",market_close,"+str(datetime.now(tz))+"\n")
		time.sleep(1)					




			
def write_log(log,file_name=file_name):
  f=open(file_name,'a')	
  f.write(log)	
  	
  			