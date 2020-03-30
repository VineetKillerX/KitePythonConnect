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
logging.basicConfig(level=logging.INFO)
import application as ap
app_properties=ap.app_properties
kite=None
api_key = app_properties['api_key']
api_secret = app_properties['api_secret']
def generate_url():
	global kite
	kite = KiteConnect(api_key,api_secret)
	url = kite.login_url()
	return url

def run(request_token):
	global kite
	data = kite.generate_session(request_token,api_secret)
	access_token = data["access_token"]
	kite.set_access_token(access_token)
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
	global kite
	profit = 0.02
	stop_loss = 0.01
	last_price = 0.00
	file_name="holdings.csv"
	holding = ''
	last_min=-1
	while True:
		datetime_obj=datetime.now()
		f=open(file_name,'a')
		min=int(str(datetime_obj).split(".")[0].split(":")[1])
		if(min%5==0 and (last_min==-1 or (min!=last_min and last_min!=-1))):
			last_min=min
			historical_data = kite.historical_data(instrument_token=token,from_date='2020-03-20',to_date='2020-03-30',interval='5minute')
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
					f.write("Sell"+","+str(instrument_token)+","+str(price)+",supertrend,"+str(datetime.now())+"\n")
					f.close()
					holding = 'down'
					last_price = price
				elif (signal == 'up' and holding != 'up' and rsi>50):
					quote = kite.quote(token)
					price = quote[token]['last_price']
					f.write("Buy"+","+str(instrument_token)+","+str(price)+",supertrend,"+str(datetime.now())+"\n")
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
					f.write("Sell"+","+str(instrument_token)+","+str(price)+",profit,"+str(datetime.now())+"\n")
					f.close()
				elif(temp_loss>stop_loss):
					holding = ''
					f.write("Sell"+","+str(instrument_token)+","+str(price)+",stoploss,"+str(datetime.now())+"\n")	
					f.close()
				if (datetime_obj.hour == 15 and datetime_obj.minute>28):
					holding = ''
					f.write("Sell"+","+str(instrument_token)+","+str(price)+",market_close,"+str(datetime.now())+"\n")
					f.close()
			elif (holding=='down'):
				temp_profit = (last_price-price)/last_price
				temp_loss = (price-last_price)/last_price
				if(temp_profit>=profit):
					holding = ''
					f.write("Buy"+","+str(instrument_token)+","+str(price)+",profit,"+str(datetime.now())+"\n")
					f.close()
				elif(temp_loss>stop_loss):
					holding = ''
					f.write("Buy"+","+str(instrument_token)+","+str(price)+",stoploss,"+str(datetime.now())+"\n")
					f.close()
				if (datetime_obj.hour == 15 and datetime_obj.minute>28):
					holding = ''
					f.write("Buy"+","+str(instrument_token)+","+str(price)+",market_close,"+str(datetime.now())+"\n")
					f.close()
		time.sleep(1)					
