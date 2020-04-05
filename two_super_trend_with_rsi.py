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
file_name="holdings_2_sup.csv"
import pytz
tz = pytz.timezone('Asia/Kolkata')
rs1_trend_log="rsi_trend_2_sup.csv"
from SlackUtil import sendMessage


def run():
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
	profit = 0.05
	stop_loss = 0.025
	last_price = 0.00
	last_min=-1
	datetime_obj_hour_fwd=getDateTime()+timedelta(hours=1)
	from_date=str(datetime_obj_hour_fwd-timedelta(days=14)).split(" ")[0]
	to_date=str(datetime_obj_hour_fwd).split(" ")[0]
	super_trend_prop1={"range":7,"mult":3}
	super_trend_prop2={"range":7,"mult":2}
	rsi_prop={"range":14}
	holding=''
	order_id=""
	historical_data=""
	while True:
		datetime_obj=getDateTime()
		min=int(str(datetime_obj).split(".")[0].split(":")[1])
		if(min%5==0 and (last_min==-1 or (min!=last_min and last_min!=-1))):
			last_min=min
			historical_data = get_data(token,from_date,to_date,"5minute",historical_data)
			df = pd.DataFrame(historical_data)
			df = indicators.SuperTrend(df,super_trend_prop1['range'],super_trend_prop1['mult'],['open','high','low','close'])
			df = indicators.SuperTrend(df,super_trend_prop2['range'],super_trend_prop2['mult'],['open','high','low','close'])
			df = indicators.RSI(df,'close',rsi_prop["range"])
			tail_dict=df.tail(1).to_dict()
			index=list(tail_dict['open'].keys())[0]
			signal1=tail_dict["STX_"+str(super_trend_prop1['range'])+"_"+str(super_trend_prop1['mult'])][index]
			signal2=tail_dict["STX_"+str(super_trend_prop2['range'])+"_"+str(super_trend_prop2['mult'])][index]
			suptrenval1=tail_dict["ST_"+str(super_trend_prop1['range'])+"_"+str(super_trend_prop1['mult'])][index]
			suptrenval2=tail_dict["ST_"+str(super_trend_prop2['range'])+"_"+str(super_trend_prop2['mult'])][index]
			rsi=tail_dict['RSI_'+str(rsi_prop["range"])][index]
			write_log(str(rsi)+","+str(signal1)+","+str(signal2)+","+str(suptrenval1)+","+str(suptrenval2)+","+str(datetime_obj)+"\n",rs1_trend_log)
			if (datetime_obj.hour == 15 and datetime_obj.minute>15):
				profit = 0.01
				stop_loss = 0.005
			else:
				(holding,last_price,order_id)=place_orders(signal1,signal2,suptrenval1,suptrenval2,token,holding,last_price,rsi,index,order_id)
		else:
		  holding=stoper(token,last_price,profit,stop_loss,datetime_obj,holding,order_id)
		time.sleep(1)		



def getDateTime():
  datetime_obj_hour_fwd=datetime.now(tz)
  return datetime_obj_hour_fwd



def place_orders(signal1,signal2,suptrenval1,suptrenval2,token,holding,last_price,rsi,index,order_id):
	price = get_price(token,last_price)
		#Stoping condition
	if((signal1!=signal2 and (suptrenval2>price>suptrenval1  or  suptrenval1>price>suptrenval2) and holding!='') or (holding=='up' and signal1=='down' and signal2=='down') or (holding=='down' and signal1=='up' and signal2=='up')):
		flag='loss'
		holding = ''
		if(price-last_price>0):
			flag=='profit'
		if(holding=='up'):
			write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+","+flag+","+str(getDateTime())+"\n")
		else:
			write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+","+flag+","+str(getDateTime())+"\n")
	if(signal1=='down' and signal2=='down' and price<suptrenval1 and price<suptrenval2 and holding!='down' and rsi<50):
		order_id=index
		write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+",supertrend,"+str(getDateTime())+"\n")
		holding = 'down'
		last_price = price
	elif(signal1=='up' and signal2=='up' and price>suptrenval1 and price>suptrenval2 and holding!='up' and rsi>50):
		order_id=index
		write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+",supertrend,"+str(getDateTime())+"\n")
		holding = 'up'
		last_price = price
	return	(holding,last_price,order_id)





def stoper(token,last_price,profit,stop_loss,datetime_obj,holding,order_id):
  price = get_price(token,last_price)
  if (holding=='up'):
    temp_profit = (price-last_price)/last_price
    temp_loss = (last_price-price)/last_price
    if(temp_profit>=profit):
      holding = ''
      write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+",profit,"+str(getDateTime())+"\n")
    elif(temp_loss>stop_loss):
      holding = ''
      write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+",stoploss,"+str(getDateTime())+"\n")	
    if (datetime_obj.hour == 15 and datetime_obj.minute>28):
      holding = ''
      write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+",market_close,"+str(getDateTime())+"\n")
  elif (holding=='down'):
    temp_profit = (last_price-price)/last_price
    temp_loss = (price-last_price)/last_price
    if(temp_profit>=profit):
      holding = ''
      write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+",profit,"+str(getDateTime())+"\n")
    elif(temp_loss>stop_loss):
      holding = ''
      write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+",stoploss,"+str(getDateTime())+"\n")
    if (datetime_obj.hour == 15 and datetime_obj.minute>28):
      holding = ''
      write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+",market_close,"+str(getDateTime())+"\n")
  return holding  





			
def write_log(log,file_name=file_name):
	sendMessage(log)
	f=open(file_name,'a')	
	f.write(log)	
	f.close()	
			
			
if __name__=='__main__':
	trade("1510401")