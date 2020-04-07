import logging
import os
import pandas as pd
import indicators
from datetime import datetime,timedelta
logging.basicConfig(level=logging.INFO)
import common.application as ap
app_properties=ap.app_properties
csv_mapping=ap.csv_mapping
from common.get_api_data import *
api_key = app_properties['api_key']
api_secret = app_properties['api_secret']
token='1510401'
os.makedirs(token,exist_ok=True)
file_name="holdings"
import pytz
tz = pytz.timezone('Asia/Kolkata')
rs1_trend_log="rsi_trend.csv"
from SlackUtil import sendMessage
logger = logging.getLogger('algo_tester')

def getDateTime():
  datetime_obj_hour_fwd=datetime.now(tz)
  return datetime_obj_hour_fwd

datetime_obj_hour_fwd=getDateTime()+timedelta(hours=1)
from_date=str(datetime_obj_hour_fwd-timedelta(days=14)).split(" ")[0]
to_date=str(datetime_obj_hour_fwd).split(" ")[0]
super_trend_prop1={"range":7,"mult":3}
super_trend_prop2={"range":7,"mult":2}
rsi_prop={"range":14}


def trade(token):
    profit = 0.02
    stop_loss = 0.01
    last_price ={"_1": 0.00,"_2":0.00,"_3":0.00}
    last_min=-1
    holding={"_1": "","_2":"","_3":""}
    order_id={"_1": "","_2":"","_3":""}
    historical_data=""
    historical_data_rsi=""
    rsi_2=0.0
    while True:
        datetime_obj=getDateTime()
        min=int(str(datetime_obj).split(".")[0].split(":")[1])
        if(min%5==0 and (last_min==-1 or (min!=last_min and last_min!=-1))):
            historical_data = get_data(token,from_date,to_date,"5minute",historical_data)
            df = pd.DataFrame(historical_data)
            df = indicators.SuperTrend(df,super_trend_prop1['range'],super_trend_prop1['mult'],['open','high','low','close'])
            df = indicators.SuperTrend(df,super_trend_prop2['range'],super_trend_prop2['mult'],['open','high','low','close'])
            df = indicators.RSI(df,'close',rsi_prop["range"])
            tail_dict=df.tail(1).to_dict()
            index=list(tail_dict['open'].keys())[0]
            signal=tail_dict["STX_"+str(super_trend_prop1['range'])+"_"+str(super_trend_prop1['mult'])][index]
            signal2=tail_dict["STX_"+str(super_trend_prop2['range'])+"_"+str(super_trend_prop2['mult'])][index]
            suptrenval=tail_dict["ST_"+str(super_trend_prop1['range'])+"_"+str(super_trend_prop1['mult'])][index]
            suptrenval2=tail_dict["ST_"+str(super_trend_prop2['range'])+"_"+str(super_trend_prop2['mult'])][index]
            rsi=tail_dict['RSI_'+str(rsi_prop["range"])][index]
            price = get_price_v1(token,last_price)
            if (datetime_obj.hour == 15 and datetime_obj.minute>15):
                profit = 0.01
                stop_loss = 0.005
            else:
                trade_one(token,last_price,rsi,signal,suptrenval,holding,index,order_id,price)
                rsi_2=trade_two(last_min,min,token,historical_data_rsi,last_price,rsi_2,signal,suptrenval,holding,index,order_id,price)
                trade_three(token,last_price,rsi,signal,signal2,suptrenval,suptrenval2,holding,index,order_id,price)
            write_log(str(rsi_2)+","+str(rsi)+","+str(signal)+","+str(signal2)+","+str(suptrenval)+","+str(suptrenval2)+","+str(datetime_obj)+"\n",rs1_trend_log)
            last_min=min
        else:
            price = get_price_v1(token,last_price)
            for num in ["_1","_2","_3"]:
                global file_name
                file_name=csv_mapping[num]
                stoper(token,last_price[num],profit,stop_loss,datetime_obj,holding[num],order_id[num],price[num])
        time.sleep(1)      

def trade_one(token,last_price,rsi,signal,suptrenval,holding,index,order_id,price):
    num='_1'
    global file_name
    file_name=csv_mapping[num]
    (holding[num],order_id[num],last_price[num])=place_orders_2(signal,suptrenval,token,holding[num],last_price[num],rsi,index,order_id[num],price[num])
      
def trade_two(last_min,min,token,historical_data_rsi,last_price,rsi_2,signal,suptrenval,holding,index,order_id,price): #rsi_2 is equivalent to rsi on 1 hours
    num='_2'
    global datetime_obj_hour_fwd
    global file_name
    file_name=csv_mapping[num]
    if(last_min==-1 or  min==15):
        historical_data_rsi = get_data(token,from_date,to_date,"hour",historical_data_rsi)
        historical_data_rsi_df=pd.DataFrame(historical_data_rsi)
        df_rsi = indicators.RSI(historical_data_rsi_df,'close',14)
        tail_dict = df_rsi.tail(1).to_dict()
        index1=list(tail_dict['open'].keys())[0]
        rsi_2=tail_dict['RSI_'+str(rsi_prop["range"])][index1]
        #datetime_obj_hour_fwd=getDateTime()+timedelta(hours=1)
    (holding[num],order_id[num],last_price[num])=place_orders_2(signal,suptrenval,token,holding[num],last_price[num],rsi_2,index,order_id[num],price[num])   
    return rsi_2


def trade_three(token,last_price,rsi,signal,signal2,suptrenval,suptrenval2,holding,index,order_id,price): 
    num="_3"
    global file_name
    file_name=csv_mapping[num]
    (holding[num],order_id[num],last_price[num])=place_orders_3(signal,signal2,suptrenval,suptrenval2,token,holding[num],last_price[num],rsi,index,order_id[num],price[num])


def place_orders_3(signal,signal2,suptrenval,suptrenval2,token,holding,last_price,rsi,index,order_id,price):
        #Stoping condition
    if((signal!=signal2 and (suptrenval2>price>suptrenval  or  suptrenval>price>suptrenval2) and holding!='') or (holding=='up' and signal=='down' and signal2=='down' and rsi<50) or (holding=='down' and signal=='up' and signal2=='up' and rsi>50)):
        flag='loss'
        holding = ''
        if(price-last_price>0):
            flag=='profit'
        if(holding=='up'):
            write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+","+flag+","+str(getDateTime())+"\n")
        else:
            write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+","+flag+","+str(getDateTime())+"\n")
    if(signal=='down' and signal2=='down' and price<suptrenval and price<suptrenval2 and holding!='down' and rsi<50):
        order_id=index
        write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+",supertrend,"+str(getDateTime())+"\n")
        holding = 'down'
        last_price = price
    elif(signal=='up' and signal2=='up' and price>suptrenval and price>suptrenval2 and holding!='up' and rsi>50):
        order_id=index
        write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+",supertrend,"+str(getDateTime())+"\n")
        holding = 'up'
        last_price = price
    return    (holding,order_id,last_price)  


    
    
def place_orders_2(signal,suptrenval,token,holding,last_price,rsi,index,order_id,price):
    if((holding=='up' and signal=='down' and rsi<50) or (holding=='down' and signal=='up' and  rsi>50)):
        flag='loss'
        holding = ''
        if(price-last_price>0):
            flag=='profit'
        if(holding=='up'):
            write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+","+flag+","+str(getDateTime())+"\n")
        else:
            write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+","+flag+","+str(getDateTime())+"\n")
        order_id=""
    if(signal=='down' and holding!='down' and rsi<50):
        order_id=index
        write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+",supertrend,"+str(getDateTime())+"\n")
        holding = 'down'
        last_price = price
    elif (signal == 'up' and holding != 'up' and rsi>50):
        order_id=index
        write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+",supertrend,"+str(getDateTime())+"\n")
        holding = 'up'
        last_price = price
    return    (holding,order_id,last_price)  


def stoper(token,last_price,profit,stop_loss,datetime_obj,holding,order_id,price):
    global file_name
    if (holding=='up'):
      temp_profit = (price-last_price)/last_price
      temp_loss = (last_price-price)/last_price
      if(temp_profit>=profit):
        holding = ''
        write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+",profit,"+str(getDateTime())+"\n")
        order_id=""
      elif(temp_loss>stop_loss):
        holding = ''
        write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+",stoploss,"+str(getDateTime())+"\n")  
        order_id=""  
      if (datetime_obj.hour == 15 and datetime_obj.minute>28):
        holding = ''
        write_log(str(order_id)+","+"Sell"+","+str(token)+","+str(price)+",market_close,"+str(getDateTime())+"\n")
        order_id=""
    elif (holding=='down'):
      temp_profit = (last_price-price)/last_price
      temp_loss = (price-last_price)/last_price
      if(temp_profit>=profit):
          holding = ''
          write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+",profit,"+str(getDateTime())+"\n")
          order_id=""
      elif(temp_loss>stop_loss):
        holding = ''
        write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+",stoploss,"+str(getDateTime())+"\n")
        order_id=""
      if (datetime_obj.hour == 15 and datetime_obj.minute>28):
        holding = ''
        write_log(str(order_id)+","+"Buy"+","+str(token)+","+str(price)+",market_close,"+str(getDateTime())+"\n")
        order_id=""
    return (holding,order_id,last_price)  


def write_log(log,name=file_name): 
    if(name.endswith("holdings")):
        name=file_name
        sendMessage(file_name+","+log)
    name=token+"/"+name
    f=open(name,'a')    
    f.write(log)    
    f.close()    




if __name__=="__main__":
    trade(token)



