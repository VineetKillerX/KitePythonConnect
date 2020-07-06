import requests
import io
from yahoo_fin.stock_info import *
import traceback
def get_data(token,from_date,to_date,interval,historical_data,key,counter=0):
    try:
        r=requests.get("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=NSE:"+str(token)+"&interval="+str(interval)+"&apikey="+key+"&datatype=csv")
        print(r.content)
        c=pd.read_csv(io.StringIO(r.content.decode('utf-8')))
        c['timestamp']=pd.to_datetime(c.timestamp)+pd.to_timedelta(570, unit='m')
        k=c.sort_values("timestamp",ascending=True)
        return k
    except:
        print("Exception Raised : ",token,traceback.format_exc())
        if(counter<=3):
            print("get_data exception ",counter)
            return get_data(token,from_date,to_date,interval,historical_data,key,counter+1)
        else:
            print("returning old history_df ")
            return historical_data


def get_price(token,price,counter=0):
    try:
        return get_live_price(token+".NS")
    except:
        if(counter<=3):
            print("get_price exception ",counter)
            return get_price(token,price,counter+1)
        else:
            print("returning old price ",price)
            return price


def get_price_v1(token,last_price_dict,counter=0):
    return_dict={}
    try:
        price=kite.quote(token)[token]['last_price']
        for key in last_price_dict:
            return_dict[key]=price
        return return_dict
    except:
        if(counter<=3):
            print("get_price exception ",counter)
            return get_price(token,price,counter+1)
        else:
            print("returning old price ",last_price_dict)
            return last_price_dict


def get_10_data(token,from_date,to_date,interval,historical_data,key,counter=0):
    try:
        r=requests.get("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=NSE:"+str(token)+"&interval=5min&apikey="+key+"&datatype=csv")
        print(r.content)
        c=pd.read_csv(io.StringIO(r.content.decode('utf-8')))
        c['timestamp']=pd.to_datetime(c.timestamp)+pd.to_timedelta(570, unit='m')
        k=c.sort_values("timestamp",ascending=True)
        if(int(str(k[0:1].timestamp).split("\n")[0].split(" ")[-1].split(":")[1])%10==0):
            k=k[1:]
        k['open_s']=k.open.shift(-1)
        k['close_s']=k.close.shift(-1)
        k['high_s']=k.high.shift(-1)
        k['low_s']=k.low.shift(-1)
        k['volume_s']=k.volume.shift(-1)
        k['seq']=pd.Series([i for i in range(1,k.shape[0]+1)][::-1])
        k=k[k.apply(lambda x:x['seq']%2==1,axis=1)]
        
        return k
    except:
        print("Exception Raised : ",token,traceback.format_exc())
        if(counter<=3):
            print("get_data exception ",counter)
            return get_data(token,from_date,to_date,interval,historical_data,key,counter+1)
        else:
            print("returning old history_df ")
            return historical_data