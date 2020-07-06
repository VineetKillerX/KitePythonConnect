import subprocess as sp
from datetime import datetime

tokens = ['INFY','RELIANCE','TCS','HDFC','HDFCBANK','ICICIBANK','AXISBANK','CIPLA',"SBIN"]
#tokens = ['RELIANCE']
processes = []
process_dictionary = {}
import time
import pytz
python_version='python'
tz = pytz.timezone('Asia/Kolkata')


def get_key(val):
    for key, value in process_dictionary.items():
        if val == value:
            return key


def getDateTime():
    datetime_obj_hour_fwd = datetime.now(tz)
    return datetime_obj_hour_fwd


for token in tokens:
    p = sp.Popen([python_version, './bollinger_rsi2.py', token])
    process_dictionary[str(token)] = [str(p.pid),p]
    print(str(token) + "  :  " + str(p.pid))


while True:
    date = getDateTime()
    if (date.hour >= 15 and date.minute > 30) or (date.hour <= 9 and date.minute <= 10):
        if date.hour == 9:
            print("Market Not Available at this moment, Please Try Again in few minutes")
            time.sleep(60)
        else:
            print("Market Not Available at this moment, Please Try Again from 09:10 till 15:30")
            time.sleep(60*60)
    else:
        for token in process_dictionary:
            pid = process_dictionary[token][0]
            process=process_dictionary[token][1]
            if process.poll() is None:
                print("Process Running for Token " + str(token))
            else:
                print("Process Not Running for Token " + str(token))
                p = sp.Popen([python_version, './bollinger_rsi2.py', token])
                process_dictionary[str(token)] = [str(p.pid),p]
                print(str(token) + "  :  " + str(p.pid))
    time.sleep(60)
