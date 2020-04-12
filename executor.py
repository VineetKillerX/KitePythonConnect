import subprocess as sp
from datetime import datetime

tokens = ['1510401','341249','348929','356865','408065','779521']
processes = []
process_dictionary = {}
import time
import pytz

tz = pytz.timezone('Asia/Kolkata')


def get_key(val):
    for key, value in process_dictionary.items():
        if val == value:
            return key


def getDateTime():
    datetime_obj_hour_fwd = datetime.now(tz)
    return datetime_obj_hour_fwd


for token in tokens:
    p = sp.Popen(['python3.7', './kite_trading.py', token])
    process_dictionary[str(token)] = str(p.pid)
    print(str(token) + "  :  " + str(p.pid))
    processes.append(p)


while True:
    date = getDateTime()
    if (date.hour >= 15 and date.minute > 30) or (date.hour <= 9 and date.minute <= 10):
        print("Market Not Available at this moment, Please Try Again from 09:10 till 15:30")
        break
    else:
        for process in processes:
            pid = process.pid
            token = get_key(str(pid))
            if process.poll() is None:
                print("Process Running for Token " + str(token))
            else:
                print("Process Not Running for Token " + str(token))
                p = sp.Popen(['python3.7', './kite_trading.py', token])
                process_dictionary[str(token)] = str(p.pid)
                print(str(token) + "  :  " + str(p.pid))
                processes.append(p)
    time.sleep(60)