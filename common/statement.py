import pandas as pd
import numpy as np
from SlackUtil import sendMessage
headers = ['OrderId', 'Action', 'StockId', 'Price', 'Signal', 'DateTime']

for i in ['1510401','3050241','779521','408065','356865','348929','341249']:
    file_name = './'+i+'/holdings_2_sup_rsi.csv'
    try:
        df = pd.read_csv(file_name, names=headers)
        mydf = df.groupby('OrderId').filter(lambda x: len(x) > 1)
        mydf['diff'] = abs(mydf['Price'] - mydf['Price'].shift(1))
        statements = mydf.iloc[1::2]
        statements['Profit'] = np.where(statements['Signal'] == 'profit',statements['diff'], -statements['diff']).astype(float)
        total_trades = statements.shape[0]
        total_profit_per_share = statements['Profit'].sum()
        message = "Total Trades Done Today for "+i+" is : "+str(total_trades)+" And Earned : "+str(total_profit_per_share)
        sendMessage(message)
    except:
        print("Error")
