tokens = ['738561','2953217','341249','356865','340481','408065','2714625','492033','424961','1270529']
import pandas as pd
import numpy as np
total = 0.00
headers = ['OrderID','SuperTrend','RSI','RSI_Slope','WMA20','WMA5','WMA14','Close_Price','Current_Price','Action','Holding','Last_Price']
for token in tokens:
    df = pd.read_csv('./' + token + "/trades.csv", names=headers)
    df_o = df[df['OrderID'].notna()]
    df_a = df_o[df_o['Action'] != 'HOLD']
    df_a = df_a.iloc[1::2]
    df_a['Change'] = df_a['Last_Price'] - df_a['Current_Price']
    df_a['Change'] = np.where(df_a['Action'] == 'SELL', -df_a['Change'], df_a['Change'])
    profl = df_a.sum().Change
    total = total + profl
    print("for Token : ",token,"Profit/Loss : ",profl)

print("Total Profit/Loss Today : ",total)





