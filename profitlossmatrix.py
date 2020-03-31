import pandas as pd
df=pd.read_csv("holdings.csv")
df["sign"] = df.Signal.apply(lambda x:1 if (x=='profit' or x=='stoploss') else 0)
df["profit/loss"]=df.sign*df.Price
df["sign_i"] = df.Signal.apply(lambda x:0 if (x=='profit' or x=='stoploss') else 1)
df["investment"]=df.sign_i*df.Price
df=df.groupby("Instrument").sum()
return df