import json
import pandas as pd
from functools import reduce
validcsv = "validuserlog.csv"

df = pd.read_csv(validcsv)
alldf =[]
for ui in set(df['UserID']):
    dfui = df[df['UserID']==ui].sort_values('Timestamp')
    alldf.append(dfui)
df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['EventType','DocumentName','DocumentType','Source','Keywords','People','Sender','Receiver','StartTime','EndTime','Origin','Destination','InnocentOrGuilty','Suspect'],how='inner'), alldf)
df_merged.to_csv("commonrows.csv",index=False,header=True)
