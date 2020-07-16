'''User ID  Timestamp   Event Type  Name of Document    Type of Document    Source  Keywords    People  Sender  Receiver    Start Time  End Time    Origin  Destination Justification   Innocent or Guilty  Suspect '''

import json
import os
import pandas as pd
def read_json(fname):
    #import pdb; pdb.set_trace()
    df=pd.read_json(fname)
    return df

#    with open(fname, 'r') as f:
#        jarray= json.load(f)
userf = "userLog.json"
df = read_json(userf)
valid_user = []
for ui in set(df['user']):
    dfui = df[df['user']==ui].sort_values('time')
    if "submit finalize" in set(dfui['action']):
        valid_user.append(ui)
        if not os.path.exists("userlog"+"_"+str(ui)+".csv"):
            dfui.to_csv("userlog"+"_"+str(ui)+".csv",index=False)
#savedf = read_json("savedsearch.json")
#savedf.to_csv("savedsearch.csv",index=False)
print(len(valid_user))
def searchdf(ndf,data,user,time):
    ndf['InnocentOrGuilty'].append('N/A')
    ndf['Suspect'].append('N/A')
    ndf['UserID'].append(user)
    ndf['Timestamp'].append(time)
    ndf['EventType'].append(data['type'])
    ndf['DocumentName'].append(data['strings']['Name'])
    if "Document Type" in data['selects']:
        ndf['DocumentType'].append(data['selects']['Document Type'])
    else:
        ndf['DocumentType'].append('Any')
    if "Document Source" in data['selects']:
        ndf['Source'].append(data['selects']['Document Source'])
    else:
        ndf['Source'].append("Any")
    if 'Keyword' in data['strings']:
        ndf['Keywords'].append(data['strings']['Keyword'])
    else:
        ndf['Keywords'].append("")
    if "Person" in data['strings']:
        ndf['People'].append(data['strings']['Person'])
    else:
        ndf['People'].append("")
    if "Sender" in data['strings']:
        ndf['Sender'].append(data['strings']['Sender'])
    else:
        ndf['Sender'].append("")
    if "Receiver" in data['strings']:
        ndf['Receiver'].append(data['strings']['Receiver'])
    else: 
        ndf['Receiver'].append("")
    if "start" in data['times']:
        ndf['StartTime'].append(data['times']['start'])
    else:
        ndf['StartTime'].append("")
    if "end" in data['times']:
        ndf['EndTime'].append(data['times']['end'])
    else:
        ndf['EndTime'].append("")
    if 'Origin' in data['locations']:
        ndf['Origin'].append(data['locations']['Origin'])
    else:
        ndf['Origin'].append("")
    if 'Destination' in data['locations']:
        ndf['Destination'].append(data['locations']['Destination'])
    else:
        ndf['Destination'].append("")
    ndf['ReasoningText'].append(data['reasoningText'])
    ndf['ReasoningSelect'].append(data['reasoningSelect'])

def readdoc(ndf,data,user,time):
    ndf['InnocentOrGuilty'].append('N/A')
    ndf['Suspect'].append('N/A')
    ndf['UserID'].append(user)
    ndf['Timestamp'].append(time)
    ndf['EventType'].append('read')
    ndf['DocumentName'].append(data['Name'])
    if "Document Type" in data:
        ndf['DocumentType'].append(data['Document Type'])
    else:
        ndf['DocumentType'].append('Any')
    if "Document Source" in data:
        ndf['Source'].append(data['Document Source'])
    else:
        ndf['Source'].append("Any")
    if 'Keyword' in data:
        ndf['Keywords'].append(data['Keyword'])
    else:
        ndf['Keywords'].append("")
    if "Person" in data:
        ndf['People'].append(data['Person'])
    else:
        ndf['People'].append("")
    if "Sender" in data:
        ndf['Sender'].append(data['Sender'])
    else:
        ndf['Sender'].append("")
    if "Receiver" in data:
        ndf['Receiver'].append(data['Receiver'])
    else: 
        ndf['Receiver'].append("")
    if "Start Time" in data:
        ndf['StartTime'].append(data['Start Time'])
    else:
        ndf['StartTime'].append("")
    if "'End Time" in data:
        ndf['EndTime'].append(data['End Time'])
    else:
        ndf['EndTime'].append("")
    if 'Origin' in data:
        ndf['Origin'].append(data['Origin'])
    else:
        ndf['Origin'].append("")
    if 'Destination' in data:
        ndf['Destination'].append(data['Destination'])
    else:
        ndf['Destination'].append("")
    ndf['ReasoningText'].append('N/A')
    ndf['ReasoningSelect'].append('N/A')

def assignsave(ndf,data,user,time,suspect,judge):
    ndf['InnocentOrGuilty'].append(judge)
    ndf['Suspect'].append(suspect)
    ndf['UserID'].append(user)
    ndf['Timestamp'].append(time)
    ndf['EventType'].append("assign")
    ndf['DocumentName'].append(data['strings']['Name'])
    if "Document Type" in data['selects']:
        ndf['DocumentType'].append(data['selects']['Document Type'])
    else:
        ndf['DocumentType'].append('Any')
    if "Document Source" in data['selects']:
        ndf['Source'].append(data['selects']['Document Source'])
    else:
        ndf['Source'].append("Any")
    if 'Keyword' in data['strings']:
        ndf['Keywords'].append(data['strings']['Keyword'])
    else:
        ndf['Keywords'].append("")
    if "Person" in data['strings']:
        ndf['People'].append(data['strings']['Person'])
    else:
        ndf['People'].append("")
    if "Sender" in data['strings']:
        ndf['Sender'].append(data['strings']['Sender'])
    else:
        ndf['Sender'].append("")
    if "Receiver" in data['strings']:
        ndf['Receiver'].append(data['strings']['Receiver'])
    else: 
        ndf['Receiver'].append("")
    if "start" in data['times']:
        ndf['StartTime'].append(data['times']['start'])
    else:
        ndf['StartTime'].append("")
    if "end" in data['times']:
        ndf['EndTime'].append(data['times']['end'])
    else:
        ndf['EndTime'].append("")
    if 'Origin' in data['locations']:
        ndf['Origin'].append(data['locations']['Origin'])
    else:
        ndf['Origin'].append("")
    if 'Destination' in data['locations']:
        ndf['Destination'].append(data['locations']['Destination'])
    else:
        ndf['Destination'].append("")
    ndf['ReasoningText'].append(data['reasoningText'])
    ndf['ReasoningSelect'].append(data['reasoningSelect'])


newdic = {"UserID":[],"Timestamp":[],"EventType":[],"DocumentName":[],"DocumentType":[],"Source":[], "Keywords":[],"People":[],"Sender":[],"Receiver":[],"StartTime":[],"EndTime":[],"Origin":[], "Destination":[],"ReasoningText":[],"ReasoningSelect":[],"InnocentOrGuilty":[],"Suspect":[]}
for index, row in df.iterrows():
    if row['action']=='user justification':
        try:
            searchdf(newdic,row['data'],row['user'],row['time'])
        except:
            import pdb; pdb.set_trace()
            print(row['data'])
    if row['action']== 'open modal':
        readdoc(newdic,row['data'],row['user'],row['time'])
    if 'drop' in row['action']:
        suspectAndjudge = row['action'].split('to')[1].strip().split(" ")[0]
        suspect = suspectAndjudge.split("_")[0]
        try:
            judge = suspectAndjudge.split("_")[1]
            if judge=='Crime':
                judge = "Guilty"
            else:
                judge = "Innocent"
        except:
            judge = "N/A" 
        assignsave(newdic,row['data'],row['user'],row['time'],suspect,judge)
print(len(newdic['EventType']))
newdf = pd.DataFrame(newdic)
validnewdf = newdf[newdf['UserID'].isin(valid_user)]
renamedic = {'WebSearch': "discover", "recover": "discover"}
validnewdf.loc[validnewdf['EventType']=='WebSearch','EventType'] = "discover"
validnewdf.loc[validnewdf['EventType']=='recover','EventType'] = "discover"
validnewdf.loc[validnewdf['EventType']=='search from table','EventType'] = "search"
validnewdf.loc[validnewdf['EventType']=='save important document','EventType'] = "save"
validnewdf.loc[validnewdf['EventType']=='save search','EventType'] = "save"
validnewdf = validnewdf.sort_values('Timestamp') 
validnewdf.to_csv("validuserlog.csv",index=False,header=True,columns=newdic.keys())
import pdb; pdb.set_trace()
print(len(validnewdf))

