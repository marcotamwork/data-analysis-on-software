import csv
import pandas as pd
import codecs
import os
from datetime import datetime
import time
import numpy as np
import itertools
from operator import itemgetter
from collections import Counter
from itertools import groupby
import re
from collections import Counter
from collections import OrderedDict


_start_time = time.time()
def tic():
    global _start_time 
    _start_time = time.time()

def tac():
    t_sec = round(time.time() - _start_time)
    (t_min, t_sec) = divmod(t_sec,60)
    (t_hour,t_min) = divmod(t_min,60) 
    print('Time passed: {}hour:{}min:{}sec'.format(t_hour,t_min,t_sec))
    
tic()
###############################################################################
def readcsv(cv,system):
        list1=[]
        ifile  = open(cv, "rb")
        read = csv.reader(codecs.iterdecode(ifile, 'utf-8'))
        for row in read :
            if 'system' in row:
                list1.append(row)
            elif system in row:
                list1.append(row)
        df = pd.DataFrame(list1[1:],columns = list1[0])
        return df


def readcsvforid(cv):
        list1 = []
        ifile  = open(cv, "rb")
        read = csv.reader(codecs.iterdecode(ifile, 'utf-8'))
        for row in read :
            if 'action' in row:
                column1 = row
            else:
                list1.append(row)
        ifile.close()
        if len(list1) > 0:
            df = pd.DataFrame(list1,columns = column1)
            return df
        else:
            return []
        
        
def identityfunction(word):
        pos = re.split('(?=[A-Z])', word)
        return pos[0]
    
        
        
def dictforfun(word):
        Dict = {'update': ['query','retrieval','request','authorise','generate','confirm','do','updateh', 'trigger','accept', 'set', 'change', 'initiate', 'create', 'cancel', 'insert', 'update', 'liftup', 'delete', 'force','validate', 'reassign', 'activate', 'add', 'lock', 'book','retrieve','updte','print','send','amend','capture','start','perform','gen','endorse','catch','unlock','final','batch','apply','stop','regen', 'assign', 'invoke', 'init', 'settle','checkin','list','revert','manual','select', 'relocate','relock','reject'],'find':['proceed','cal','finde','trf', 'reopen','checkout','re','receive','get','search','refresh','load','display','open','find','complete','check','verify','view','ack']}
        result = [key for key in Dict if word in Dict[key]][0] 
        return result
    

def processidentity(row,index):
        steps = [row[row.columns[i]].loc[index].split(':')[0] for i  in range(len(row.columns)) if ('step' in row.columns[i]) and (row[row.columns[i]].loc[index] != '')]
        res = [dictforfun(identityfunction(word))for word in steps]
        ans = list(OrderedDict.fromkeys(res))
        tf = [ ((ans[i-1] == 'find') and (ans[i] == 'update')) for i in range(1,len(ans)) ]
        if True in tf:
            return row
            
        
        
def rowindf(j,read):
        rows = [x for x in [processidentity(read[j].loc[[i]],i) for i,row in read[j].iterrows() if (read[j]['step 0'].loc[i] != 'Only auth and logoff') and ('F' in read[j]['machine_no'].loc[i])] if x is not None]
        if len(rows) > 0:
            return pd.concat(rows, ignore_index=True, sort=False).reset_index()
        else:
            return pd.DataFrame()
        
def readdf(role):
    print(role)
    f =  [ name for name in os.listdir(os.path.join(os.getcwd(),'saving',role)) if (os.path.isfile(os.path.join(os.getcwd(),'saving',role, name)))and ('.csv' in name) ]
    print(f)
    read = [readcsv(os.path.join(os.getcwd(),'saving',role,i),'CSS') for i in f]#read all
    dfs = [rowindf(j,read) for j in range(len(read))]
    df= pd.concat(dfs, ignore_index=True, sort=False).reset_index()
    if not df.empty:
        del df['level_0']
        del df['index']
    return df

def sdandmean(df):
    if not df.empty:
        df['time spent'] = pd.to_timedelta(df['time spent'])
        print(df['time spent'].iloc[0])
        print(type(df['time spent'].iloc[0]))
        SD = df['time spent'][df['time spent'].dt.total_seconds() > 0].std()
        mean = df['time spent'][df['time spent'].dt.total_seconds() > 0].mean()
        return SD,mean
    else:
        return 'nan','nan'


role = ['OTHERS','OC','SUPR','MANAGER']

df = [readdf(role[i]) for i in range(len(role))]

F = [str(i) for i in range(7)]
list_df = [[df_r[list(df_r['machine_no'].str)[1] == i]  for i in F]for df_r in df]
SDandMEAN = [[sdandmean(role[i]) for i in range(len(role))]for role in list_df]
df_SDmean = pd.DataFrame(SDandMEAN,index = role,columns = ['SD','Mean'])

###############################################################################
#count = [ [k,]*v for k,v in Counter(res).items()]
tac()


