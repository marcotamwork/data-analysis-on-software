import csv
import pandas as pd
import codecs
import os
from os import walk
from datetime import datetime
import time
import numpy as np
import itertools
from operator import itemgetter
from collections import Counter
from itertools import groupby 
from datetime import date

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


def readcsv(cv):
        list1=[]
        ifile  = open(cv, "rb")
        read = csv.reader(codecs.iterdecode(ifile, 'utf-8'))
        for row in read :
            list1.append(row)
        df = pd.DataFrame(list1[1:],columns = list1[0])
        return df
def readcsvforid(cv):
        list1=[]
        if 'q3_1week_20to26_ip_log_data' in cv:
            ifile  = open(cv, "rb")
            read = csv.reader(codecs.iterdecode(ifile, 'utf-8'))
            for row in read :
                if 'IP' in row:
                    column1 = row
                else:
                    list1.append(row)
            ifile.close()
            df = pd.DataFrame(list1,columns = column1)

            return df            
            
        elif 'VEHICLE_TRIP_AUDIT_TRAIL_202008051525' in cv:
            ifile  = open(cv, "rb")
            read = csv.reader(codecs.iterdecode(ifile, 'utf-8'))
            for row in read :
                if 'CREATED_DATETIME' in row:
                    column1 = row
                else:
                    list1.append(row)
                    
            ifile.close()
            if len(list1) > 0:
                df = pd.DataFrame(list1,columns = column1)
                df.rename(columns={'CREATED_DATETIME':'start','CREATED_BY':'ID','CREATED_BY_FUNCTION':'action'}, inplace=True)
                df = df.drop(['TRANSACTION_UID','VEHICLE_TRIP_ID','COLUMN_BEFORE_VALUE','COLUMN_AFTER_VALUE','TABLE_NAME','COLUMN_NAME','ACTION'], axis=1)
                df['start'] =  pd.to_datetime(df['start'])
                df = df[(df['start'] >  date(2020, 7, 20)) & (df['start'] <  date(2020, 7, 27))]
                df['ID'] = df['ID'].str.upper()

                return df
            else:
                return []

        else:    
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

        

def sumoflist(list1,i,j):
    list2 = []
    for x in list1[i:j]:
        list2.extend(x)
    return list2

        
        
def nat_check(nat):
    return nat == np.datetime64('NaT')  

def handleforlist(df,i):
    df_auth = df[(df['function'] == 'AUTHENTICATE|EAF')|(df['function'] == 'LOGOUT|ELAPSED')]
    df_auth['index_col'] = df_auth.index
    df_auth = df_auth.reset_index(drop=True)
#######################
    if 'CPlus' in f[i+1]:
        print('CPlus')
        df_perlog = pd.DataFrame()
        save_group = []
        for i,row in df_auth[:-1].iterrows():
            if df_auth['function'].iloc[i] == 'AUTHENTICATE|EAF':################################################need to split by time(timebreak)
                df_funneed = df[df_auth['index_col'].iloc[i]+1:df_auth['index_col'].iloc[i+1]].reset_index(drop=True)
                df_funneed.loc[df_funneed['action'].str.contains('authorise|generate|confirm|do|updateh|trigger|accept|set|change|initiate|create|cancel|insert|update|liftup|delete|force|validate|reassign|activate|add|lock|book|retrieve|updte|print|send|amend|capture|start|perform|gen|endorse|catch|unlock|final|batch|apply|stop|regen|assign|invoke|init|settle|checkin|list|revert|manual|select|relocate|relock|reject', na=False, regex=True),'function'] = 'update'
                df_funneed.loc[df_funneed['action'].str.contains('checkLogReadAndUpdateEgRuleLog', na=False, regex=True),'function'] = 'Find'

               #**************************************************************************************************
                
                group_ids = (df_funneed['start']>(df_funneed['start'].shift()+ pd.to_timedelta(300,unit='second'))).cumsum()
                grouped = df_funneed.groupby(group_ids)
                group_list = [g for k,g in grouped]
                save_group.append(group_list)
                
                #check for time break
                #**************************************************************************************************
                if len(group_list)>0:
                    for df_need in group_list:
                        if len(df_need)>0:
                            timeend_save =  df_need['start'].tail(1).values
                            df_count = df_need['action'].tolist()
                            res = [": ".join([label, str(sum(1 for _ in group))]) for label, group in groupby(df_count)]
                            size = len(res)
                            idx_list = [idx + 1 for idx, val in enumerate(res) if( 'authorise' in val ) or ( 'generate' in val ) or ( 'confirm' in val ) or ( 'do' in val ) or ( 'updateh' in val ) or ( 'trigger' in val ) or ( 'accept' in val ) or ( 'set' in val ) or ( 'change' in val ) or ( 'initiate' in val ) or ( 'create' in val ) or ( 'cancel' in val ) or ( 'insert' in val ) or ( 'update' in val ) or ( 'liftup' in val ) or ( 'delete' in val ) or ( 'force' in val ) or ( 'validate' in val ) or ( 'reassign' in val ) or ( 'activate' in val ) or ( 'add' in val ) or ( 'lock' in val ) or ( 'book' in val ) or ( 'retrieve' in val ) or ( 'updte' in val ) or ( 'print' in val ) or ( 'send' in val ) or ( 'amend' in val ) or ( 'capture' in val ) or ( 'start' in val ) or ( 'perform' in val ) or ( 'gen' in val ) or ( 'endorse' in val ) or ( 'catch' in val ) or ( 'unlock' in val ) or ( 'final' in val ) or ( 'batch' in val ) or ( 'apply' in val ) or ( 'stop' in val ) or ( 'regen' in val ) or ( 'assign' in val ) or ( 'invoke' in val ) or ( 'init' in val ) or ( 'settle' in val ) or ( 'checkin' in val ) or ( 'list' in val ) or ( 'revert' in val ) or ( 'manual' in val ) or ( 'select' in val ) or ( 'relocate' in val ) or ( 'relock' in val ) or ( 'reject' in val )]

                            if len(idx_list) >0:
                                fun_up = [res[i: j] for i, j in zip([0] + idx_list, idx_list + ([size] if idx_list[-1] != size else []))]
                                numof1 = [i for i in range(1,len(fun_up)) if (any(x in fun_up[i][0] for x in ['authorise','generate','confirm','do','updateh', 'trigger','accept', 'set', 'change', 'initiate', 'create', 'cancel', 'insert', 'update', 'liftup', 'delete', 'force','validate', 'reassign', 'activate', 'add', 'lock', 'book','retrieve','updte','print','send','amend','capture','start','perform','gen','endorse','catch','unlock','final','batch','apply','stop','regen', 'assign', 'invoke', 'init', 'settle','checkin','list','revert','manual','select', 'relocate','relock','reject']))  and (len(fun_up[i]) == 1)]
                                if len(numof1)>0:                                
                                    fun_out = [sumoflist(fun_up,i,j) for i, j in zip([i for i in list(range(len(fun_up))) if i not in numof1],[i+1 for i in list(range(len(fun_up))) if i+1 not in numof1])]
                                else:
                                    fun_out = fun_up
                            else:
                                fun_out = [res]

                            df_fun_out = pd.DataFrame(fun_out) ####action/function
                            df_fun_out.columns =  ['step ' + str(x) for x in df_fun_out.columns]
                            df_time = df_need.loc[df_need['function'] != df_need['function'].shift()].reset_index(drop=True)

                            if df_time['function'].iloc[0] != 'update':
                                df_info = df_time[df_time['function'] == 'Find'][['ID','machine_no','system']].reset_index(drop=True) ###info
                            else:
                                df_firup =  df_time.head(1)[['ID','machine_no','system']]
                                df_secfind = df_time[df_time['function'] == 'Find'][['ID','machine_no','system']]
                                df_info = pd.concat([df_firup, df_secfind], ignore_index=True, sort=False)

                            df_time_a = [pd.concat([df_time['start'].loc[j:j+1].shift(-i) for i in range(2)], axis=1, keys=['start','end']).head(1) for j in df_time.index if (j == 0) or (df_time['function'].iloc[j] == 'Find') ]
                            
                            df_info['auth'] = df_auth['start'].iloc[i]
                            df_info['logoff'] =  df_auth['start'].iloc[i+1]
                            

                            df_time_ans = pd.concat(df_time_a).reset_index(drop=True)
                            if pd.isnull(df_time_ans['end'].iloc[-1]):
                                df_time_ans['end'].iloc[-1] = timeend_save
                            
                            df_time_ans['time spent'] = df_time_ans['end'] - df_time_ans['start']####Time
                            result_persession = pd.concat([df_info, df_time_ans, df_fun_out], axis=1)
                            df_perlog = pd.concat([df_perlog, result_persession], ignore_index=True, sort=False)
                    
                    
                else:
                    df_info = df_auth[['ID','machine_no','start']].iloc[i].to_frame().T
                    df_end =  df_auth['start'].iloc[i+1]
                    df_info['end'] = df_end
                    df_timediff = df_end - df_info['start']
                    df_info['time spent'] = df_timediff
                    df_info['step 0'] = 'Only auth and logoff'        
                    df_info['auth'] = df_info['start'].iloc[0]
                    df_info['logoff'] =  df_auth['start'].iloc[i+1]
                    df_perlog = pd.concat([df_perlog, df_info], ignore_index=True, sort=False)
                    
                
        df_perlog['system'] = 'CPlus'
        return df_perlog
        
    
    if 'BSS' in f[i+1]:
        print('BSS')
        df_perlog = pd.DataFrame()

        for i,row in df_auth[:-1].iterrows():
            if df_auth['function'].iloc[i] == 'AUTHENTICATE|EAF':################################################need to split by time(timebreak)
                df_funneed = df[df_auth['index_col'].iloc[i]+1:df_auth['index_col'].iloc[i+1]].reset_index(drop=True)
               #**************************************************************************************************
                
                group_ids = (df_funneed['start']>(df_funneed['start'].shift()+ pd.to_timedelta(300,unit='second'))).cumsum()
                grouped = df_funneed.groupby(group_ids)
                group_list = [g for k,g in grouped]
                
                #check for time break
                #**************************************************************************************************  
                
                if len(group_list)>0:
                    for df_need in group_list:
                        if len(df_need)>0:
                            
                            timeend_save =  df_need['start'].tail(1).values
                            df_count = df_need['action'].tolist()
                            
                            fun_out = [": ".join([label, str(sum(1 for _ in group))]) for label, group in groupby(df_count)]
                            df_fun_out = pd.DataFrame(data = [fun_out]) ####action/function
                            df_fun_out.columns =  ['step ' + str(x) for x in df_fun_out.columns]
                            df_time = df_need.loc[df_need['function'] != df_need['function'].shift()].reset_index(drop=True).iloc[[0, -1]].reset_index(drop=True)
                            df_info = df_time.head(1)[['ID','machine_no','system']]###info
                            df_time_a = [pd.concat([df_time['start'].loc[j:j+1].shift(-i) for i in range(2)], axis=1, keys=['start','end']).head(1) for j in df_time.index if j == 0]

                            df_time_ans = pd.concat(df_time_a).reset_index(drop=True)
                            if pd.isnull(df_time_ans['end'].iloc[-1]):
                                df_time_ans['end'].iloc[-1] = timeend_save
                            df_time_ans['time spent'] = df_time_ans['end'] - df_time_ans['start']####Time
                            df_info['auth'] = df_auth['start'].iloc[i]
                            df_info['logoff'] =  df_auth['start'].iloc[i+1]
                            result_persession = pd.concat([df_info, df_time_ans, df_fun_out], axis=1)
                            
                            df_perlog = pd.concat([df_perlog, result_persession], ignore_index=True, sort=False)

                            
                    
                else:
                    df_info = df_auth[['ID','machine_no','start']].iloc[i].to_frame().T

                    df_end =  df_auth['start'].iloc[i+1]
                    df_info['end'] = df_end
                    df_timediff = df_end - df_info['start']
                    df_info['time spent'] = df_timediff
                    df_info['step 0'] = 'Only auth and logoff'           
                    df_info['auth'] = df_info['start'].iloc[0]
                    df_info['logoff'] =  df_auth['start'].iloc[i+1]
                    df_perlog = pd.concat([df_perlog, df_info], ignore_index=True, sort=False)
                    
        df_perlog['system'] = 'BSS'
        return df_perlog

    if'CSS' in f[i+1]:
        print('CSS')
        df_perlog = pd.DataFrame()
        for i,row in df_auth[:-1].iterrows():
            if df_auth['function'].iloc[i] == 'AUTHENTICATE|EAF':################################################need to split by time(timebreak)
                df_funneed = df[df_auth['index_col'].iloc[i]+1:df_auth['index_col'].iloc[i+1]].reset_index(drop=True)
               #**************************************************************************************************
                
                group_ids = (df_funneed['start']>(df_funneed['start'].shift()+ pd.to_timedelta(300,unit='second'))).cumsum()
                grouped = df_funneed.groupby(group_ids)
                group_list = [g for k,g in grouped]
                
                #check for time break
                #**************************************************************************************************  
                
                if len(group_list)>0:
                    for df_need in group_list:
                        if len(df_need)>0:
                            df_need['function'] = 'Find'   
                            df_need.loc[df_need['action'].str.contains('authorise|generate|confirm|do|updateh|trigger|accept|set|change|initiate|create|cancel|insert|update|liftup|delete|force|validate|reassign|activate|add|lock|book|retrieve|updte|print|send|amend|capture|start|perform|gen|endorse|catch|unlock|final|batch|apply|stop|regen|assign|invoke|init|settle|checkin|list|revert|manual|select|relocate|relock|reject', na=False, regex=True),'function'] = 'update'                        
                            timeend_save =  df_need['start'].tail(1).values
                            df_count = df_need['action'].tolist()
                            res = [": ".join([label, str(sum(1 for _ in group))]) for label, group in groupby(df_count)]
                            size = len(res)
                            idx_list = [idx + 1 for idx, val in enumerate(res) if( 'authorise' in val ) or ( 'generate' in val ) or ( 'confirm' in val ) or ( 'do' in val ) or ( 'updateh' in val ) or ( 'trigger' in val ) or ( 'accept' in val ) or ( 'set' in val ) or ( 'change' in val ) or ( 'initiate' in val ) or ( 'create' in val ) or ( 'cancel' in val ) or ( 'insert' in val ) or ( 'update' in val ) or ( 'liftup' in val ) or ( 'delete' in val ) or ( 'force' in val ) or ( 'validate' in val ) or ( 'reassign' in val ) or ( 'activate' in val ) or ( 'add' in val ) or ( 'lock' in val ) or ( 'book' in val ) or ( 'retrieve' in val ) or ( 'updte' in val ) or ( 'print' in val ) or ( 'send' in val ) or ( 'amend' in val ) or ( 'capture' in val ) or ( 'start' in val ) or ( 'perform' in val ) or ( 'gen' in val ) or ( 'endorse' in val ) or ( 'catch' in val ) or ( 'unlock' in val ) or ( 'final' in val ) or ( 'batch' in val ) or ( 'apply' in val ) or ( 'stop' in val ) or ( 'regen' in val ) or ( 'assign' in val ) or ( 'invoke' in val ) or ( 'init' in val ) or ( 'settle' in val ) or ( 'checkin' in val ) or ( 'list' in val ) or ( 'revert' in val ) or ( 'manual' in val ) or ( 'select' in val ) or ( 'relocate' in val ) or ( 'relock' in val ) or ( 'reject' in val )]
                            if len(idx_list) >0:
                                fun_up = [res[i: j] for i, j in zip([0] + idx_list, idx_list + ([size] if idx_list[-1] != size else []))]
                                numof1 = [i for i in range(1,len(fun_up)) if (any(x in fun_up[i][0] for x in ['authorise','generate','confirm','do','updateh', 'trigger','accept', 'set', 'change', 'initiate', 'create', 'cancel', 'insert', 'update', 'liftup', 'delete', 'force','validate', 'reassign', 'activate', 'add', 'lock', 'book','retrieve','updte','print','send','amend','capture','start','perform','gen','endorse','catch','unlock','final','batch','apply','stop','regen', 'assign', 'invoke', 'init', 'settle','checkin','list','revert','manual','select', 'relocate','relock','reject']))  and (len(fun_up[i]) == 1)]
                                if len(numof1)>0:                                
                                    fun_out = [[y for loc in fun_up[i:j] for y in loc] for i, j in zip([i for i in list(range(len(fun_up))) if i not in numof1],[i+1 for i in list(range(len(fun_up))) if i+1 not in numof1])]
                                else:
                                    fun_out = fun_up
                            else:
                                fun_out = [res]
                            df_fun_out = pd.DataFrame(fun_out) ####action/function
                            df_fun_out.columns =  ['step ' + str(x) for x in df_fun_out.columns]
                            df_time = df_need.loc[df_need['function'] != df_need['function'].shift()].reset_index(drop=True)
                            df_info = df_time[df_time['function'] == 'Find'][['ID','machine_no','system']].reset_index(drop=True) ###info

                            df_time_a = [pd.concat([df_time['start'].loc[j:j+1].shift(-i) for i in range(2)], axis=1, keys=['start','end']).head(1) for j in df_time.index if df_time['function'].iloc[j] == 'Find']

                            if len(df_time_a) > 0:

                                df_time_ans = pd.concat(df_time_a).reset_index(drop=True)
                                if pd.isnull(df_time_ans['end'].iloc[-1]):
                                    df_time_ans['end'].iloc[-1] = timeend_save
                                df_time_ans['time spent'] = df_time_ans['end'] - df_time_ans['start']####Time
                                result_persession = pd.concat([df_info, df_time_ans, df_fun_out], axis=1)
                            else:
                                df_info = df_time[df_time['function'] == 'update'][['ID','machine_no','system','start']].reset_index(drop=True) ###info
                                df_info['end'] = df_info['start']
                                df_info['time spent'] = df_info['end'] - df_info['start']####Time
                                result_persession = pd.concat([df_info, df_fun_out], axis=1)            
                                
                            result_persession['auth'] = df_auth['start'].iloc[i]
                            result_persession['logoff'] =  df_auth['start'].iloc[i+1]
                            df_perlog = pd.concat([df_perlog, result_persession], ignore_index=True, sort=False)
                else:
                    df_info = df_auth[['ID','machine_no','start']].iloc[i].to_frame().T

                    df_end =  df_auth['start'].iloc[i+1]
                    df_info['end'] = df_end

                    df_timediff = df_end - df_info['start']
                    df_info['time spent'] = df_timediff
                    df_info['step 0'] = 'Only auth and logoff'          
                    df_info['auth'] = df_info['start'].iloc[0]
                    df_info['logoff'] =  df_auth['start'].iloc[i+1]
                    df_perlog = pd.concat([df_perlog, df_info], ignore_index=True, sort=False)
                    
        df_perlog['system'] = 'CSS'
        return df_perlog




    if 'log_data' in f[i+1]:
    
        print('HCD')
        df_perlog = pd.DataFrame()
        for i,row in df_auth[:-1].iterrows():
            if df_auth['function'].iloc[i] == 'AUTHENTICATE|EAF':################################################need to split by time(timebreak)
                df_allneed = df[df_auth['index_col'].iloc[i]+1:df_auth['index_col'].iloc[i+1]].reset_index(drop=True)
                ip  = df_auth['location'].iloc[i]
                df_need = df_allneed[df_allneed['location'] == ip]    
                if len(df_need)>0:
                    timeend_save =  df_need['start'].tail(1).values
                    df_count = df_need['function'].tolist()
                    fun_out = [[": ".join([label, str(sum(1 for _ in group))]) for label, group in groupby(df_count)]]

                    df_fun_out = pd.DataFrame(fun_out) ####action/function
                    df_fun_out.columns =  ['step ' + str(x) for x in df_fun_out.columns]
                    df_info =  df_auth.loc[[i]][['ID']].reset_index(drop=True) ###info

                    df_time_a = [pd.concat([df_auth['start'].loc[i:i+2].shift(-j) for j in range(2)], axis=1, keys=['start','end']).head(1)]
                    df_time_ans = pd.concat(df_time_a).reset_index(drop=True)
                    if pd.isnull(df_time_ans['end'].iloc[-1]):
                        df_time_ans['end'].iloc[-1] = timeend_save
                    df_time_ans['time spent'] = df_time_ans['end'] - df_time_ans['start']####Time
                    
                    df_info['auth'] = df_auth['start'].iloc[i]
                    df_info['logoff'] =  df_auth['start'].iloc[i+1]

                    result_persession = pd.concat([df_info, df_time_ans, df_fun_out], axis=1)

                    df_perlog = pd.concat([df_perlog, result_persession], ignore_index=True, sort=False)
            
                else:
                    df_info = df_auth[['ID','machine_no','start']].iloc[i].to_frame().T
                    df_end =  df_auth['start'].iloc[i+1]
                    df_info['end'] = df_end
                    df_timediff = df_end - df_info['start']
                    df_info['time spent'] = df_timediff
                    df_info['step 0'] = 'Only auth and logoff'                    
                    df_info['auth'] = df_info['start'].iloc[0]
                    df_info['logoff'] =  df_auth['start'].iloc[i+1]
                    df_perlog = pd.concat([df_perlog, df_info], ignore_index=True, sort=False)
                
                    
        df_perlog['system'] = 'HCD'        
        return df_perlog
    
    if 'VEHICLE' in f[i+1]:
        print('VEHICLE')
        df_perlog = pd.DataFrame()
        save_group = []
        for i,row in df_auth[:-1].iterrows():
            if df_auth['function'].iloc[i] == 'AUTHENTICATE|EAF':################################################need to split by time(timebreak)

                
                df_funneed = df[df_auth['index_col'].iloc[i]+1:df_auth['index_col'].iloc[i+1]].reset_index(drop=True)
               #**************************************************************************************************
                
                group_ids = (df_funneed['start']>(df_funneed['start'].shift()+ pd.to_timedelta(300,unit='second'))).cumsum()
                grouped = df_funneed.groupby(group_ids)
                group_list = [g for k,g in grouped]
                save_group.append(group_list)
                
                #check for time break
                #**************************************************************************************************
                if len(group_list)>0:
                    for df_need in group_list:
                        if len(df_need)>0:
                            timeend_save =  df_need['start'].tail(1).values
                            df_count = df_need['action'].tolist()
                            fun_out = [[": ".join([label, str(sum(1 for _ in group))]) for label, group in groupby(df_count)]]
                            df_fun_out = pd.DataFrame(fun_out) ####action/function
                            df_fun_out.columns =  ['step ' + str(x) for x in df_fun_out.columns]
                            df_info = df_need.head(1)[['ID']].reset_index(drop=True) ###info
                            df_headtail = df_need['start'].iloc[[0, -1]]
                            df_time_a = [pd.concat([df_headtail.shift(-j) for j in range(2)], axis=1, keys=['start','end']).head(1)]
                            df_time_ans = pd.concat(df_time_a).reset_index(drop=True)
                            if pd.isnull(df_time_ans['end'].iloc[-1]):
                                df_time_ans['end'].iloc[-1] = timeend_save
                            df_info['auth'] = df_auth[['start']].iloc[i].to_frame().T
                            df_info['logoff'] =  df_auth['start'].iloc[i+1]
                            df_time_ans['time spent'] = df_time_ans['end'] - df_time_ans['start']####Time
                            result_persession = pd.concat([df_info, df_time_ans, df_fun_out], axis=1)
                            
                            df_perlog = pd.concat([df_perlog, result_persession], ignore_index=True, sort=False)
                            
                            
                        else:
                            df_info = df_auth[['ID','machine_no','start']].iloc[i].to_frame().T
                 
                            df_info['end'] = df_auth['start'].iloc[i+1]
                
                            df_timediff = df_end - df_info['start']
                            df_info['time spent'] = df_timediff
                            df_info['step 0'] = 'Only auth and logoff'
                            df_info['auth'] = df_auth['start'].iloc[i]
                            df_info['logoff'] =  df_auth['start'].iloc[i+1]
                            
                            df_perlog = pd.concat([df_perlog, df_info], ignore_index=True, sort=False)
                            
                
                    
        df_perlog['system'] = 'VEHICLE'        
        return df_perlog
    
    

f =  [ name for name in os.listdir(os.getcwd()) if (os.path.isfile(os.path.join(os.getcwd(), name)))and ('.csv' in name) ]
data_id_total = readcsv(f[0])
data_id_total = data_id_total.rename({'Time': 'start'}, axis=1) 

id_empty=[]
ID_0 = list(dict.fromkeys(data_id_total['ID']))
print('no. of ID: ', str(len(ID_0)))
###############################################################################
tac()

data_all = [readcsvforid(f[i]) for i in range(1,len(f))]
print('data saved!')
tac()

for i in ID_0:
    print(i)
    ID = i
    data_id = data_id_total[data_id_total['ID'] == ID ]
    data = [data_all[0]] + [data_all[no][data_all[no]['ID'] == ID] for no in range(1,len(data_all))]
    
    if isinstance(data[1], pd.DataFrame):
        del data[1]['end']
        data[1]['system'] = 'CPlus'
    if isinstance(data[0], pd.DataFrame):
        data[0].rename(columns={'IP': 'location'})
        data[0]['system'] = 'HCD'
    
    list123 = []
    id_store=pd.DataFrame()
    for i in range(len(data)):
        if not data[i].empty:
            ##############
            data_i = data_id.append(data[i], sort=False)
            data_i['start'] =  pd.to_datetime(data_i['start']) # transfrom string to datetime
            data_i = data_i.sort_values(by=['start']).reset_index(drop=True)
            
            dfsave = handleforlist(data_i,i)
            list123.append(dfsave)        
            id_store = id_store.append(dfsave, sort=False)
    steps = list(i for i in id_store.columns if 'step' in i) 
    list_name = list(i for i in id_store.columns if 'step' not in i) 
    list_col = list_name+steps
    id_store = id_store[list_col]
    
    if not id_store.empty:
        last = id_store.sort_values(by=['start']).reset_index(drop=True)
        

        csvname ='D:\\5_6\\task5\\saving\\'+ ID +'.csv'
        last.to_csv(csvname, index=False)  

    else:
        id_empty.append(ID)
    
    tac()
###############################################################################


tac()
