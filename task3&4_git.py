import os
import codecs
import time
import pandas as pd
from datetime import datetime

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
def datatosort(file_in_hcd,listoflogs,listofUI):
        print(file_in_hcd)
        store_L = []
        store_UI = []
        listoflogsnotexist = []
        path_logs= '/3/hcd/'+file_in_hcd+'/logs/'
        path_ui= '/3/hcd/'+file_in_hcd+'/ui'+'/logs/'
        starting_time = 0
            
        for i in range(len(listoflogs)):
            pathLinfile = path_logs + listoflogs[i] #set path
            if os.path.exists(pathLinfile) == True:
                with codecs.open(pathLinfile, 'r', encoding='utf8', errors='ignore') as f:
                    text = f.read().split('\n')
                for j in range(len(text)):
                    if 'authenticationService.authenticate() start' in text[j]:
                        
                        if j+2 < len(text):#########################
                            if ('  [com.xxx.xxx.xxx.appmgr.ui.LoginDialog$2]:Authentication Exception error Code ' in text[j+2]) or ('authenticationService.authenticate() end' not in text[j+2]):
                                if ('  [com.xxx.xxx.xxx.appmgr.ui.LoginDialog$2]:Authentication Exception error Code XXXXXXX' in text[j+2]):
                                    pass
                                else:
                                    continue 
                        content_list = text[j].split(' ')
                        datetime_L = (datetime.strptime((content_list[0]+' '+content_list[1]),'[%Y-%m-%d %H:%M:%S,%f'))
                        login = content_list[5].split(':')[1].split("(")[0].split(".")[1]                    
                        store_L.append([datetime_L,login,pathLinfile])
                    if '[PlatformSvcImpl]:   PlatformSvcImpl.launch' in text[j]:
                        content_list = text[j].split(' ')
                        datetime_L = (datetime.strptime((content_list[0]+' '+content_list[1]),'[%Y-%m-%d %H:%M:%S,%f'))
                        APP = content_list[6].split('(')[1].split(")")[0]
                        store_L.append([datetime_L,APP,pathLinfile])
            else:
                logsnotexist = [file_in_hcd[2],listoflogs[i]]
                listoflogsnotexist.append(logsnotexist)
        if len(store_L) != 0:
            last_record = len(store_L) -1
            end_time = store_L[last_record][0]
        else:
            return starting_time
                   
        if os.path.exists(path_ui) == True:
            for i in range(len(listofUI)):
                pathUIinfile = path_ui + listofUI[i]
                if os.path.exists(pathUIinfile) == True:
                    with codecs.open(pathUIinfile, 'r', encoding='utf8', errors='ignore') as f:
                        textUI = f.read().split('\n')
                    for j in range(len(textUI)):
                            if ('authenticationService.authenticate() start' in textUI[j]) or ('authenticationService.logoff() end' in textUI[j]):
                                
                                if (j+2 < len(textUI)):#########################
                                    if '  [com.xxx.xxx.xxx.appmgr.ui.LoginDialog$2]:Authentication Exception error Code ' in textUI[j+2]:
                                        if ('  [com.xxx.xxx.xxx.appmgr.ui.LoginDialog$2]:Authentication Exception error Code XXXXXXX' in textUI[j+2]):
                                            pass
                                        else:
                                            continue 
                                content_list = textUI[j].split(' ')
                                datetime_UI = (datetime.strptime((content_list[0]+' '+content_list[1]),'[%Y-%m-%d %H:%M:%S,%f'))
                                loginorout = content_list[5].split(':')[1].split("(")[0].split(".")[1]
                                store_UI.append([datetime_UI,loginorout,pathUIinfile])
                                if len(store_UI) == 1:
                                    starting_time = store_UI[0][0] #saving starting time
            
        if (starting_time != 0) and (starting_time < end_time):
            df_L = pd.DataFrame(store_L, columns =['Time', 'Function','Location_L'])
            df_L = df_L[df_L['Time'] >= starting_time]
            df_UI = pd.DataFrame(store_UI, columns =['Time', 'Function','Location_UI'])
            df_merged = pd.merge(df_L, df_UI, how='outer', on=['Time', 'Function'])
            df_merged.sort_values(by=['Time'], inplace=True)
            
            df_merged_2020 = df_merged[df_merged['Time'] >= starting_time]
            df_merged_2020 = df_merged_2020.reset_index(drop=True)
            end_record = df_merged_2020[df_merged_2020.eq(end_time).any(1)]
            if len(end_record)>1:
                s = len(end_record)
                end_record = end_record.iloc[s-1:s]
            df_merged_sorted = df_merged_2020[df_merged_2020['Time'].values <= end_record['Time'].values]
            
            for i,row in df_merged_sorted.iterrows():
                try:
                    if (i>0) and (i<len(df_merged_sorted)):
                        if (df_merged_sorted['Function'].iloc[i] == df_merged_sorted['Function'].iloc[i-1])and (df_merged_sorted['Function'].iloc[i] == 'authenticate'):
                            end = df_merged_sorted['Time'].iloc[i]
                            start = df_merged_sorted['Time'].iloc[i-1]
                            if (end-start).total_seconds() <= 60:
                                df_merged_sorted = df_merged_sorted.drop(df_merged_sorted.index[i-1])
                                df_merged_sorted = df_merged_sorted.reset_index(drop=True)
                except IndexError:
                    break
            
            return df_merged_sorted, listoflogsnotexist
        else:
            return 0


def find_logoffip(df):
    df_store = pd.DataFrame()
    list_of_ID = df['ID'].unique().tolist()
    for id in range(len(list_of_ID)):
        df2 = df[df['ID'] == list_of_ID[id]]
        df2 = df2.reset_index(drop=True)   
        for index ,row in df2.iterrows():
            try:
                if df2['Function'].iloc[index] == "logoff":
                    num = index
                    while (num >= 0):
                        if (df2['Function'].iloc[num] == "authenticate"):
                            time_diff = (df2['Time'].iloc[index] - df2['Time'].iloc[num]).total_seconds()
                            if time_diff < 43200:
                                df2['ip'].iloc[index] = df2['ip'].iloc[num]

                            break
                        num -= 1
            except IndexError:
                break
        df_store = df_store.append(df2)
    df_store.sort_values(by=['Time'], inplace=True)
    return df_store

def time_in_range(start, end, x):
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end



###############################################################################
mylines = []

listoflogs = ['CAS_PlatformService.log.10','CAS_PlatformService.log.9','CAS_PlatformService.log.8','CAS_PlatformService.log.7','CAS_PlatformService.log.6','CAS_PlatformService.log.5','CAS_PlatformService.log.4','CAS_PlatformService.log.3','CAS_PlatformService.log.2','CAS_PlatformService.log.1','CAS_PlatformService.log']
listofUI = ['AppMgr_UI.log.10','AppMgr_UI.log.9','AppMgr_UI.log.8','AppMgr_UI.log.7','AppMgr_UI.log.6','AppMgr_UI.log.5','AppMgr_UI.log.4','AppMgr_UI.log.3','AppMgr_UI.log.2','AppMgr_UI.log.1','AppMgr_UI.log']
listoflogsnotexist = []

APPlist = ['AdminConsole','CSS-RPT','CSS-SUP','CAS-NS','NSAdminConsole','CSS-UTIL-1','CSS-UTIL-2','VIS2','Hactl.com','HEx','iHRIS','CSS-SDS','BSS-SDS','CSS-EMS','BSS-EMS','VIS2Fallback','HactlPlus','BSS-SDS-FB','CSS-SDS-FB','EAMS','CSS-UTILITY','CP-Browser','CP-Browser-Fallback','CSS-Mobile','CP-Mobile','LP-BSS-RPT','LP-BSS-UTILITY','e-Learning','LP-CSS-Support-Util','LP-BSS-SUP','LP-CSS-RPT','LP-CSS-SUP','LP-CSS-UTILITY','FRWEB']
c = len(APPlist)
file_in_hcd =  [ name for name in os.listdir('/3/hcd') if os.path.isdir(os.path.join('/3/hcd', name)) ]
file_that_have_problem = []

df2 = pd.read_csv('task3source.csv', header = 0)
df2.columns = ['Time','ID','Function','serve','machine_no','ip','role']

df2 = df2.replace(to_replace ='AUTHENTICATE|EAF', value ='authenticate') 
df2 = df2.replace(to_replace ='LOGOFF|START', value ='logoff') 

df2['Time'] = df2['Time'].astype('datetime64[ns]') 
df2 = find_logoffip(df2)

df_store_logs = pd.DataFrame()


ans = []
listofIP=[]

start_df2 = df2["Time"].iloc[0]
end_df2 = df2["Time"].iloc[-1]

file_list_ok=[]

for f1 in range(len(file_in_hcd)):
    data = datatosort(file_in_hcd[f1],listoflogs,listofUI) ##data in logs and ui files
    if data == 0:
        file_that_have_problem.append(file_in_hcd[f1])
        pass
    else:
        data_merged_df = data[0]
        data_L_notexist = data[1]
        del data_merged_df['Location_L']
        del data_merged_df['Location_UI']
        data_merged_df.insert(2, "ip", file_in_hcd[f1])
        data_merged_df = data_merged_df[(data_merged_df['Time'] <= end_df2) & (data_merged_df['Time'] >= start_df2)]
        
        if data_merged_df.empty == True:
            print('empty')
            continue
        else:
            df_store_logs = df_store_logs.append(data_merged_df)
            file_list_ok.append(file_in_hcd[f1])


df2 = pd.concat([df2, df_store_logs]).sort_index()
df2.sort_values(by=['Time'], inplace=True)

df2.insert(7, 'merge', '0')





for file in file_list_ok:
    df_test_104 = df2[(df2['ip'] == file)]
    df_test_104 = df_test_104.reset_index(drop=True)
    print('file sorting: ')
    print(file)    
    for i ,row in df_test_104[1:-1].iterrows():
        try:
            if (df_test_104['Function'].iloc[i] == "authenticate") and  (df_test_104['Function'].iloc[i-1] == "authenticate") :
                time_diff = (df_test_104['Time'].iloc[i] - df_test_104['Time'].iloc[i-1]).total_seconds()
                if time_diff < 3:
                    if pd.isnull(df_test_104['ID'].iloc[i-1]):
                        df_test_104_1 = df_test_104.drop(df_test_104.index[i-1])
                        df_test_104 = df_test_104_1.reset_index(drop=True)
                    elif pd.isnull(df_test_104['ID'].iloc[i]):
                        df_test_104_1 = df_test_104.drop(df_test_104.index[i])
                        df_test_104 = df_test_104_1.reset_index(drop=True)    
                        
    
            if (df_test_104['Function'].iloc[i] == "logoff") and  (df_test_104['Function'].iloc[i-1] == "logoff") :
                time_diff = (df_test_104['Time'].iloc[i] - df_test_104['Time'].iloc[i-1]).total_seconds()
                if time_diff < 3:
                    if pd.isnull(df_test_104['ID'].iloc[i-1]):
                        df_test_104_1 = df_test_104.drop(df_test_104.index[i-1])
                        df_test_104 = df_test_104_1.reset_index(drop=True)
                    elif pd.isnull(df_test_104['ID'].iloc[i]):
                        df_test_104_1 = df_test_104.drop(df_test_104.index[i])
                        df_test_104 = df_test_104_1.reset_index(drop=True)                 
        except IndexError:
            break
        
    for i ,row in df_test_104[3:-1].iterrows():
        try:
            if (df_test_104['Function'].iloc[i] == "authenticate") and  (df_test_104['Function'].iloc[i-2] == "authenticate") :
                if (df_test_104['Function'].iloc[i-2] == "authenticate") and  (df_test_104['Function'].iloc[i-3] == "logoff") :
                    time_diff = (df_test_104['Time'].iloc[i] - df_test_104['Time'].iloc[i-1]).total_seconds()
                    if time_diff < 3:
                        time_s = df_test_104['Time'].iloc[i-2]
                        df_test_104.iloc[i-2] = df_test_104.iloc[i]
                        df_test_104['Time'].iloc[i-2] = time_s
                        df_test_104 = df_test_104.drop(df_test_104.index[i]).reset_index(drop=True)    
        except IndexError:
            break
      
    for i,row in df_test_104[::-1].iterrows():
        if (df_test_104['Function'].iloc[i] == 'logoff') and (df_test_104['merge'].iloc[i] == '0'):
            end = df_test_104['Time'].iloc[i]#************end
            df_test_104['merge'].iloc[i] == 1
            score = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            APP = []
            start = 'nth'
            for j,row in df_test_104[i::-1].iterrows():### find 'logoff'

                if (df_test_104['Function'].iloc[j] == 'authenticate') and (df_test_104['merge'].iloc[j] == '0'):
                    start = df_test_104['Time'].iloc[j] #************start
                    ID = df_test_104['ID'].iloc[j]
                    role = df_test_104['role'].iloc[j]
                    ip = df_test_104['ip'].iloc[j]
                    df_test_104['merge'].iloc[j] = 1
                    break
                elif (df_test_104['Function'].iloc[j] != 'logoff')and(df_test_104['merge'].iloc[j] == '0'):
                    df_test_104['merge'].iloc[j] = 1
                    for s in range(len(APPlist)):
                        if APPlist[s] == df_test_104['Function'].iloc[j]:
                            score[s] += 1
                            break
            if start == 'nth':
                continue
            list_app = [start,ID,role,ip]
            list_app.extend(score)
            ans.append(list_app)

df_atlast = pd.DataFrame(ans,columns=['Time',"ID",'role','ip','AdminConsole','CSS-RPT','CSS-SUP','CAS-NS','NSAdminConsole','CSS-UTIL-1','CSS-UTIL-2','VIS2','Hactl.com','HEx','iHRIS','CSS-SDS','BSS-SDS','CSS-EMS','BSS-EMS','VIS2Fallback','HactlPlus','BSS-SDS-FB','CSS-SDS-FB','EAMS','CSS-UTILITY','CP-Browser','CP-Browser-Fallback','CSS-Mobile','CP-Mobile','LP-BSS-RPT','LP-BSS-UTILITY','e-Learning','LP-CSS-Support-Util','LP-BSS-SUP','LP-CSS-RPT','LP-CSS-SUP','LP-CSS-UTILITY','FRWEB'])
asdf = df_atlast
df_atlast.sort_values(by=['role','Time'], inplace=True)
df_atlast.to_csv('task3&4_data2107.csv', index=False)          
###############################################################################
tac()
                
