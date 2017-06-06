import pandas as pd
import numpy as np
from copy import deepcopy
from itertools import chain

import pandas as pd 
import numpy as np
import pymysql


#cluster1 = postSet2[i][1] == [ [xxx],[xxx]...]
#cluster2 = postSet2[j][1] == [ [xxx],[xxx]...]
def clusterij(cluster1,cluster2):         
    tmp = list()
    for i in cluster1:
        for j in cluster2: 
            union = list( set(i) | set(j) )
            intersec = list( set(i) & set(j) )
            similarity = len(intersec)/len(union)    #number
            tmp.append(similarity)                  # 裝在一個list裡  方便等下 sum 
    Len = len(cluster1)*len(cluster2)
    similarity = sum(tmp)/Len
    return similarity
    

conn= pymysql.connect(
        host='databda.ddns.net',
        port = 3306,
        user='root',
        passwd='iiibda',
        db ='fb_all',
        charset='utf8',
        )

df1 = pd.read_sql("SELECT fpId,postId,createdTime FROM editorpost", con=conn)
df2 = pd.read_sql("SELECT postId,userId FROM `like`", con=conn)

like = pd.merge(df1, df2, on=['postId'])

conn.close()
liket = like[(pd.to_datetime(like['createdTime'],errors='coerce') > pd.to_datetime ('2016-01-01',errors='coerce'))]

sample = liket[liket["fpId"] == '193982817376415']   #選擇fpid==bnq

postId = list(set(sample.iloc[:,1]))               #postId list

df_post = dict()
count = 1
for i in postId :
    df_post[i] = [[str(count)], [list(set(sample[sample["postId"] == str(i)].iloc[:,3]))],[str(count)] ]
    count += 1

postSet = df_post


#postSet = {101:[[1],[ ['1','2','3'] ]],102:[[2],[ ['4','5'] ]],103:[[3],[ ['4','5'] ]],104:[[4],[ ['2','3','4'] ]]}


combine=dict()                      #for newcluster
countNum= len(postSet)+1            # if n=5 +1 從6 開始新的count
countrun = 1                        # for countrun
table = []                          #for table

k = list(postSet.keys())            
limitLen = len(k)
k2 = k[:]  #copy k (namelist)
postSet2 = deepcopy(postSet)

while True:
    Max = 0 
    for i in k2:   
        for j in k2:
            if postSet2[i][0] != postSet2[j][0]:   # 用 標籤看是否相同
                similarity =  clusterij(postSet2[i][1],pos  tSet2[j][1])
                if Max < similarity:
                    Max = similarity 
                    geti = i                       # get    最大值 時 的兩個post名稱
                    getj = j
            else:                                 # 如果相同就直接pass不用看
                pass
            
    
    
    
    #print出運行結果 ＆  紀錄 table
    print(countrun,Max,geti,getj,postSet2[geti][2],postSet2[getj][2])
    tablelist = [countrun,Max,geti,getj,postSet2[geti][2],postSet2[getj][2]]
    table.append(tablelist)
    
    
    #合併與刪除
    k2.append(countNum)    # k2 == k = list(postSet.keys()) 
    k2.remove(geti)
    k2.remove(getj)

    valuetemp = [postSet2[geti][1],postSet2[getj][1]] 
    idtemp = [postSet2[geti][2],postSet2[getj][2]] 
    postSet2[countNum] = [ [countNum],list(chain(*(valuetemp))) ,list(chain(*(idtemp)))]
    del postSet2[geti]
    del postSet2[getj]

    #計數加一
    countNum += 1
    countrun += 1

    #當100群時 則結束
    if len(postSet2) == 100:
        break 
