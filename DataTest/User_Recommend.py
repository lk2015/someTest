#!user/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import math

spark = SparkSession.builder.appName("User_Recommend").getOrCreate()

df = spark.read.text("online_log_http_20160920_001.txt")



#用户使用条目汇总
rd=df.rdd\
    .map(lambda row:row[0].split('|'))\
    .filter(lambda row:row[6]!='其他业务' and row[7]!='未知')\
    .map(lambda row:(row[0],[row[7]]))\
    .reduceByKey(lambda x,y:x+y)\
    .cache()


#得到所有app的名目
AllApp=list(rd.map(lambda row:set(row[1])).reduce(lambda x,y:x|y))



#       将app的list转化为value是使用次数的字典
def list2dict(row):
    dic=dict(zip(AllApp,len(AllApp)*[0]))
    for i in row[1]:
        dic[i]+=1
    return (row[0],dic)

#按照合法条目数量进行过滤得到条目大于1000的用户的app使用次数
UserApp=rd.filter(lambda row:len(row[1])>=1000).cache()
UserAppDic=UserApp.map(list2dict).cache()

#按照次数计算个人评分
def times2score(row):
    for k in row.keys():
        row[k]=200*math.atan((row[k])/10)/math.pi
    return row

UserScore=UserAppDic.map(times2score).cache()

#计算app相对评分
def relativeScore(row):
    a=[]
    for i in row.keys():
        for j in row.keys():
            if i==j :
                continue
            a.append(((i,j),(row[i]-row[j],1)))
    return a

def anotherList2Dic(row):
    a={}
    for i in row[1]:
        if i in a:
            a[i]+=1
        else:
            a[i]=1
    return a

relative=UserApp.map(anotherList2Dic)\
    .map(times2score)\
    .flatMap(relativeScore)\
    .reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))


rSdic={}                      #相对分数的字典
for i in relative.collect():
    rSdic[i[0]]=i[1]
#print(relScoreDic)

#回到每个用户，为每个用户补全评分
def new_times2scores(row):
    for i in row[1].keys():
        row[1][i]=200*math.atan((row[1][i])/10)/math.pi
    return row

newdict=dict(zip(AllApp,len(AllApp)*[0])) #存放空的评分，用来更新

def fillScore(row):    #填满评分
    rs=newdict.copy()
    for i in row[1].keys():
        a=0
        b=0
        if row[1][i]!=0:
            continue
        for j in rSdic:
            if j[0]==i and row[1][j[1]]!=0:
                a+=rSdic[j][0]+rSdic[j][1]*row[1][j[1]]
                b+=rSdic[j][1]
        if b==0:
            rs[i]=0.0
        else:
            rs[i]=a/b
    return (row[0],rs)

#def getR(row):    #得到结果
def filterSmall(row):
    a=[]
    for i in row[1]:
        if row[1][i]>50:                #大于50分才推荐
            a.append((i,row[1][i]))
    return (row[0],sorted(a,key=lambda x:x[1],reverse=True))




predict=UserAppDic.map(new_times2scores).map(fillScore).map(filterSmall).filter(lambda row:len(row[1])>=3)

#写入文件
f=open('result.txt','w',encoding='utf8')
for i in predict.collect():
    f.write(str(i)+'\n')
f.close()

#print(predict.collect())
print(predict.count())

spark.stop()