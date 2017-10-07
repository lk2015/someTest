#!user/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import numpy as np
from collections import Counter

spark = SparkSession.builder.appName("Place_predict").getOrCreate()

#df = spark.read.text("online_log_http_20160920_001_small.txt")
df = spark.read.text("1.txt")
placeList = spark.read.text("place.txt").rdd.map(lambda row:(row[0].split(',')[0],row[0].split(',')[1:3]))

def getData(row):
    x=row[0].split('|')
    user=x[0]
    time=int(x[1][6:])
    placeID=x[4]
    kind=x[6]
    app=x[7]
    return (placeID,[(time,user,kind,app)])

def reArrange(row):
    user=row[1][0][0][1]
    time=row[1][0][0][0]
    app=row[1][0][0][3]
    placeX=float(row[1][1][0])
    placeY=float(row[1][1][1])
    return (user,[(time,app,(placeX,placeY))])

#第二个过滤器需要改成大的数
rdd=df.rdd\
    .map(getData)\
    .filter(lambda row:row[1][0][2]!='其他业务' and row[1][0][3]!='未知')\
    .join(placeList)\
    .map(reArrange)\
    .reduceByKey(lambda x,y:x+y)\
    .filter(lambda row:len(row[1])>0)\
    .map(lambda row:(row[0],sorted(row[1],key=lambda item:item[0]))).cache()
#('eab8248cc6c83f8c9a491619cd099d61', [(14121056, '微信', (125.416944, 43.993611)), (17205122, '腾讯新闻', (125.416944, 43.993611))])
#（已按时间进行排序）




#这一步需要对每个人的地点信息进行聚类（kmeans算法）
convergeDist = float(0.0000000001)

def kmeans(row):
    k=7
    tempDist = 1.0
    points=np.array([x[2] for x in row[1]])
    #centers=[points[i] for i in np.random.choice(len(points),k,replace=False)] 此方法可能出现重复点
    x=np.array(list(set([tuple(t) for t in points])))   #放进set去重
    centers=[x[i] for i in np.random.choice(len(x),k,replace=False)]
    while tempDist > convergeDist:
        bestCenter={i:[] for i in range(len(centers))}
        for point in points:
            bestIndex = -1
            closest = float("+inf")
            for i in range(len(centers)):
                temp = np.sum((point - centers[i]) ** 2)
                #print(point,'to',centers[i],' is ',temp)
                if temp < closest:
                    closest = temp
                    bestIndex = i
            bestCenter[bestIndex].append(point)
        #return [len(bestCenter[i]) for i in bestCenter]
        newCenters=[]
        for i in range(len(centers)):
            newCenters.append(np.average(bestCenter[i],axis=0))
        newCenters=np.array(newCenters)
        tempDist = np.sum((newCenters-centers)**2)
        centers=newCenters
        #return (newPoints)
        #print(tempDist)
    return (row[0],centers)

classify=rdd.map(kmeans)

def changePlaceToOrder(row):
    a=[]
    #print(row)
    center=row[1][1]
    #print(type(place))
    #return place
    for record in row[1][0]:
        app=record[1]
        place=np.array(record[2])
        centerIndex=0
        closest=float('+inf')
        for i in range(len(center)):
            temp=np.sum((place-center[i])**2)
            if temp<closest:
                closest=temp
                centerIndex=i
        a.append((centerIndex,app))
    return (row[0],a,center)

def computerProbability(row):
    placeCount = {i:[] for i in range(len(row[2]))}   #记录第i中心点转向的中心点序号
    appCount={i:[] for i in range(len(row[2]))}       #记录第i中心点使用的app
    #print(row)
    #return row[1][0][1]
    appCount[row[1][0][0]].append(row[1][0][1])
    for i in range(1,len(row[1])):
        if row[1][i][1]!=row[1][i-1][1]:
            appCount[row[1][i][0]].append(row[1][i][1])
        if row[1][i][0]!=row[1][i-1][0]:
            placeCount[row[1][i-1][0]].append(row[1][i][0])
    placePro={}
    appPro={}
    for k in placeCount:
        placePro[k]=Counter(placeCount[k]).most_common(1)[0][0]
    for k in appCount:
        appPro[k]=Counter(appCount[k]).most_common(1)[0][0]
    lastplace=row[1][-1][0]
    predictplace=placePro[lastplace]
    predictapp=appPro[lastplace]
    return (row[0],(float(row[2][predictplace][0]),float(row[2][predictplace][1])),predictapp)


newRDD=rdd.join(classify)\
    .map(changePlaceToOrder)\
    .map(computerProbability)

with open('TempResult.txt','w',encoding='utf8') as f:
    for i in newRDD.collect():
        f.write(str(i)+'\n')

spark.stop()