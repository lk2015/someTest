#!user/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import math

spark = SparkSession.builder.appName("Recommend").getOrCreate()

df = spark.read.text("dictionary.txt")
####
sdf=spark.read.text("ndic")
####
def getItems(row):
    li = []
    for r in row:
        li.append(r.split(',')[3])
    return li

rd = df.rdd.map(lambda row: row[0].split('|')[1].split('#')[:-1])
rd.cache() #持久化
rd1=rd.map(getItems).map(set).reduce(lambda a, b: a | b)
#####
srd=sdf.rdd.map(lambda row: row[0].split('|')[1].split('#')[:-1])
#####
dic=dict(zip(rd1,len(rd1)*[0])) #空字典


def getTimes(row): #统计次数
    a = dic.copy()
    li = []
    for r in row:
        a[r.split(',')[3]] += 1
    return a

def t2s(row): #次数转化为评分
    for r in row:
        row[r]=200*math.atan(row[r]/10)/math.pi
    return row

rd2=rd.map(getTimes).map(t2s)
######
rd3=srd.map(getTimes).map(t2s)
######

user=df.rdd.map(lambda row:row[0].split('|')[0]).collect() #用户id
#user=sdf.rdd.map(lambda row:row[0].split('|')[0]).collect()

def relativeScore(row): #得到相对评分
    a = []
    for i in row:
        if row[i] == 0:
            continue
        for j in row:
            if i == j or row[j] == 0:
                continue
            else:
                a.append(((i, j), (row[i] - row[j], 1)))
                a.append(((j, i), (row[j] - row[i], 1)))
    return a


rScore = rd2.flatMap(relativeScore).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

rSdic={}       #相对评分汇总
for i in rScore.collect():
    rSdic[i[0]]=i[1]

def fi(row): #考虑更新情况
    rs=dic.copy()
    for i in row:
        a=0
        b=0
        if row[i]!=0:
            rs[i]=(row[i],0) # 0表示原有评分，1表示预测评分
            continue
        for j in rSdic:
            if j[0]==i and row[j[1]]!=0:
                a+=rSdic[j][0]+rSdic[j][1]*row[j[1]]
                b+=rSdic[j][1]
        rs[i]=(a/b,1)
    return rs

def recom(row):
    rec=[]
    for i in row:
        if row[i][0]>50 and row[i][1]==1:
            rec.append((i,row[i][0]))
    return rec

#####
#fillScores=rd2.map(fi).map(recom).map(lambda row:sorted(row,key=lambda x:x[1],reverse=True)).collect()
fillScores=rd3.map(fi).map(recom).map(lambda row:sorted(row,key=lambda x:x[1],reverse=True)).collect()
#####
f=open('result.txt','w')

for i in zip(user,fillScores):
    f.write(str(i)+'\n')

f.close()

spark.stop()
