#!/usr/bin/python

d1={}

file1=open("online_log_http_20160920_001.txt",'r',encoding='utf8')
#file1=open("test.txt",'r',encoding='utf8')
file2=open('resultNum.txt','w',encoding='utf8')

read=file1.readline()

while (read):
    content = read.split('|')
    if content[6] != '其他业务':
        if content[0] not in d1:
            d1[content[0]] = 1
        else:
            d1[content[0]] += 1

    read=file1.readline()


for i in d1.keys():
    if d1[i]>=1000:
        file2.write(i+'|'+str(d1[i])+'\n')

file1.close()