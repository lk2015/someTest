#!/usr/bin/python

d1={}

file1=open("online_log_http_20160920_001.txt",'r',encoding='utf8')
#file1=open("test.txt",'r',encoding='utf8')
file2=open('result.txt','w',encoding='utf8')

read=file1.readline()

while (read):
    content = read.split('|')
    if content[6] != '其他业务':
        if content[0] not in d1:
            d1[content[0]] = [content[7]]
        else:
            d1[content[0]].append(content[7])
    read=file1.readline()

file1.close()

d2={}
#print(type(list))

for i in d1.values():
    new=set(i)
    for j in new:
        if j not in d2:
            d2[j] = 1
        else:
            d2[j] += 1

for i in d2.keys():
    new=i+' '+str(d2[i])+'\n'
    file2.write(new)

file2.close()
