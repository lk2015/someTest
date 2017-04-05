import pickle

file1=open("resultNum.txt",'r',encoding='utf8')

l=[]

read=file1.readline()

while (read):
    content = read.split('|')
    l.append(content[0])
    read=file1.readline()

file1.close()



file1=open("online_log_http_20160920_001.txt",'r',encoding='utf8')
#file1=open("test.txt",'r',encoding='utf8')


d1={}

read=file1.readline()

while (read):
    content = read.split('|')
    if content[6] != '其他业务':
        if content[0] in l:
            if content[0] not in d1:
                d1[content[0]]=[[content[1],content[2],content[7]]]
            else:
                d1[content[0]].append([content[1],content[2],content[7]])
    read = file1.readline()

file1.close()

file=open("dictionary.txt","wb")
pickle.dump(d1,file)
file.close()