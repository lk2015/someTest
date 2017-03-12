import random
from skimage import io
import numpy as np
import SRG

img=io.imread('flower.jpg')
n=10
x=[0,0]*10
v=[0,0]*10
c1=2
c2=2
pbest=[10000]*10
gbest=10000
vmax=5
tbest=[0,0]

def std(pic):
    sub=[]
    for i in pic:
        sub.append(img[i[0]][i[1]])
    return np.std(sub,axis=0)

def fitness(a):
    global gbest
    for i in range(n):
        srg=SRG.SRG()
        sub1=srg.grow([0,0],a[i][0])
        sub2=srg.grow([200,300],a[i][1])
        sub3=srg.other()
        stdnum=std(sub1)+std(sub2)+std(sub3)
        if stdnum[0]+stdnum[1]+stdnum[2]<pbest[i]:
            pbest[i] = stdnum[0]+stdnum[1]+stdnum[2]
        if pbest[i]<gbest:
            gbest=pbest[i]
            tbest[0]=a[i][0]
            tbest[1] = a[i][1]


def init():
    for i in range(n):
        x[i]=[random.uniform(0,422),random.uniform(0,422)]
        v[i]=[random.uniform(0,0.1),random.uniform(0,0.1)]
    fitness(x)

init()
maxtimes=50
w_Threshold=0.8
for i in range(maxtimes):
    for j in range(n):
        for k in range(2):
            v[j][k] = w_Threshold * v[j][k] + c1 * random.uniform(0, 1) * (pbest[j] - x[j][k]) + c2 * random.uniform(0,1) * (gbest - x[j][k])
            if v[j][k] > vmax:
                v[j][k] = vmax
            x[j][k] += v[j][k]
            if x[j][k]>422:
                x[j][k]=422
            if x[j][k]<0:
                x[j][k]=0
    fitness(x)

print tbest
