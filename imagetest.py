from skimage import io,data,color,draw
import numpy as np
import matplotlib.pyplot as plt
import numpy as np
#img=io.imread('sky.jpg')
img=data.coffee()

plt.subplot(1,2,1)
plt.title('origin')
plt.imshow(img)
plt.axis('off')



row,column,dimension=img.shape
#print row,column
pic = [([0] * column) for i in range(row)]
stack=[]
k=1
points=[[-1,-1],[-1,0],[-1,1],[0,-1],[0,1],[1,-1],[1,0],[1,1]]

def search(r,c):
    for m in range(8):
        a=r+points[m][0]
        b=c+points[m][1]
        if 0<=a<row and 0<=b<column and pic[a][b]== 0 :
            if np.linalg.norm(img[a][b]-img[r][c])<=50:
                pic[a][b]=k
                stack.append([a,b])
    return

for i in range(row):
    for j in range(column):
        if pic[i][j]==0:
            pic[i][j]=k
            search(i,j)
            while stack!=[]:
                [a,b]=stack.pop()
                search(a,b)
            k=k+1

print k-1


for i in range(row):
    for j in range(column):
        img[i][j]=[pic[i][j]*11%256,pic[i][j]*13%256,pic[i][j]*17%256]
        #print pic[i][j]

plt.subplot(1,2,2)
plt.title('then')
plt.imshow(img)
plt.axis('off')

plt.show()

