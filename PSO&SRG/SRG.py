from skimage import io
import numpy as np

class SRG:
    def __init__(self):
        self.img = io.imread('flower.jpg')
        self.row, self.column, self.dimension = self.img.shape
        self.stack = []
        self.points = [[-1, -1], [-1, 0], [-1, 1], [0, -1], [0, 1], [1, -1], [1, 0], [1, 1]]
        self.pic = [([0] * self.column) for i in range(self.row)]

    def search(self,r, c, t):
        for m in range(8):
            a = r + self.points[m][0]
            b = c + self.points[m][1]
            if 0 <= a < self.row and 0 <= b < self.column and self.pic[a][b] == 0:
                if np.linalg.norm(self.img[a][b] - self.img[r][c]) <= t:
                    self.pic[a][b] = 1
                    self.stack.append([a, b])

    def grow(self,place,t):
        sub=[]
        self.pic[place[0]][place[1]]=1
        sub.append(place)
        self.search(place[0],place[1],t)
        while self.stack!=[]:
            [a, b] = self.stack.pop()
            sub.append([a,b])
            self.search(a,b,t)
        return sub

    def other(self):
        sub=[]
        for i in range(self.row):
            for j in range(self.column):
                if self.pic[i][j]==0:
                    sub.append([i,j])
        return sub

