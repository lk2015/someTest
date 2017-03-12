from skimage import io
import SRG
import matplotlib.pyplot as plt

img=io.imread('flower.jpg')

plt.subplot(1,2,1)
plt.title('origin')
plt.imshow(img)
plt.axis('off')

srg=SRG.SRG()
#349.657404982

sub1=srg.grow([200,300],349.657404982)
sub2=srg.other()

for i in sub2:
    img[i[0]][i[1]]=[255,255,255]

plt.subplot(1,2,2)
plt.title('then')
plt.imshow(img)
plt.axis('off')

plt.show()