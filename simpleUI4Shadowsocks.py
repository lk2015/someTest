import tkinter as tk
import os

if os.system("lsof -i|grep \"sslocal\"")==256:
    os.system("nohup sslocal -s 服务器 -p 端口号 -l 1080 -k \"密码\" -m \"加密方式\" &")
os.system("gsettings set org.gnome.system.proxy mode \"none\" ")

root=tk.Tk()
root.title('shadowsocks')
root.geometry("250x80")

F1=tk.Frame(root,width=200,height=100)
F2=tk.Frame(root,width=200,height=100)
F1.pack(side="top")
F2.pack(side="bottom")

hit=False
var=tk.StringVar()
var.set('disConnect')

def click():
    global hit
    if hit==False:
        hit=True
        os.system("gsettings set org.gnome.system.proxy mode \"manual\" ")
        var.set("Connect!!")
    else:
        hit=False
        os.system("gsettings set org.gnome.system.proxy mode \"none\" ")
        var.set("disConnect")




labal=tk.Label(F1,font=20,textvariable=var)
button=tk.Button(F2,font=20,text="Change",command=click)

labal.pack(expand=1)
button.pack(expand=1)


def kill():
    os.system("lsof -i|grep \"sslocal\" |head -1|awk \'{print $2}\'|xargs kill -9")
    os.system("gsettings set org.gnome.system.proxy mode \"none\" ")
    root.destroy()

root.protocol('WM_DELETE_WINDOW', kill) # root is your root window


root.mainloop()

