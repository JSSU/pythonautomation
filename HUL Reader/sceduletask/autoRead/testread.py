import os

def sorted_ls(path):
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    return list(sorted(os.listdir(path), key=mtime))

l=sorted_ls('../HUL Files') #./
print(l)
print("length of the list is: ", len(l))
w=open("log.txt","r+") 
#w.write(', '.join(l))
rinfo=w.read()
print(rinfo)
#w.write(str(len(l)+100)) #store iterator
w.seek(0,0)
w.write(str(int(rinfo)+1))
w.close()

#r=open("log.txt","r")
#rinfo=r.read()
#print("after + 1:",int(rinfo)+1)
#r.close()

# rl=open("", "r+")
# for f in rl:
#    info=f
# li=f.split(",")
#
