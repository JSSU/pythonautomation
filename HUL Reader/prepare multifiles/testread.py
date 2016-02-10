from datetime import datetime
oldformat = '20140716'
datetimeobject = datetime.strptime(oldformat,'%Y%m%d')
newformat2 = datetimeobject.strftime('%m/%d/%Y')
print newformat2


##print "hello the world"
##f=open("11071430.HUL","r+")
##counter=0
##for lines in f:
##    print lines[0:3]
##    if counter>3:
##        break
##    counter+=1


