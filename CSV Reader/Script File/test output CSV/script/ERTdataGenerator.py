import sys
import os
import datetime #for NameConvention
import uuid #for NameConvention
from time import sleep
#import random
#	outputname=random.randint(0,2222)
# test read file :
# "inputfolder/327137-001_BALTIMORE CITY OF_Water Meter_1512_11252014.csv"
def getdig(words):
	i=0
	for word in words:
		if word.isdigit():
			i+=1
		else:
			break;
	return i
def getNameConvention():
	dt = datetime.datetime.today()
	daTi = dt.strftime("%Y%m%d_%H%M%S_")
	GUID = uuid.uuid4()
   #cName= ("F-UMX-ESR-01-{}{}.csv").format(daTi,GUID)
	cName= ("F-INV-UMX-01-{}{}.csv").format(daTi,GUID)
	return cName
def main(fileName):
	#r=open('../inputfolder/'+fileName,'r')
	r=open('../inputfolder/'+fileName,'r')
	w=open('../outputfolder/'+ getNameConvention(),'w+')
	#w=open('../outputfolder/'+'output '+fileName,'w+')
	w.truncate()
	r.readline() #Get ride of the header line
	w.write("Asset ID;Installation ID;Installation object;Installation object type;External ID type 1;External ID value 1;External ID type 2;External ID value 2\n")
	for line in r:
		before=line  
		sbefore=line.split(",")
		###for ERT reading. No Description
		#installtype=sbefore[11]   #"MTR-LL 5/8SRII ER-ITRON .01CF* BTM/CI  8WHL1A  5'ITRON ERT  * SN=REGID  R/DP BALTIMORE"
		#sinstalltype=installtype.split() #['MTR-LL', '5/8SRII', 'ER-ITRON', '.01CF*', 'BTM/CI', '8WHL1A', "5'ITRON", 'ERT', '*', 'SN=REGID', 'R/DP', 'BALTIMORE']
		#firstdes=sinstalltype[1] #'5/8SRII'
		#seconddes=sinstalltype[2] #'ER-ITRON'
		#sfirstdes=firstdes.split("/") #['5', '8SRII']
		#ldig=getdig(sfirstdes[1])
		#dig=sfirstdes[0]+sfirstdes[1][0:ldig]
		#dis=sfirstdes[1][ldig:]
		#description=dig+" "+dis+" "+seconddes.split("-")[0]
		###for ERT reading. No Description End.
		#after=";;ERT;"+description+";Serial ID;"+sbefore[5]+";;\n"
		#after=";;ERT;"+sbefore[2]+";Serial ID;"+sbefore[5]+";;\n"
		after=";;ERT;"+"100W"+";ERT ID;"+sbefore[5]+";;\n"
		w.write(after)
	r.close()
	w.close()

if __name__ == '__main__':
	print("You enter:"+str(sys.argv))
	print("sys.argv[1] is:"+str(sys.argv[1]))
	if len(sys.argv)!=2:	
		print("Not correct Auguments, quit in 3secs")
		sleep(3)
		quit()
	main(sys.argv[1])
