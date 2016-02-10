from datetime import datetime
import _mssql
import os
from time import sleep
def sorted_ls(path):
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    return list(sorted(os.listdir(path), key=mtime))
conn = _mssql.connect(server='balt-sql-fc', user='dpw_obc_intranet', password='MO864gEPS%?D', database='DPW_OBC_Prequal')
#f=open("11071430.HUL","r+")
#f=open("testread.txt","r+")
#
#    For changing test evironment check:
#       1.conn string
#       2.skip list
#       3.directory(file list)
#       4.insert table name
#
rlog=open("log.txt","r+")
starting_from=int(rlog.read()) #Continuous from this number
rlog.seek(0,0) # start from begining for overwriting the whole file
filelist=sorted_ls("HUL Files") ###Filelist over here###
end_from=len(filelist) #store new Continuous from number
rlog.write(str(end_from))
rlog.close()
print("File reading starting at: ", starting_from, " End at:", end_from)
if starting_from==end_from:
    print("There is no new files, quit script in 3 seconds...")
    sleep(3)
    quit()
skip=["FHD", "RTR", "RFF"] #RHD -read date
end=["FTR",""] #"RTR"
cmFlag=0
Totalline=0
TotalCus=0
#filelist=os.listdir("testfolder")
#filelist=os.listdir("history")
#for file in filelist:
for iterator in range(starting_from, end_from):
    lineCnt=0
    cusCnt=0
    f=open("HUL Files/"+filelist[iterator],"r+")  #need change to the directory name
    RDHTime=""        #RDHtime for MTM, for each block
    for lines in f:
        #accountNumber=""
        #meterNumber=""
        comment=""
        rdgRead=""
        readDate=""
        readTime=""
        readCode=""
        skipCode=""
        tCode1=""
        tCode2=""
        mReaderId=""
        readType=""
        preReading=""
        if lines[0:3] in skip:
            lineCnt+=1
            continue
        elif lines[0:3] in end:
            lineCnt+=1
            print("Filename: ",file," End of the file, import total:", cusCnt," Lines: ",lineCnt)
            Totalline+=lineCnt
            TotalCus+=cusCnt
            break
        if lines.startswith("RHD"):
            RDHTime=lines[103:107]+lines[99:103]
            lines = next(f)
            lineCnt+=1
        if lines.startswith("CUS"):
            accountNumber = lines[14:25]
            lines = next(f)
            lineCnt+=1
        if lines.startswith("MTR"):
            meterNumber = lines[45:53]
            if lines[88] == "Y":
                lines = next(f)
                cmFlag=1
                lineCnt+=1
                if lines.startswith("MTM"):
                    comment=lines[23:73]
                    lines = next(f)
                    lineCnt+=1
                else:
                    lines = next(f)
                    lineCnt+=1
                    if lines.startswith("MTM"):
                        comment=lines[23:73]
                        lines = next(f)
                        lineCnt+=1
                    else:
                        print("Error Reading MTM in line: ", lineCnt)
                        print("              CUS Number : ", accountNumber)
                        print("              FileName   : ", file)
            elif lines[87] == "Y":
                lines = next(f)
                cmFlag=2
                lineCnt+=1
                if lines.startswith("MTS"):
                    comment=lines[23:73]
                    lines = next(f)
                    lineCnt+=1
                else:
                    lines = next(f)
                    lineCnt+=1
                    if lines.startswith("MTM"):
                        comment=lines[23:123]
                        lines = next(f)
                        lineCnt+=1
                    else:
                        print("Error Reading MTM in line: ", lineCnt)
                        print("              CUS Number : ", accountNumber)
                        print("              FileName   : ", file)
            else:
                lines = next(f)
                lineCnt+=1

        if lines[0:3] in skip:
            lines=next(f)
            lineCnt+=1
        if lines.startswith("RDG"):
            rdgRead=lines[33:43]
            if lines[47:55]=="00000000":
                if RDHTime=="00000000":
                    readDate=""
                else:
                    readDate=RDHTime;
            else:
                readDate=lines[51:55]+lines[47:51]  #lines[47:55]
            readTime=lines[55:57]+":"+lines[57:59]+":"+lines[59:61]  #lines[55:61]
            readCode=lines[61]
            skipCode=lines[63:65]
            tCode1=lines[67:69]
            tCode2=lines[70:72]
            mReaderId=lines[75:84]
            readType=lines[103:105]
            preReading=lines[105:115]
        if cmFlag==1:
            # because have RDHTime readDate=""
            #readTime=""
            #readCode=""
            #mReaderId=""
            cmFlag=0
        elif cmFlag==2: #MTS
            cmFlag=0
        lineCnt+=1
        conn.execute_non_query(
        """INSERT INTO MVRS_New(
           [accountNumber]
          ,[meterNumber]
          ,[comment]
          ,[rdgRead]
          ,[readDate]
          ,[readTime]
          ,[readCode]
          ,[skipCode]
          ,[tCode1]
          ,[tCode2]
          ,[mReaderId]
          ,[readType]
          ,[preReading]
          ,[FileName])
          VALUES(%(a)s,%(m)s,%(c)s,%(r)s,%(rd)s,%(rt)s,%(rc)s,%(sc)s,%(tc1)s,%(tc2)s,%(mri)s,%(rty)s,%(pr)s,%(fn)s)""",\
          {"a":accountNumber,"m":meterNumber,"c":comment,"r":rdgRead,"rd":readDate,"rt":readTime,"rc":readCode,\
           "sc":skipCode,"tc1":tCode1,"tc2":tCode2,"mri":mReaderId,"rty":readType,"pr":preReading,"fn":file})
        cusCnt+=1
    #print("Acc:", accountNumber)
    #print("MNum: ", meterNumber)
    #print("comment: ", comment)
    #print("rdgRead: ", read)
    #print("readDate: ", readDate)
    #print("readTime: ", readTime)
    #print("readCode: ", readCode)
    #print("skipCode: ", skipCode)
    #print("tCode1: ", tCode1)
    #print("tCode2: ", tCode2)
    #print("mReaderId: ", mReaderId)
    #print("readType: ", readType)
    #print("preReading: ", preReading)
    #print("========")
    
#    if cusCnt>5:
#        break;

print("Total lines read:", Totalline)
print("Total Cus read  :", TotalCus)
        


