from datetime import datetime
import _mssql
import os
conn = _mssql.connect(server='balt-sql-fc', user='dpw_obc_intranet', password='MO864gEPS%?D', database='DPW_OBC_Prequal')
#f=open("11071430.HUL","r+")
#f=open("testread.txt","r+")
skip=["FHD", "RHD", "RTR","MTS"]
end=["FTR",""] #"RTR"
cmFlag=0
Totalline=0
TotalCus=0
filelist=os.listdir("testfolder")
for file in filelist:
    lineCnt=0
    cusCnt=0
    f=open("testfolder/"+file,"r+")
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
            else:
                lines = next(f)
                lineCnt+=1
        if lines[0:3] in skip:
            lines=next(f)
            lineCnt+=1
        if lines.startswith("RDG"):
            rdgRead=lines[33:43]
            if lines[47:55]=="00000000":
                readDate=""
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
            readDate=""
            readTime=""
            readCode=""
            mReaderId=""
            cmFlag=0
        lineCnt+=1
        conn.execute_non_query(
        """INSERT INTO MVRS(
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
          ,[preReading])
          VALUES(%(a)s,%(m)s,%(c)s,%(r)s,%(rd)s,%(rt)s,%(rc)s,%(sc)s,%(tc1)s,%(tc2)s,%(mri)s,%(rty)s,%(pr)s)""",\
          {"a":accountNumber,"m":meterNumber,"c":comment,"r":rdgRead,"rd":readDate,"rt":readTime,"rc":readCode,"sc":skipCode,"tc1":tCode1,"tc2":tCode2,"mri":mReaderId,"rty":readType,"pr":preReading})
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
        
