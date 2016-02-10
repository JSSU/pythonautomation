from datetime import datetime

#f=open("11071430.HUL","r+")
f=open("testread.txt","r+")
skip=["FHD", "RHD", "RTR"]
end=["FTR",""] #"RTR"
lineCnt=0
cusCnt=0
for lines in f:
    accountNumber=""
    meterNumber=""
    comment=""
    read=""
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
        print("End of the file, import total:", cusCnt)
        lineCnt+=1
        break
    
    if lines.startswith("CUS"):
        accountNumber = lines[14:25]
        lines = next(f)
        lineCnt+=1
        cusCnt+=1
    if lines.startswith("MTR"):
        meterNumber = lines[45:53]
        if lines[88] == "Y":
            lines = next(f)
            lineCnt+=1
            if lines.startswith("MTM"):
                comment=lines[23:73]
                lines = next(f)
                lineCnt+=1
            else:
                print("Error Reading MTM in line: ", lineCnt)
                print("               CUS Number: ", accountNumber)
        else:
            lines = next(f)
            lineCnt+=1
    if lines.startswith("RDG"):
        read=lines[33:43]
        readDate=lines[51:55]+lines[47:51]  #lines[47:55]
        readTime=lines[55:57]+":"+lines[57:59]+":"+lines[59:61]  #lines[55:61]
        readCode=lines[61]
        skipCode=lines[63:65]
        tCode1=lines[67:69]
        tCode2=lines[70:72]
        mReaderId=lines[75:84]
        readType=lines[103:105]
        preReading=lines[105:115]
    lineCnt+=1
    print("Acc:", accountNumber)
    print("MNum: ", meterNumber)
    print("comment: ", comment)
    print("read: ", read)
    print("readDate: ", readDate)
    print("readTime: ", readTime)
    print("readCode: ", readCode)
    print("skipCode: ", skipCode)
    print("tCode1: ", tCode1)
    print("tCode2: ", tCode2)
    print("mReaderId: ", mReaderId)
    print("readType: ", readType)
    print("preReading: ", preReading)
    print("========")
    
#    if cusCnt>5:
#        break;
print("Total lines read:", lineCnt)
print("Total Cus read  :", cusCnt)
        
