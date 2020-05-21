import os
import glob
import re
import string
#根据源文件放置的目录修改下面的语句，该目录下不能有其他无关文件
os.chdir("f:\\temp\\t")
files=glob.glob("*")

#结果文件，根据需要修改
outFile=open("f:\\temp\\out.txt","w")

for fileName in files:
    inFile=open(fileName,'r')
    fileData=inFile.read()
    #匹配公司名称
    nameMatch=re.findall(' \S*股份有限公司',fileData)
    if len(nameMatch) == 0:
        nameMatch.append('未找到')

    #匹配以“预计”开头的语句，语句以逗号或者句号结尾，如有其他标点符号，请添加        
    yjMatch=re.findall('预计[\s\S]*?[，|。]',fileData)
    if len(yjMatch) == 0:
        yjMatch.append('未找到')
    yjMatch[0]=string.replace(yjMatch[0],'\n','')
    #匹配日期，以阿拉伯数字来匹配，是否存在汉字日期？
    dateMatch=re.findall('[0-9]*年[0-9]*月[0-9]*日',fileData)
    if len(dateMatch) == 0:
        dateMatch.append('未找到')
    inFile.close()
    outFile.write('%s %s   %s\n'%(nameMatch[0],yjMatch[0],dateMatch[0]))
outFile.close()
        