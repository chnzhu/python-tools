import os
import glob
import re
import string
#����Դ�ļ����õ�Ŀ¼�޸��������䣬��Ŀ¼�²����������޹��ļ�
os.chdir("f:\\temp\\t")
files=glob.glob("*")

#����ļ���������Ҫ�޸�
outFile=open("f:\\temp\\out.txt","w")

for fileName in files:
    inFile=open(fileName,'r')
    fileData=inFile.read()
    #ƥ�乫˾����
    nameMatch=re.findall(' \S*�ɷ����޹�˾',fileData)
    if len(nameMatch) == 0:
        nameMatch.append('δ�ҵ�')

    #ƥ���ԡ�Ԥ�ơ���ͷ����䣬����Զ��Ż��߾�Ž�β���������������ţ������        
    yjMatch=re.findall('Ԥ��[\s\S]*?[��|��]',fileData)
    if len(yjMatch) == 0:
        yjMatch.append('δ�ҵ�')
    yjMatch[0]=string.replace(yjMatch[0],'\n','')
    #ƥ�����ڣ��԰�����������ƥ�䣬�Ƿ���ں������ڣ�
    dateMatch=re.findall('[0-9]*��[0-9]*��[0-9]*��',fileData)
    if len(dateMatch) == 0:
        dateMatch.append('δ�ҵ�')
    inFile.close()
    outFile.write('%s %s   %s\n'%(nameMatch[0],yjMatch[0],dateMatch[0]))
outFile.close()
        