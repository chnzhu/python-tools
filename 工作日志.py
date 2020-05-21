#日期相关模块
from datetime import date,timedelta
import os
import thread,time
job_list=['17','1348','9251','021','2009-08-10','程序开发','2','程序开发','6小时','1','2','','2009-08-10','2009-08-10','四川省分行信息科技部',\
        '管理信息与办公自动化','朱诚','分行团队主管','recid']

begin_date=date(2009,12,22)
end_date=date(2010,5,31)
sqlfile=open(r"c:\temp\job_log.csv","w")
recid=925100192
sqlfile.write('DEPID,TEAMID,TLRID,HEADSHIP,WORKDAT,WORKDESC,ISTEMPORARY,WORKRESULT,SPENDTIME,WORKSTATUS,OCCURDEGREE,REMARK,INPDAT,UPDDAT,DEPNAM,TEAMNAM,USERNAM,HEADSHIPNAM,RECID')
while end_date >= begin_date:
    t1 = time.strptime(begin_date.isoformat(),"%Y-%m-%d")
    t2 = time.mktime(t1)
    weekdays=time.localtime(t2)[6]
    if weekdays == 5 or weekdays == 6:
        begin_date=begin_date+timedelta(days=1)
        continue
    job_list[4] = begin_date.isoformat()
    job_list[12] = job_list[4]
    job_list[13] = job_list[4]
    job_list[18] = str(recid)
    recid = recid + 1
    job_list[5] = '处理公文'
    job_list[7] = '处理公文'
    job_list[8] = '1小时'
    output_line='%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n'%(job_list[0],job_list[1],job_list[2],job_list[3],job_list[4],job_list[5],\
                                                                            job_list[6],job_list[7],job_list[8],job_list[9],job_list[10],job_list[11],\
                                                                            job_list[12],job_list[13],job_list[14],job_list[15],job_list[16],job_list[17],\
                                                                            job_list[18])
    
    sqlfile.write(output_line)
    if weekdays == 0:
        job_list[5] = '晨会，安排本周工作'
        job_list[7] = '晨会会议纪要'
        job_list[8] = '1小时'
        job_list[18] = str(recid)
        recid = recid + 1
        output_line='%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n'%(job_list[0],job_list[1],job_list[2],job_list[3],job_list[4],job_list[5],\
                                                                            job_list[6],job_list[7],job_list[8],job_list[9],job_list[10],job_list[11],\
                                                                            job_list[12],job_list[13],job_list[14],job_list[15],job_list[16],job_list[17],\
                                                                            job_list[18])
    
        sqlfile.write(output_line)
        job_list[8] = '6小时'
    else:
        job_list[8] = '7小时'
    job_list[5] = 'IT蓝图工作'
    job_list[7] = 'IT蓝图工作'
    job_list[18] = str(recid)
    recid = recid + 1
    output_line='%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n'%(job_list[0],job_list[1],job_list[2],job_list[3],job_list[4],job_list[5],\
                                                                            job_list[6],job_list[7],job_list[8],job_list[9],job_list[10],job_list[11],\
                                                                            job_list[12],job_list[13],job_list[14],job_list[15],job_list[16],job_list[17],\
                                                                            job_list[18])
    
    sqlfile.write(output_line)
    begin_date=begin_date+timedelta(days=1)
sqlfile.close()