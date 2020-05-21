#win32程序扩展模块
import win32serviceutil,win32service,win32event,servicemanager,win32com.client
import win32con,win32file,pywintypes
import cx_Oracle
#日期相关模块
from datetime import date,timedelta
import os
import thread,time
import ftplib,types
import tarfile
import gzip
#解析参数模块
import ConfigParser,string
import zipfile
import tempfile
from socket import *
#webservice模块
from ZSI.client import NamedParamBinding as NPBinding
from ZSI.client import AUTH
import sys

sys.path.append(r'D:\Projects\boc\pgm\etlnew')
from SasConfig import SasConfig,Sms

#定义异常
class MyException(Exception):
    def __init__(self,msg):
        self.args=msg
     
class RunPython:
    #判断GLDM表是否倒入完成
    def checkGLDM(self,procDate):
        try:
            str_date=string.replace(procDate.isoformat(),'-','')
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            curs=conn.cursor()
            runsql="select count(*) from dpc_load_data where tabnam in ('BANCS_GLDM','BANCS_GECTU00') and prcdat='%s' and prcsts='2'"%str_date
            while 1:
                curs.execute (runsql) 
                row=curs.fetchone()
                cnt=int(row[0])
                if cnt >= 4 :
                    break
                time.sleep(30)
            conn.close()
        except Exception,msg:
            return False,'运行loadGLDM错误，错误信息：%s'%str(msg)
        else:
            return True,'GLDM倒入完成'
            #判断GLDM表是否倒入完成
    #sgjj_hd:判断ITAS和INVM表是否倒入完成    
    def check_HD(self,procDate):
        try:
            str_date=string.replace(procDate.isoformat(),'-','')
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            curs=conn.cursor()
            runsql="select count(*) from dpc_load_data where tabnam in ('ITAS_TXN_TIF_BAS','BANCS_INVM') and prcdat='%s' and prcsts='2'"%str_date
            while 1:
                curs.execute (runsql) 
                row=curs.fetchone()
                cnt=int(row[0])
                if cnt >= 7 :
                    break
                time.sleep(30)
            conn.close()
        except Exception,msg:
            return False,'运行loadITAS_TXN_TIF_BAS错误，错误信息：%s'%str(msg)
        else:
            return True,'ITAS_TXN_TIF_BAS倒入完成'

    #产生公务卡的数据
    def procCorpTran_v3(self,procDate):
        try:
            str_date=string.replace(procDate.isoformat(),'-','')
            date_plus2=procDate+timedelta(days=1)
            
            conn=cx_Oracle.connect(SasConfig.conn_ereport)
            curs=conn.cursor()
            

            sqlCommand="select pan_txt, txn_cde,txn_dte, txn_tme,txn_des,txn_amt, txn_cry,ptg_txn_amt,corp_nme from corp_tran where rptdat='%s'"%str_date
            print sqlCommand
            curs.execute(sqlCommand)
            corp_name="corp_tran_new%s"%(string.replace(date_plus2.isoformat(),'-',''))
            extr_file=open("d:\\temp\\corp\\%s"%corp_name,'wb')
            
            for row in curs.fetchall():
                writeLine=row[0]+' '+row[1]+' '+row[2]+' '+row[3]+' '+row[4]+' '+str(row[5])+' '+row[6]+' '+str(row[7])+" "+str(row[8])+'\n'
                extr_file.write(writeLine)
            extr_file.close()

            ftp_crm = ftplib.FTP()
            ftp_crm.connect("21.96.5.54",21)
            ftp_crm.login("cardftp","QWER5678b")
            ftp_crm.set_pasv(False)
            ftp_crm.cwd("gwk")
            #ftp_crm.cwd("gwk")
            extr_file=open("d:\\temp\\corp\\%s"%corp_name,'rb')
            ftp_crm.storbinary ("STOR %s"%corp_name,extr_file)
            extr_file.close()
            ftp_crm.close()
            
            
            
            sqlCommand="select pan_txt,csr_nme,lvl_1_corp_nme,scl_scy_typ_des,scl_scy_nbr_txt,status,newflg from " +\
                        " corp_cus where rptdat='%s'"%str_date
            curs.execute(sqlCommand)
            corp_name="corp_cus%s"%(string.replace(date_plus2.isoformat(),'-',''))
            extr_file=open("d:\\temp\\corp\\%s"%corp_name,'wb')
            for row in curs.fetchall():
                writeLine=row[0]+' '+row[1]+' '+row[2]+' '+row[3]+' '+row[4]+' '+str(row[5])+' '+row[6] +'\n'
                extr_file.write(writeLine)
            conn.close()
            extr_file.close()
            
            ftp_crm = ftplib.FTP()
            ftp_crm.connect("21.96.5.54",21)
            ftp_crm.login("cardftp","QWER5678b")
            ftp_crm.set_pasv(False)
            ftp_crm.cwd("gwk")
            extr_file=open("d:\\temp\\corp\\%s"%corp_name,'rb')
            ftp_crm.storbinary ("STOR %s"%corp_name,extr_file)
            extr_file.close()
            ftp_crm.close()
            return True,'公务卡数据产生成功'
        except Exception,msg:
            errmsg=str(msg)
            if errmsg.find('descriptor') != -1 :
                return None,'公务卡数据产生失败：'+str(msg)
            else:
                return False,'公务卡数据产生失败：'+str(msg)

    def procCorpText(self,procDate):
        try:
            str_date=string.replace(procDate.isoformat(),'-','')
            conn=cx_Oracle.connect(SasConfig.conn_ereport)
            curs=conn.cursor()
            sqlCommand = "select crpcde,crpnam,actnam,actno,opnorg,czcde from corp_list";
            curs.execute(sqlCommand)
            extr_file=open("d:\\temp\\corp\\cpy.txt",'wb')
            for row in curs.fetchall():
                writeLine=row[0]+' '+row[1]+' '+row[2]+' '+row[3]+' '+row[4]+' '+row[5] +'\n'
                extr_file.write(writeLine)
            extr_file.close()

            sqlCommand = "select distinct czcde from corp_list";
            curs.execute(sqlCommand)
            extr_file=open("d:\\temp\\corp\\num.txt",'wb')
            for row in curs.fetchall():
                writeLine=row[0] +'\n'
                extr_file.write(writeLine)
            extr_file.close()            
        
            sqlCommand = "select crpcde,crpnam,actnam,actno,opnorg,czcde from corp_list_his where upddat='%s' and crpsts in ('1')"%procDate.isoformat();
            curs.execute(sqlCommand)
            extr_file=open("d:\\temp\\corp\\cpyadd.txt.%s"%procDate.isoformat(),'wb')
            for row in curs.fetchall():
                writeLine=row[0]+' '+row[1]+' '+row[2]+' '+row[3]+' '+row[4]+' '+row[5] +'\n'
                extr_file.write(writeLine)
            extr_file.close()

            sqlCommand = "select crpcde,crpnam,actnam,actno,opnorg,czcde from corp_list_his where upddat='%s' and crpsts in ('2')"%procDate.isoformat();
            curs.execute(sqlCommand)
            extr_file=open("d:\\temp\\corp\\cpyedit.txt.%s"%procDate.isoformat(),'wb')
            for row in curs.fetchall():
                writeLine=row[0]+' '+row[1]+' '+row[2]+' '+row[3]+' '+row[4]+' '+row[5] +'\n'
                extr_file.write(writeLine)
            extr_file.close()

            sqlCommand = "select crpcde,crpnam,actnam,actno,opnorg,czcde from corp_list_his where upddat='%s' and crpsts in ('4')"%procDate.isoformat();
            curs.execute(sqlCommand)
            extr_file=open("d:\\temp\\corp\\cpyedit1.txt.%s"%procDate.isoformat(),'wb')
            for row in curs.fetchall():
                writeLine=row[0]+' '+row[1]+' '+row[2]+' '+row[3]+' '+row[4]+' '+row[5] +'\n'
                extr_file.write(writeLine)
            extr_file.close()            
            

            ftp_crm = ftplib.FTP()
            ftp_crm.connect("21.96.5.54",21)
            ftp_crm.login("cardftp","QWER5678b")
            ftp_crm.set_pasv(False)
            ftp_crm.cwd("bin")
            extr_file=open("d:\\temp\\corp\\cpy.txt",'rb')
            ftp_crm.storbinary ("STOR cpy.txt",extr_file)
            extr_file.close()
            extr_file=open("d:\\temp\\corp\\num.txt",'rb')
            ftp_crm.storbinary ("STOR num.txt",extr_file)
            extr_file.close()
            extr_file=open("d:\\temp\\corp\\cpyadd.txt.%s"%procDate.isoformat(),'rb')
            ftp_crm.storbinary ("STOR cpyadd.txt.%s"%procDate.isoformat(),extr_file)
            extr_file.close()
            extr_file=open("d:\\temp\\corp\\cpyedit.txt.%s"%procDate.isoformat(),'rb')
            ftp_crm.storbinary ("STOR cpyedit.txt.%s"%procDate.isoformat(),extr_file)
            extr_file.close()
            extr_file=open("d:\\temp\\corp\\cpyedit1.txt.%s"%procDate.isoformat(),'rb')
            ftp_crm.storbinary ("STOR cpyedit1.txt.%s"%procDate.isoformat(),extr_file)
            extr_file.close()
            ftp_crm.close()
            return True,'公务卡数据产生成功'
        except Exception,msg:
            return False,'公务卡数据产生失败：'+str(msg)
    #集中采购项目短信提示
    def jzcgMessage(self,procDate):
        try:
            conn=cx_Oracle.connect(SasConfig.conn_newdss)
            curs_table=conn.cursor()
            curs_col=conn.cursor()
            runsql="select a.projno,a.prjnam, a.connum, a.expdat, b.phone  phone1, c.phone  phone2";
            runsql+="  from jzcg_projinfo a"
            runsql+="  left join (select jpj.phone ,projid from jzcg_projper jpj where jpj.pertyp = '1') b"
            runsql+="    on a.recid = b.projid "
            runsql+="  left join (select jpj.phone,projid from jzcg_projper jpj where jpj.pertyp = '2') c"
            runsql+="  on a.recid = c.projid"
            runsql+=" where a.status = '1'"
            runsql+="   and (trunc(sysdate) between add_months(to_date(expdat,'yyyymmdd'),-3) and add_months(to_date(expdat,'yyyymmdd'),-3) + 5"
            runsql+="    or trunc(sysdate) between add_months(to_date(expdat,'yyyymmdd'),-4) and add_months(to_date(expdat,'yyyymmdd'),-4) + 3)"

            curs_table.execute(runsql)
            for row_table in curs_table.fetchall():
                message="项目编号：%s，项目名称：%s，合同编号：%s 将于%s到期，请及时处理"%(row_table[0],row_table[1],row_table[2],row_table[3])
                Sms().SendSms2Server(message,row_table[4])
                Sms().SendSms2Server(message,row_table[5])
        except Exception,msg:
             return False,'集中采购项目发短信错误，错误信息：%s'%str(msg)
        else:
            return True,'集中采购项目发短信成功'
    def procBackupSas(self,procDate):
        os.system(r'D:\Projects\boc\batch\backup.bat')
        return True,'备份成功'
    def BackupScis(self,procDate):
        try:
            conn=cx_Oracle.connect(SasConfig.conn_dss)
            cursLog=conn.cursor()
            cursLast=conn.cursor()
            cursLog.execute("select table_name from cat where substr(table_name,1,3) in (%s) "%SasConfig.backup_tables)
            #for row in cursLog.fetchall():
            #    table_name=row[0]
            #    if os.system(r'exp %s file=D:\Backup\dump\%s tables=%s '%(SasConfig.conn_dss,table_name,table_name)) != 0:
            #         raise MyException('备份表%s错误'%table_name)
            conn.close()

            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            cursLog=conn.cursor()
            cursLast=conn.cursor()
            cursLog.execute("select table_name from cat where substr(table_name,1,3) in ('DPC') ")
            for row in cursLog.fetchall():
                table_name=row[0]
                os.system(r'exp %s file=D:\Backup\dump\%s tables=%s '%(SasConfig.conn_bancs,table_name,table_name)) 
            conn.close()            
            os.system(r'exp %s owner=dss file=D:\Backup\dump\dss rows=n'%SasConfig.conn_dss)
                #raise MyException('备份用户dss错误')
            tar = tarfile.open(r"d:\backup\custombac.tar.gz", "w:gz")
            tar.add(r"D:\Backup\dump")
            tar.close()
            time.sleep(30)

            
            ftp = ftplib.FTP(SasConfig.jzbf_ftp_ip)
            ftp.login(SasConfig.jzbf_ftp_user,SasConfig.jzbf_ftp_passwd)
            ftp.cwd("vch/custombac")
            ftp_file=open(r"d:\backup\custombac.tar.gz","rb")
            ftp.storbinary ("STOR custombac%s.tar.gz"%procDate.isoformat(),ftp_file)
            ftp_file.close()
          
            ftp.close()
        except Exception,msg:
             return False,'备份数据时错误，错误信息：%s'%str(msg)
        else:
            return True,'备份数据成功'


    def procCzt(self,procDate):
        #财政厅数据
        os.system(r'D:\Projects\boc\java\czt\czt.bat %s'%string.replace(procDate.isoformat(),'-',''))
        return True,'取ei报表数据成功'


    #信用卡数据装载-从FTP取数
    def get_card_data_v3(self,procDate):
        try:
            long_date=procDate.isoformat()
            #将当日需要处理的文件写入数据库
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            cursLog=conn.cursor()
            cursProc=conn.cursor()
            cursLog.callproc('proc_data.insert_card_log',(long_date,))
            #连接到FTP服务器
            ftp = ftplib.FTP()
            ftp.connect(SasConfig.card_ftp_ip,SasConfig.card_ftp_port)
            ftp.login(SasConfig.card_ftp_user,SasConfig.card_ftp_passwd)

            ftp.cwd("data")
            dir_list=ftp.nlst()
            cursLog.execute("select to_char(caddat,'yyyymmdd') caddat,filnam,filetype from dpc_card_log  where prcsts='0' order by caddat, filnam")
            for row in cursLog.fetchall():
                if row[2] == '1':
                    cardfile=row[1]+'_'+row[0][0:8]
                else:
                    cardfile=row[1]+'_'+row[0][2:8]
                print cardfile
                isfind = False
                get_card_file=""
                for server_file in dir_list:
                    
                    if string.find(server_file,cardfile) != -1 and server_file[-1] == 'c':
                        isfind=True
                        get_card_file = server_file
                        break;
                if isfind == True:
                    data_file=open(SasConfig.card_file_dir+get_card_file,'wb')
                    ftp.retrbinary("RETR "+get_card_file,data_file.write)
                    data_file.close()
                    cursProc.execute("update dpc_card_log set prcsts='1',relnam='%s' where caddat='%s' and filnam='%s'"%\
                                     (get_card_file,row[0],row[1]))
                    conn.commit()
            conn.commit()
            conn.close()
        except Exception,msg:
            errmsg=str(msg)
            if errmsg.find('descriptor') != -1 :
                return None,'取信用卡文本错误'+str(msg)
            else:
                return False,'取信用卡文本错误'+str(msg)
        return True,'取信用卡文本成功'
    
    #将信用卡的压缩文件转换成可以sqlldr到数据库的文件
    def procCardFile(self,sourceFile,destinFile,ctrnam,sourceFileName,filetype): 
        #print r'd:\PACL\paext -o+ -p%s %s'%(SasConfig.card_sqlldr_data,sourceFile)
        returnCode=os.system(r'd:\PACL\paext -o+ -p%s %s'%(SasConfig.card_sqlldr_data,sourceFile))
        
        if returnCode!= 0 :
            raise MyException('解压%s时出错'%sourceFile)
        if filetype == '1':
            inFileName=r"%s%s.gz"%(SasConfig.card_sqlldr_data,sourceFileName[:-5])
        else:
            inFileName=r"%s%s.txt"%(SasConfig.card_sqlldr_data,sourceFileName[:-6])
        inFile=open(inFileName,'rb')
        outFile=open(destinFile,'wb')
        line=inFile.readline() #忽略第一行
        while 1:
            line=inFile.readline()
            if not line:
                break
            if line[0:11] == 'TotalRecord':
                continue
            outFile.write(line)
        outFile.close()
        inFile.close()
        os.remove(inFileName)

    #数据装入数据库
    def procCard2DB_v3(self,procDate):
        #reload(SasConfig)
        try:
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            cursLog=conn.cursor()
            cursLog.execute("select to_char(caddat,'yyyymmdd') caddat,relnam,ctlfile,incdat,filetype from dpc_card_log  \
                            where prcsts='1' and prcdb='0'  order by caddat")
            for row in cursLog.fetchall():
                fields=string.split(row[1],'_')
                ctrnam=row[2]
                self.procCardFile(SasConfig.card_file_dir+row[1],SasConfig.card_sqlldr_data+ctrnam+'.txt',ctrnam,row[1],row[4])

                control_file=SasConfig.oracle_ctl_dir+ctrnam+'.ctl'
                log_file=SasConfig.sas_log+row[0][0:4]+'-'+row[0][4:6]+'-'+row[0][6:8]+ctrnam+".log"
                bad_file=SasConfig.card_sqlldr_data+row[0][0:4]+'-'+row[0][4:6]+'-'+row[0][6:8]+ctrnam+".bad"
                
                if(os.system(r"sqlldr %s control=%s bindsize=10000000 rows=500 log=%s data=%s bad=%s"%\
                             (SasConfig.conn_dataprc,control_file,log_file,SasConfig.card_sqlldr_data+ctrnam+'.txt',bad_file)) != 0):
                    raise MyException('用sqlldr装入数据%s时出错！'%ctrnam)
                #部分表要加上更新日期
                if row[3] == '1':
                    cursLog.execute("update dataprc.%s set upddat='%s'"%(ctrnam,row[0]))
                cursLog.callproc('proc_data.prc_inc_data',(ctrnam,))
                cursLog.execute("update dpc_card_log set prcdb='1' where caddat='%s' and relnam='%s'"%\
                                     (row[0],row[1]))
                conn.commit()
            conn.commit()
            conn.close()
            return True,'信用卡数据装入数据库成功!'
        except Exception,msg:
            return False,'信用卡数据装入数据库失败：'+str(msg)+ctrnam
        
    def procCard2DB_v3_t(self,procDate,filnam):
        #reload(SasConfig)
        try:
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            cursLog=conn.cursor()
            cursLog.execute("select to_char(caddat,'yyyymmdd') caddat,relnam,ctlfile,incdat,filetype from dpc_card_log  where prcsts='1' \
                             and prcdb='0' and filnam='%s' order by caddat"%filnam)
            for row in cursLog.fetchall():
                fields=string.split(row[1],'_')
                ctrnam=row[2]
                self.procCardFile(SasConfig.card_file_dir+row[1],SasConfig.card_sqlldr_data+ctrnam+'.txt',ctrnam,row[1],row[4])

                control_file=SasConfig.oracle_ctl_dir+ctrnam+'.ctl'
                log_file=SasConfig.sas_log+row[0][0:4]+'-'+row[0][4:6]+'-'+row[0][6:8]+ctrnam+".log"
                bad_file=SasConfig.card_sqlldr_data+row[0][0:4]+'-'+row[0][4:6]+'-'+row[0][6:8]+ctrnam+".bad"
                
                if(os.system(r"sqlldr %s control=%s bindsize=10000000 rows=500 log=%s data=%s bad=%s"%\
                             (SasConfig.conn_dataprc,control_file,log_file,SasConfig.card_sqlldr_data+ctrnam+'.txt',bad_file)) != 0):
                    raise MyException('用sqlldr装入数据%s时出错！'%ctrnam)
                #部分表要加上更新日期
                if row[3] == '1':
                    cursLog.execute("update dataprc.%s set upddat='%s'"%(ctrnam,row[0]))
                cursLog.callproc('proc_data.prc_inc_data',(ctrnam,))
                cursLog.execute("update dpc_card_log set prcdb='1' where caddat='%s' and relnam='%s'"%\
                                     (row[0],row[1]))
                conn.commit()
            conn.commit()
            conn.close()
            return True,'信用卡数据装入数据库成功!'
        except Exception,msg:
            return False,'信用卡数据装入数据库失败：'+str(msg)+ctrnam

    #卡文件传集中备份服务器
    def procCardData2Jzbf(self,procDate):
        try:
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            cursLog=conn.cursor()

            ftp_jzbf = ftplib.FTP()
            ftp_jzbf.connect(SasConfig.jzbf_ftp_ip,SasConfig.jzbf_ftp_port)
            ftp_jzbf.login(SasConfig.jzbf_ftp_user,SasConfig.jzbf_ftp_passwd)
            ftp_jzbf.cwd("vch/card_data")
            
            cursLog.execute("select to_char(caddat,'yyyymmdd') caddat,relnam,ctlfile,incdat from dpc_card_log  where tojzbf='0' and prcsts='1'")
            for row in cursLog.fetchall():
                data_file=open(SasConfig.card_file_dir+row[1],"rb")
                ftp_jzbf.storbinary ("STOR "+row[1],data_file)
                data_file.close()

                cursLog.execute("update dpc_card_log set tojzbf='1' where caddat='%s' and relnam='%s'"%\
                                     (row[0],row[1]))
                conn.commit()
            ftp_jzbf.quit()
            conn.commit()
            conn.close()
            return True,'信用卡数据传集中备份成功!'
        except Exception,msg:
            return False,'信用卡数据传集中备份失败：'+str(msg)

    #卡文件传稽核
    def procCardData2JH(self,procDate):
        try:
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            cursLog=conn.cursor()

            ftp_jh = ftplib.FTP()
            ftp_jh.connect("22.96.2.91","21")
            ftp_jh.login("iax","iaxisftp")
            
            cursLog.execute("select to_char(caddat,'yyyymmdd') caddat,relnam,ctlfile,incdat from dpc_card_log  where tojh='0' and prcsts='1'")
            for row in cursLog.fetchall():
                data_file=open(SasConfig.card_file_dir+row[1],"rb")
                ftp_jh.storbinary ("STOR "+row[1],data_file)
                data_file.close()

                cursLog.execute("update dpc_card_log set tojh='1' where caddat='%s' and relnam='%s'"%\
                                     (row[0],row[1]))
                conn.commit()
            ftp_jh.quit()
            conn.commit()
            conn.close()
            return True,'信用卡数据传稽核成功!'
        except Exception,msg:
            return False,'信用卡数据传稽核失败：'+str(msg)
if __name__=='__main__':
    method=getattr(RunPython(),sys.argv[1])
    procDate=date(int(sys.argv[2][0:4]),int(sys.argv[2][4:6]),int(sys.argv[2][6:8])) 
    (success,errMsg)=method(procDate)
    print errMsg
    ##extr_file=open("d:\\tmp\\1",'w')
    ##extr_file.write(errMsg)
    ##extr_file.close()
    if success == False:
        sys.exit(2)
    else:
        sys.exit(0)
