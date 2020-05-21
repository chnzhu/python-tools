#win32������չģ��
import win32serviceutil,win32service,win32event,servicemanager,win32com.client
import win32con,win32file,pywintypes
import cx_Oracle
#�������ģ��
from datetime import date,timedelta
import os
import thread,time
import ftplib,types
import tarfile
import gzip
#��������ģ��
import ConfigParser,string
import zipfile
import tempfile
from socket import *
#webserviceģ��
from ZSI.client import NamedParamBinding as NPBinding
from ZSI.client import AUTH
import sys

sys.path.append(r'D:\Projects\boc\pgm\etlnew')
from SasConfig import SasConfig,Sms

#�����쳣
class MyException(Exception):
    def __init__(self,msg):
        self.args=msg
     
class RunPython:
    #�ж�GLDM���Ƿ������
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
            return False,'����loadGLDM���󣬴�����Ϣ��%s'%str(msg)
        else:
            return True,'GLDM�������'
            #�ж�GLDM���Ƿ������
    #sgjj_hd:�ж�ITAS��INVM���Ƿ������    
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
            return False,'����loadITAS_TXN_TIF_BAS���󣬴�����Ϣ��%s'%str(msg)
        else:
            return True,'ITAS_TXN_TIF_BAS�������'

    #�������񿨵�����
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
            return True,'�������ݲ����ɹ�'
        except Exception,msg:
            errmsg=str(msg)
            if errmsg.find('descriptor') != -1 :
                return None,'�������ݲ���ʧ�ܣ�'+str(msg)
            else:
                return False,'�������ݲ���ʧ�ܣ�'+str(msg)

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
            return True,'�������ݲ����ɹ�'
        except Exception,msg:
            return False,'�������ݲ���ʧ�ܣ�'+str(msg)
    #���вɹ���Ŀ������ʾ
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
                message="��Ŀ��ţ�%s����Ŀ���ƣ�%s����ͬ��ţ�%s ����%s���ڣ��뼰ʱ����"%(row_table[0],row_table[1],row_table[2],row_table[3])
                Sms().SendSms2Server(message,row_table[4])
                Sms().SendSms2Server(message,row_table[5])
        except Exception,msg:
             return False,'���вɹ���Ŀ�����Ŵ��󣬴�����Ϣ��%s'%str(msg)
        else:
            return True,'���вɹ���Ŀ�����ųɹ�'
    def procBackupSas(self,procDate):
        os.system(r'D:\Projects\boc\batch\backup.bat')
        return True,'���ݳɹ�'
    def BackupScis(self,procDate):
        try:
            conn=cx_Oracle.connect(SasConfig.conn_dss)
            cursLog=conn.cursor()
            cursLast=conn.cursor()
            cursLog.execute("select table_name from cat where substr(table_name,1,3) in (%s) "%SasConfig.backup_tables)
            #for row in cursLog.fetchall():
            #    table_name=row[0]
            #    if os.system(r'exp %s file=D:\Backup\dump\%s tables=%s '%(SasConfig.conn_dss,table_name,table_name)) != 0:
            #         raise MyException('���ݱ�%s����'%table_name)
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
                #raise MyException('�����û�dss����')
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
             return False,'��������ʱ���󣬴�����Ϣ��%s'%str(msg)
        else:
            return True,'�������ݳɹ�'


    def procCzt(self,procDate):
        #����������
        os.system(r'D:\Projects\boc\java\czt\czt.bat %s'%string.replace(procDate.isoformat(),'-',''))
        return True,'ȡei�������ݳɹ�'


    #���ÿ�����װ��-��FTPȡ��
    def get_card_data_v3(self,procDate):
        try:
            long_date=procDate.isoformat()
            #��������Ҫ������ļ�д�����ݿ�
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            cursLog=conn.cursor()
            cursProc=conn.cursor()
            cursLog.callproc('proc_data.insert_card_log',(long_date,))
            #���ӵ�FTP������
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
                return None,'ȡ���ÿ��ı�����'+str(msg)
            else:
                return False,'ȡ���ÿ��ı�����'+str(msg)
        return True,'ȡ���ÿ��ı��ɹ�'
    
    #�����ÿ���ѹ���ļ�ת���ɿ���sqlldr�����ݿ���ļ�
    def procCardFile(self,sourceFile,destinFile,ctrnam,sourceFileName,filetype): 
        #print r'd:\PACL\paext -o+ -p%s %s'%(SasConfig.card_sqlldr_data,sourceFile)
        returnCode=os.system(r'd:\PACL\paext -o+ -p%s %s'%(SasConfig.card_sqlldr_data,sourceFile))
        
        if returnCode!= 0 :
            raise MyException('��ѹ%sʱ����'%sourceFile)
        if filetype == '1':
            inFileName=r"%s%s.gz"%(SasConfig.card_sqlldr_data,sourceFileName[:-5])
        else:
            inFileName=r"%s%s.txt"%(SasConfig.card_sqlldr_data,sourceFileName[:-6])
        inFile=open(inFileName,'rb')
        outFile=open(destinFile,'wb')
        line=inFile.readline() #���Ե�һ��
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

    #����װ�����ݿ�
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
                    raise MyException('��sqlldrװ������%sʱ����'%ctrnam)
                #���ֱ�Ҫ���ϸ�������
                if row[3] == '1':
                    cursLog.execute("update dataprc.%s set upddat='%s'"%(ctrnam,row[0]))
                cursLog.callproc('proc_data.prc_inc_data',(ctrnam,))
                cursLog.execute("update dpc_card_log set prcdb='1' where caddat='%s' and relnam='%s'"%\
                                     (row[0],row[1]))
                conn.commit()
            conn.commit()
            conn.close()
            return True,'���ÿ�����װ�����ݿ�ɹ�!'
        except Exception,msg:
            return False,'���ÿ�����װ�����ݿ�ʧ�ܣ�'+str(msg)+ctrnam
        
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
                    raise MyException('��sqlldrװ������%sʱ����'%ctrnam)
                #���ֱ�Ҫ���ϸ�������
                if row[3] == '1':
                    cursLog.execute("update dataprc.%s set upddat='%s'"%(ctrnam,row[0]))
                cursLog.callproc('proc_data.prc_inc_data',(ctrnam,))
                cursLog.execute("update dpc_card_log set prcdb='1' where caddat='%s' and relnam='%s'"%\
                                     (row[0],row[1]))
                conn.commit()
            conn.commit()
            conn.close()
            return True,'���ÿ�����װ�����ݿ�ɹ�!'
        except Exception,msg:
            return False,'���ÿ�����װ�����ݿ�ʧ�ܣ�'+str(msg)+ctrnam

    #���ļ������б��ݷ�����
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
            return True,'���ÿ����ݴ����б��ݳɹ�!'
        except Exception,msg:
            return False,'���ÿ����ݴ����б���ʧ�ܣ�'+str(msg)

    #���ļ�������
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
            return True,'���ÿ����ݴ����˳ɹ�!'
        except Exception,msg:
            return False,'���ÿ����ݴ�����ʧ�ܣ�'+str(msg)
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
