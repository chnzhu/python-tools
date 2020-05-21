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
from SasConfig import SasConfig
import RunPython

    
#�����쳣
class MyException(Exception):
    def __init__(self,msg):
        self.args=msg
        
class ProcETL:
    #def __init__(self):
    def GetRealDate(self):
        try:
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            curs=conn.cursor ()
            curs.execute ("select to_char(sysdate,'yyyy-mm-dd') as crndat from dual") 
            row=curs.fetchone()
            crndat=row[0]
            conn.close()
            return date(int(crndat[0:4]),int(crndat[5:7]),int(crndat[8:10]))
        except cx_Oracle.Error,msg:
            raise MyException(str(msg[0]))
    def GetRealTime(self):
        try:
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            curs=conn.cursor ()
            curs.execute ("select to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') as crndat from dual") 
            row=curs.fetchone()
            crndat=row[0]
            conn.close()
            return  (int(crndat[0:4]),int(crndat[5:7]),int(crndat[8:10]),int(crndat[11:13]),int(crndat[14:16]),int(crndat[17:19]))
        except cx_Oracle.Error,msg:
            raise MyException(str(msg[0]))

    def ETLProcess(self):
        try:
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            cursLog=conn.cursor()
            cursLast=conn.cursor()
            #�ҳ�ÿһ�����δִ�е���Сֵ
            cursLog.execute("select job_class,min(to_char(job_date,'yyyy-mm-dd')||trim(to_char(job_id,'09999')))\
                             from DPC_PROC_LOG where run_flag='0' group by job_class")
            for row in cursLog.fetchall():
                job_date=row[1][0:10]
                job_id=int(row[1][10:15])
                #�ó������һ�����״̬
                cursLast.execute("select run_flag,checked from (select * from DPC_PROC_LOG where job_class='%s' and job_date='%s' and \
                                job_id < %d order by job_id desc) where rownum=1"%(row[0],job_date,job_id))
      
                rowLast=cursLast.fetchone()
                if not rowLast:
                    thread.start_new_thread(self.ProcessJob,(job_date,job_id))
                elif rowLast[0] == '1' or rowLast[0] == '4':
                    continue
                elif rowLast[0] == '3' and rowLast[1] == '1':
                    continue
                else:
                    thread.start_new_thread(self.ProcessJob,(job_date,job_id))
                time.sleep(30)
            conn.close()
        except Exception,msg:
            if __name__=='__main__':
                print msg
            else:
                servicemanager.LogErrorMsg('ETLProcess����,'+str(msg))
                self.SendSms('ETLProcess����,'+str(msg))
    def ProcessJob(self,jobDate,jobID):
        try:
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            cursLog=conn.cursor()
            cursLog.execute("select job,job_type,to_char(proc_date,'yyyy-mm-dd'),send_sms,rerun from DPC_PROC_LOG where job_date='%s'\
                            and job_id = %d"%(jobDate,jobID))
            row=cursLog.fetchone()
            proc_date=date(int(row[2][0:4]),int(row[2][5:7]),int(row[2][8:10]))
            cursLog.execute("update DPC_PROC_LOG set run_flag='1',begin_run=sysdate where job_date='%s'\
                            and job_id = %d"%(jobDate,jobID))
            conn.commit()

            if row[3] == '1':
                self.SendSms('��ʼ����%s����������%s'%(row[0],row[2]))
            success = False
            errMsg = ""
            if row[1] == '1':
                (success,errMsg) = self.ProcessSas(row[0],proc_date)
            elif row[1] == '2':
                method=getattr(self,row[0])
                (success,errMsg) = method(proc_date)
            elif row[1] == '3':
                (success,errMsg) = self.ProcessProcedurePlan(row[0],proc_date)
            elif row[1] == '4':
                (success,errMsg) = self.ProcessProcedureDss(row[0],proc_date)
            elif row[1] == '5':
                (success,errMsg) = self.ProcessProcedureBancs(row[0],proc_date)
            elif row[1] == '6':
                (success,errMsg) = self.ProcessProcedureEreport(row[0],proc_date)
            elif row[1] == '7':
                (success,errMsg) = self.ProcessProcedureNewdss(row[0],proc_date)
            elif row[1] == '8':
                (success,errMsg) = self.ProcessProcedureGyjx(row[0],proc_date)
            elif row[1] == '9':
                reload(RunPython)
                method=getattr(RunPython.RunPython(),row[0])
                (success,errMsg) = method(proc_date)
            elif row[1] == 'a':
                return
            elif row[1] == 'b':
                (success,errMsg) = self.ProcessProcedureDappdm(row[0],proc_date)
            elif row[1] == '0':
                cursLog.execute("update DPC_PROC_LOG set run_flag='4' where job_date='%s'\
                                and job_id = %d"%(jobDate,jobID))
                conn.commit()
                conn.close()
                return
            #�жϽ��
            if success == None:#snapshut too old����
                if row[4] == '1':
                    cursLog.execute("update DPC_PROC_LOG set run_flag='0',end_run=sysdate where job_date='%s'\
                                and job_id = %d"%(jobDate,jobID))
                    self.SendSms('ִ��%s�յ�%s����snapshot����'%(row[2],row[0]))
                else:
                    cursLog.execute("update DPC_PROC_LOG set run_flag='3',end_run=sysdate where job_date='%s'\
                                and job_id = %d"%(jobDate,jobID))
                    self.SendSms('ִ��%s�յ�%sʧ��'%(row[2],row[0]))           
            elif success == False:#ʧ��
                cursLog.execute("update DPC_PROC_LOG set run_flag='3',end_run=sysdate where job_date='%s'\
                            and job_id = %d"%(jobDate,jobID))
                ##servicemanager.LogErrorMsg('ִ��%sʧ�ܣ�������Ϣ:%s'%(row[0],errMsg))
                self.SendSms('ִ��%s�յ�%sʧ��'%(row[2],row[0]))
            elif success == True:#�ɹ�
                cursLog.execute("update DPC_PROC_LOG set run_flag='2',end_run=sysdate where job_date='%s'\
                            and job_id = %d"%(jobDate,jobID))
            else:
                self.SendSms('Process����ķ���ֵ�д���')
            errMsg=string.replace(errMsg,"'","��")
            cursLog.execute("update DPC_PROC_LOG set job_comment='%s' where job_date='%s'\
                            and job_id= %d"%(errMsg,jobDate,jobID))
            if row[3] == '2':
                self.SendSms('����%s��ɣ���������%s'%(row[0],row[2]))
            conn.commit()
            conn.close()
        except Exception,msg:
            if __name__=='__main__':
                print msg
            else:
                servicemanager.LogErrorMsg('ProcessJob����,'+str(msg))
                self.SendSms('ProcessJob����,'+str(msg))
##    def ProcessSas(self,jobName,procDate):
##        time.sleep(60*1)
##        return True,'ִ��%s�ɹ�'%jobName
    def ProcessSas(self,jobName,procDate):
        try:
            #�޸�sas_date�ļ��е�����
            fp=open(SasConfig.sas_date,'w')
            fp.write(procDate.isoformat())
            fp.close()
            
            log_file='%s%s%s.log'%(SasConfig.sas_log,procDate.isoformat(),jobName)
            command='call "D:\SAS9\SAS9.1\sas" -nodms -sysin "%s%s.sas" -log "%s"'%(SasConfig.sas_file,jobName,log_file)
            os.system(command)
            #�����־���Ƿ��д� 
            fp=open(log_file,'r')
            log_msg=fp.read()
            fp.close()
            #�����ļ� WORK.ZFMMEM.UTILITY �����ø�����ɾ���Ĵ���
            log_msg=string.replace(log_msg,'ERROR: ������ҳ���ϳ������Դ���','tttt')
            tns_err=""
            tns_err=tns_err+"ERROR: ORACLE connection error: ORA-12560: TNS:protocol adapter error."+chr(13)
            tns_err=tns_err+"ERROR: ORACLE connection error: ORA-12560: TNS:protocol adapter error."+chr(13)
            tns_err=tns_err+"ERROR: ORACLE connection error: ORA-12560: TNS:protocol adapter error.\n"
            tns_err=tns_err+"ERROR: LIBNAME ������"+chr(13)+"ERROR: LIBNAME ������"+chr(13)
            tns_err=tns_err+"ERROR: LIBNAME ������"
            having_tns_err=log_msg.find(tns_err)
            log_msg=string.replace(log_msg,tns_err,'tttt')
            
            tns_err="ERROR: ORACLE connection error: ORA-12500: TNS:protocol adapter error." +chr(13)
            tns_err=tns_err+"ERROR: ORACLE connection error: ORA-12500: TNS:protocol adapter error."+chr(13)
            tns_err=tns_err+"ERROR: ORACLE connection error: ORA-12500: TNS:protocol adapter error.\n"
            tns_err=tns_err+"ERROR: LIBNAME ������"+chr(13)+"ERROR: LIBNAME ������"+chr(13)
            tns_err=tns_err+"ERROR: LIBNAME ������"
            if having_tns_err == -1:
                having_tns_err=log_msg.find(tns_err)
            log_msg=string.replace(log_msg,tns_err,'tttt')
            
            log_msg=string.replace(log_msg,'ERROR: �ļ� WORK.ZFMMEM.UTILITY �����ø�����ɾ��','tttt')
            log_msg=log_msg.lower()
            if log_msg.find('ora-01555') != -1 or log_msg.find('ora-12560') != -1 or log_msg.find('ora-12500') != -1\
               or (having_tns_err != -1 and log_msg.find('error:') != -1):
                return None,'ִ��SAS����%s����,��鿴��־�ļ�:%s'%(jobName,log_file)
            elif log_msg.find('error:') != -1 or log_msg.find('ora-') != -1:
                return False,'ִ��SAS����%s����,��鿴��־�ļ�:%s'%(jobName,log_file)
            else:
                return True,'ִ��SAS����%s�ɹ�'%jobName
        except Exception,msg:
            return False,'ִ��SAS����%s����,������Ϣ:%s'%(jobName,str(msg))

    def ProcessProcedurePlan(self,jobName,procDate):
        try:
            conn=cx_Oracle.connect (SasConfig.conn_plan)
            curs=conn.cursor ()
            curs.callproc(jobName,(procDate.isoformat(),))
            conn.close()
        except Exception,msg:
            errmsg=str(msg)
            if errmsg.find('ORA-01555') != -1 or errmsg.find('ORA-12537') != -1 or errmsg.find('ORA-12560') != -1 :
                return None,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
            else:
                return False,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
        else:
            return True,'ִ�д洢����%s�ɹ�'%jobName

    def ProcessProcedureDss(self,jobName,procDate):
        try:
            conn=cx_Oracle.connect (SasConfig.conn_dss)
            curs=conn.cursor ()
            curs.callproc(jobName,(procDate.isoformat(),))
            conn.close()
        except Exception,msg:
            errmsg=str(msg)
            if errmsg.find('ORA-01555') != -1 or errmsg.find('ORA-12537') != -1 or errmsg.find('ORA-12560') != -1 :
                return None,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
            else:
                return False,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
        else:
            return True,'ִ�д洢����%s�ɹ�'%jobName
    def ProcessProcedureBancs(self,jobName,procDate):
        try:
            conn=cx_Oracle.connect (SasConfig.conn_bancs)
            curs=conn.cursor ()
            curs.callproc(jobName,(procDate.isoformat(),))
            conn.close()
        except Exception,msg:
            errmsg=str(msg)
            if errmsg.find('ORA-01555') != -1 or errmsg.find('ORA-12537') != -1 or errmsg.find('ORA-12560') != -1 :
                return None,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
            else:
                return False,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
        else:
            return True,'ִ�д洢����%s�ɹ�'%jobName

    def ProcessProcedureEreport(self,jobName,procDate):
        try:
            conn=cx_Oracle.connect (SasConfig.conn_ereport)
            curs=conn.cursor ()
            curs.callproc(jobName,(procDate.isoformat(),))
            conn.close()
        except Exception,msg:
            errmsg=str(msg)
            if errmsg.find('ORA-01555') != -1 or errmsg.find('ORA-12537') != -1 or errmsg.find('ORA-12560') != -1 :
                return None,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
            else:
                return False,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
        else:
            return True,'ִ�д洢����%s�ɹ�'%jobName
    def ProcessProcedureNewdss(self,jobName,procDate):
        try:
            conn=cx_Oracle.connect (SasConfig.conn_newdss)
            curs=conn.cursor ()
            curs.callproc(jobName,(procDate.isoformat(),))
            conn.close()
        except Exception,msg:
            errmsg=str(msg)
            if errmsg.find('ORA-01555') != -1 or errmsg.find('ORA-12537') != -1 or errmsg.find('ORA-12560') != -1 :
                return None,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
            else:
                return False,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
        else:
            return True,'ִ�д洢����%s�ɹ�'%jobName
    def ProcessProcedureGyjx(self,jobName,procDate):
        try:
            conn=cx_Oracle.connect (SasConfig.conn_gyjx)
            curs=conn.cursor ()
            curs.callproc(jobName,(procDate.isoformat(),))
            conn.close()
        except Exception,msg:
            errmsg=str(msg)
            if errmsg.find('ORA-01555') != -1 or errmsg.find('ORA-12537') != -1 or errmsg.find('ORA-12560') != -1 :
                return None,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
            else:
                return False,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
        else:
            return True,'ִ�д洢����%s�ɹ�'%jobName
    def ProcessProcedureDappdm(self,jobName,procDate):
        try:
            conn=cx_Oracle.connect (SasConfig.conn_dappdm)
            curs=conn.cursor ()
            curs.callproc(jobName,(procDate.isoformat(),))
            conn.close()
        except Exception,msg:
            errmsg=str(msg)
            if errmsg.find('ORA-01555') != -1 or errmsg.find('ORA-12537') != -1 or errmsg.find('ORA-12560') != -1 :
                return None,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
            else:
                return False,'ִ�д洢���̴���%s����,������Ϣ:%s'%(jobName,str(msg))
        else:
            return True,'ִ�д洢����%s�ɹ�'%jobName
##    def SendSms(self,send_message):#������
##        servicemanager.LogInfoMsg('�����ţ�%s'%send_message)
    def SendSms(self,send_message):#������
        try:
            thread.start_new_thread(self.SendSmsBack,(send_message,))
        except Exception,msg:
            servicemanager.LogErrorMsg('������ʧ�ܣ�������Ϣ��%s'%str(msg))
    def SendSmsBack(self,send_message):#������
        try:
            servicemanager.LogInfoMsg('�����ţ�%s'%send_message)
            for phone_num in SasConfig.phone_num:
                try:
                    self.SendSms2Server(send_message,phone_num)
                except Exception,msg:
                    #ʧ�ܺ������·�һ��
                    time.sleep(30)
                    self.SendSms2Server(send_message,phone_num)
        except Exception,msg:
            servicemanager.LogErrorMsg('������ʧ�ܣ�������Ϣ��%s'%str(msg))

    def SendSms2Server(self,message,phoneNumber):
        
        sendMessage=message.decode('GBK').encode('UTF-8')
        fp=open(r'D:\Projects\boc\logs\sms.log','a+')
        b=NPBinding(url='http://21.96.51.66:8080/axis/services/SendSms?wsdl',tracefile=fp)
        b.SetAuth(AUTH.httpbasic,'sendsms','zaq1xsw2')
        b.sendSms(phoneNumber=phoneNumber,message=sendMessage)
        fp.close()
    def Job2Log(self,conn,whereSQL,jobDate):
        conn=cx_Oracle.connect (SasConfig.conn_bancs)
        curs=conn.cursor ()
        curs.callproc("proc_data.job2log",(jobDate.isoformat(),whereSQL))
        conn.close()

    def ETLPrepare(self):
       
        try:
            #��ʱִ�еĳ���
            conn=cx_Oracle.connect(SasConfig.conn_bancs)
            (y,m,d,h,mi,s)= self.GetRealTime()[0:6]
            curr_time = "%02d%02d"%(h,mi)
            whereSQL="job_class in (select job_class from dpc_proc_class where run_type='2') and run_time<='%s'"%curr_time
            jobDate=self.GetRealDate() -  timedelta(days=1)
            self.Job2Log(conn,whereSQL,jobDate)
            conn.commit()

            #�������
            curs=conn.cursor ()
            #����װ�ص�����
            curs.execute ("select to_char(curdat,'yyyy-mm-dd'),prcsts from dpc_dssdate where rownum=1") 
            row=curs.fetchone()
            crndat=row[0]
            if row[1] != '2' and row[1] != '3':
                return;

            crndat=date(int(crndat[0:4]),int(crndat[5:7]),int(crndat[8:10]))

            #���ݴ�����������ݵ�����
            curs.execute ("select to_char(date_value,'yyyy-mm-dd') from dpc_proc_date where date_type='DSS'") 
            row=curs.fetchone()
            crmDate=row[0]
            crmDate=date(int(crmDate[0:4]),int(crmDate[5:7]),int(crmDate[8:10]))
            
            if crmDate < crndat :
                whereSQL="job_class in (select job_class from dpc_proc_class where run_type='1') and run_type='1'"
                while crmDate < crndat:
                    crmDate=crmDate + timedelta(days=1)
                    self.Job2Log(conn,whereSQL,crmDate)
                ##curs.execute("update dpc_proc_date set date_value='%s' where date_type='DSS'"%crmDate.isoformat())
            conn.commit()
            conn.close()           
        except Exception,msg:
            if __name__=='__main__':
                print msg
            else:
                servicemanager.LogErrorMsg('ETLPrepare����,'+str(msg))
                self.SendSms('ETLPrepare����,'+str(msg))
        

class ProcJobService(win32serviceutil.ServiceFramework):
    _svc_name_ = "procauto"
    _svc_display_name_ = "���ݴ��������ȷ���"
    _svc_description_="���ݴ��������ȷ���"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.timerForProcess=win32event.CreateWaitableTimer(None,0,None) 
        self.timerForPrepare=win32event.CreateWaitableTimer(None,0,None) 
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        # And set my event.
        win32event.SetEvent(self.hWaitStop)
 
    def SvcDoRun(self):
        # Log a "started" message to the event log.
        
        servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE, 
                servicemanager.PYS_SERVICE_STARTED,
                (self._svc_name_, ''))

        ProcETL().SendSms('ProcJob��ʼ����')
        #���ϵͳ���ڲ���2007�꣬��ʱ�����Ϊ2007
        (y,m,d,h,mi,s)= time.localtime()[0:6]
        if y != 2007:
            setdate='%04d-%02d-%02d'%(2007,m,d)
            os.system('date %s'%setdate)
        win32event.SetWaitableTimer(self.timerForProcess,0,1000*60*1,None,None,0)
        win32event.SetWaitableTimer(self.timerForPrepare,0,1000*60*2,None,None,0)
        while 1:
            timeout = win32event.INFINITE
            waitHandles = self.hWaitStop,self.timerForProcess,self.timerForPrepare
            rc = win32event.WaitForMultipleObjects(waitHandles, 0, timeout)
            if rc==win32event.WAIT_OBJECT_0:
                # Stop event
                break
            elif rc==win32event.WAIT_OBJECT_0+1:
                ProcETL().ETLProcess()
            elif rc==win32event.WAIT_OBJECT_0+2:
                ProcETL().ETLPrepare()
        win32event.CancelWaitableTimer(self.timerForProcess)
        win32event.CancelWaitableTimer(self.timerForPrepare)
        servicemanager.LogMsg(
                servicemanager.EVENTLOG_INFORMATION_TYPE, 
                servicemanager.PYS_SERVICE_STOPPED,
                (self._svc_name_, ''))


if __name__=='__main__':
    win32serviceutil.HandleCommandLine(ProcJobService)
    ##ProcETL().ETLPrepare()
    ##ProcETL().ETLProcess()
    ##ProcETL().CheckSasJob(date(2008,10,30))