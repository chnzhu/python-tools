from ZSI.client import NamedParamBinding as NPBinding
from ZSI.client import AUTH
import win32serviceutil,win32service,win32event,servicemanager,win32com.client
class SasConfig:
    conn_plan="plan/y9d6@query" #oracle���ݿ�����Ӳ���
    conn_dss="dss/ssd@dbwh" #oracle���ݿ�����Ӳ���
    conn_channel="channel/channel@dbwh" #oracle���ݿ�����Ӳ���
    conn_crm="duns/nsdu@crm" #oracle���ݿ�����Ӳ���
    conn_card='card/qydrac@query'#���ÿ����ݵ�����
    conn_bancs='bancs/true56@query570'
    conn_ereport='ereport/cvdr12@query570'
    conn_newdss='newdss/tiwn23@query570'
    conn_dataprc='dataprc/yetx12@query570'
    conn_gyjx='gyjx/wsie32@query570'
    
    sas_date=r'D:\Projects\boc\pgm\ETLNEW\date.txt' #sas���ڵ��ļ�
    sas_file="D:\\Projects\\boc\\pgm\\ETLNEW\\" #sas�����Ŀ¼
    sas_log="D:\\Projects\\boc\\logs\\" #sas��־��Ŀ¼
    phone_num=('13551384548',) #����ʧ��ʱ���Ͷ��ŵĺ���

    card_ftp_ip='21.7.1.47' #���ÿ�FTP��������ַ  
    card_ftp_port='21'   #���ÿ�FTP�������˿�
    card_ftp_user='s5100000'#���ÿ�FTP�������û�
    card_ftp_passwd='q1w2e3r4'#���ÿ�FTP����������
    card_file_dir="d:\\card_data\\" #���ÿ��ı�����Ŀ¼
    card_sqlldr_data="d:\\temp\card\\"#sqlldr�����ļ���Ŀ¼
    jzbf_ftp_ip='21.96.4.100' #���б���FTP��������ַ  
    jzbf_ftp_port='21'   #���б���FTP�������˿�
    jzbf_ftp_user='vch'#���б���FTP�������û�
    jzbf_ftp_passwd='rNJkal@4' #CRM��FTP����������
    crm_ftp_ip='22.96.2.77' #CRM��FTP��������ַ  
    crm_ftp_port='21'   #CRM��FTP�������˿�
    crm_ftp_user='crmftp'#CRM��FTP�������û�
    crm_ftp_passwd='ftp12crm'#CRM��FTP����������
    oracle_ctl_dir='D:\\Projects\\boc\\oracle_ctl\\'#sqlldr��control�ļ��ŵ�Ŀ¼
    

    backup_tables="'XT_','BIL','ICC'"#��Ҫ���ݵı�
class Sms:
    def SendSms(self,send_message):#������
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