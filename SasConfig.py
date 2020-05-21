from ZSI.client import NamedParamBinding as NPBinding
from ZSI.client import AUTH
import win32serviceutil,win32service,win32event,servicemanager,win32com.client
class SasConfig:
    conn_plan="plan/1111@query" #oracle数据库的连接参数
    conn_dss="dss/111@dbwh" #oracle数据库的连接参数
    conn_channel="channel/11111@dbwh" #oracle数据库的连接参数
    conn_crm="duns/1111@crm" #oracle数据库的连接参数
    conn_card='card/11111@query'#信用卡数据的连接
    conn_bancs='bancs/11111@query570'
    conn_ereport='ereport/11111@query570'
    conn_newdss='newdss/11111@query570'
    conn_dataprc='dataprc/11111@query570'
    conn_gyjx='gyjx/11111@query570'
    
    sas_date=r'D:\Projects\boc\pgm\ETLNEW\date.txt' #sas日期的文件
    sas_file="D:\\Projects\\boc\\pgm\\ETLNEW\\" #sas程序的目录
    sas_log="D:\\Projects\\boc\\logs\\" #sas日志的目录
    phone_num=('1111111111',) #批量失败时发送短信的号码

    card_ftp_ip='' #信用卡FTP服务器地址  
    card_ftp_port='21'   #信用卡FTP服务器端口
    card_ftp_user='s5100000'#信用卡FTP服务器用户
    card_ftp_passwd='11111111'#信用卡FTP服务器密码
    card_file_dir="d:\\card_data\\" #信用卡文本放置目录
    card_sqlldr_data="d:\\temp\card\\"#sqlldr数据文件的目录
    jzbf_ftp_ip='' #集中备份FTP服务器地址  
    jzbf_ftp_port='21'   #集中备份FTP服务器端口
    jzbf_ftp_user=''#集中备份FTP服务器用户
    jzbf_ftp_passwd='' #CRM的FTP服务器密码
    crm_ftp_ip='' #CRM的FTP服务器地址  
    crm_ftp_port='21'   #CRM的FTP服务器端口
    crm_ftp_user=''#CRM的FTP服务器用户
    crm_ftp_passwd=''#CRM的FTP服务器密码
    oracle_ctl_dir='D:\\Projects\\boc\\oracle_ctl\\'#sqlldr的control文件放的目录
    

    backup_tables="'XT_','BIL','ICC'"#需要备份的表
class Sms:
    def SendSms(self,send_message):#发短信
        try:
            servicemanager.LogInfoMsg('发短信：%s'%send_message)
            for phone_num in SasConfig.phone_num:
                try:
                    self.SendSms2Server(send_message,phone_num)
                except Exception,msg:
                    #失败后尝试重新发一次
                    time.sleep(30)
                    self.SendSms2Server(send_message,phone_num)
        except Exception,msg:
            servicemanager.LogErrorMsg('发短信失败，错误信息：%s'%str(msg))

    def SendSms2Server(self,message,phoneNumber):      
        sendMessage=message.decode('GBK').encode('UTF-8')
        fp=open(r'D:\Projects\boc\logs\sms.log','a+')
        b=NPBinding(url='http://xxx:xxx:xxx:xxx:8080/axis/services/SendSms?wsdl',tracefile=fp)
        b.SetAuth(AUTH.httpbasic,'sendsms','')
        b.sendSms(phoneNumber=phoneNumber,message=sendMessage)
        fp.close()
