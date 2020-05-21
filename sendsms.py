from ZSI.client import NamedParamBinding as NPBinding
import sys
from ZSI.client import AUTH

b=NPBinding(url='http://21.96.51.66:8080/axis/services/SendSms?wsdl',tracefile=sys.stdout)
b.SetAuth(AUTH.httpbasic,'sendsms','')
message='kkkkkkkkk'
a=message.decode('GBK').encode('UTF-8')
b.sendSms(phoneNumber='1111111111',message=a)
