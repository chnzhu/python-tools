from ZSI.client import NamedParamBinding as NPBinding
import sys
from ZSI.client import AUTH

b=NPBinding(url='http://21.96.51.66:8080/axis/services/SendSms?wsdl',tracefile=sys.stdout)
b.SetAuth(AUTH.httpbasic,'sendsms','zaq1xsw2')
message='“¯’∆πÒ≤‚ ‘,2001.10'
a=message.decode('GBK').encode('UTF-8')
b.sendSms(phoneNumber='13551384548',message=a)
