import tarfile
import sys
import string
from datetime import date,timedelta

begin_date=date(2014,9,19)
end_date=date(2014,10,31)

while end_date >= begin_date:
    strDate=string.replace(begin_date.isoformat(),'-','')
    tar=tarfile.open('F:\\sas_backup\\sjxc%s.tar'%strDate)
    tar.extract('7886310D.TXP.gz.%s1'%strDate,"f:\\temp\\txp\\")
    tar.close()

    begin_date=begin_date + timedelta(days=1)
