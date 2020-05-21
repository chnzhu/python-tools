import xlrd
import string
import re
import scanf
xlfile=xlrd.open_workbook(r"D:\IT蓝图工作\建表语句\a.xls")
sqlfile=open(r"c:\temp\create_table.sql","w")
sh=xlfile.sheet_by_name("BANCS")
##sh=xlfile.sheet_by_name("BANCSCARD")
nrows = sh.nrows
ncols = sh.ncols
print "nrows %d, ncols %d" % (nrows,ncols)
##for i in range(3,11127):
old_table=''
index_pk_list=[]
oldDownFile=''
##for i in range(3,10295):
for i in range(1,141):
    tableName=string.replace(string.replace(sh.cell_value(i,0),'(',''),')','')
    downFile=string.replace(sh.cell_value(i,2),' ','')
    if downFile == '' :
        downFile=oldDownFile
    else:
        oldDownFile=downFile
    if tableName == 'GECT' or tableName == 'INCT' or tableName == 'BOCT':
        tableName=tableName+downFile[-3:]
        print tableName
    col_type=sh.cell_value(i,13)
    col_name=string.replace(string.replace(sh.cell_value(i,5),'(',''),')','')
    try:
        if col_name[0].isdigit():
            col_name='C'+col_name
    except Exception,msg:
        print msg
    print tableName,col_name
    try:
        col_length=int(sh.cell_value(i,14))
    except Exception,msg:
        continue
    tran_flag=sh.cell_value(i,7)
    try:
        col_copybook=str(sh.cell_value(i,10))
    except Exception,msg:
        col_copybook=''
        print col_name
        print msg
    

    if col_length == 0:
        continue       
    if tableName != old_table:
        writeLine='\n);\n'
        sqlfile.write(writeLine)
        if len(index_pk_list) != 0:
            writeLine='alter table bancs_%s add constraint pk_%s primary key ('%(old_table,old_table)
            col_list=''
            for index_pk in index_pk_list:
                col_list =col_list+index_pk+','
            col_list=col_list[:-1]
            writeLine=writeLine+col_list+') using index ;\n'
            sqlfile.write(writeLine)
        index_pk_list=[]
        writeLine='drop  table bancs_%s ;\n'%tableName
        sqlfile.write(writeLine)
        writeLine='create table bancs_%s (\n'%tableName
        sqlfile.write(writeLine)
    else:
        sqlfile.write(',\n')
    old_table=tableName
   
    if string.replace(col_name,' ','') == '':
        continue
    ##print col_name
    col_name=string.replace(string.replace(col_name,' ',''),'-','_')
    writeLine=col_name

    if col_type == 'CHAR' or col_type == 'char' or tran_flag=='A' or tran_flag=='D' or tran_flag=='C' or tran_flag=='T' \
       or tran_flag=='L' or tran_flag=='B':
        writeLine=writeLine+'  varchar2(%d)'%col_length
    else:
        #判断数字的小数位数
        p=re.compile("\.9\([0-9]*\)")
        m=p.findall(col_copybook)
        if len(m) == 0:
            writeLine=writeLine+'  number(%d)'%col_length
        elif len(m) == 1:
            point_length=scanf.sscanf(m[0],".9(%d)")
            writeLine=writeLine+'  number(%d,%d)'%(col_length,point_length[0])
        else:
            print col_name
            print "read copybook err"
            continue
    if sh.cell_value(i,16) == 'Y':
        index_pk_list.append(col_name)
    sqlfile.write(writeLine)
sqlfile.close()