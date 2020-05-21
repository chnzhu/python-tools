#-*- coding:UTF-8
import xlrd
import string
xlfile=xlrd.open_workbook(r"D:\temp\wms\scs.xls")
sqlfile=open(r"d:\temp\wms\create_table.sql","w")
sh=xlfile.sheet_by_name("scs")
sysnam='SCS'

old_table=''
index_pk_list=[]

for i in range(1,263):
    tableName=sh.cell_value(i,0)
    col_type=sh.cell_value(i,9).replace(' ','')
    col_name=sh.cell_value(i,4)
    try:
        if col_name[0].isdigit():
            col_name='C'+col_name
    except Exception as error:
        print("err col_name:",str(error))
    ##print(tableName,col_name)
    try:
        col_length=int(sh.cell_value(i,11))
    except Exception as error:
        print('err col_length:',str(error))         
        continue

    if col_length == 0:
        continue       
    if tableName != old_table:
        writeLine='\n);\n'
        sqlfile.write(writeLine)
        if len(index_pk_list) != 0:
            writeLine='alter table %s_%s add constraint pk_%s_%s primary key ('%(sysnam,old_table,sysnam,old_table)
            col_list=''
            for index_pk in index_pk_list:
                col_list =col_list+index_pk+','
            col_list=col_list[:-1]
            writeLine=writeLine+col_list+') using index ;\n'
            sqlfile.write(writeLine)
        index_pk_list=[]
        ##writeLine='drop  table %s_%s ;\n'%(sysnam,tableName)
        ##sqlfile.write(writeLine)
        writeLine='create table %s_%s (\n'%(sysnam,tableName)
        sqlfile.write(writeLine)
    else:
        sqlfile.write(',\n')
    old_table=tableName
   
    if col_name.replace(' ','') == '':
        continue
    ##print col_name
    col_name=col_name.replace('-','_')
    writeLine=col_name

    if col_type == 'CHAR' or col_type == 'VARCHAR2' or col_type == 'TIMESTAMP':
        writeLine=writeLine+'  varchar2(%d)'%col_length
    elif  col_type == 'NUMBER':
        try:
            xs=int(sh.cell_value(i,12))
        except Exception as error:
            ##print("err 小数位数:",str(error))
            xs=0
        if xs == 0:
            writeLine=writeLine+'  number(%d)'%(col_length)
        else:
            writeLine=writeLine+'  number(%d,%d)'%(col_length,xs)
    elif  col_type == 'DECIMAL':
        try:
            xs=int(sh.cell_value(i,12))
        except Exception as error:
            ##print("err 小数位数:",str(error))
            xs=0
        if xs == 0:
            writeLine=writeLine+'  DECIMAL(%d)'%(col_length)
        else:
            writeLine=writeLine+'  DECIMAL(%d,%d)'%(col_length,xs)
    elif  col_type == 'INTEGER':
         writeLine=writeLine+'  INTEGER'
    elif  col_type == 'INT':
         writeLine=writeLine+'  INT'
    elif  col_type == 'BIGINT':
         writeLine=writeLine+'  integer'
    elif  col_type == 'DATETIME':
         writeLine=writeLine+'  varchar2(20)'
    elif  col_type == 'DATE':
         writeLine=writeLine+'  varchar2(10)'
    elif  col_type == 'NVARCHAR2':
         writeLine=writeLine+'  nvarchar2(%d)'%col_length
    else:
        print("err col type:[",col_type,"]")
    if sh.cell_value(i,15) == 'Y':
        index_pk_list.append(col_name)
    sqlfile.write(writeLine)
sqlfile.close()