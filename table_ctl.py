import cx_Oracle
conn=cx_Oracle.connect("bancs/wert45@query570")
curs_table=conn.cursor() 
curs_col=conn.cursor()
curs_table.execute("select table_name, table_type   from sys.all_catalog where owner = 'BANCS'  and \
                   table_type = 'TABLE'   and table_name  like 'RLMS%'")
for row_table in curs_table.fetchall():
    curs_col.execute("select to_char(max(column_id)) from sys.all_tab_columns where table_name='%s'"%row_table[0])
    row_max=curs_col.fetchone()
    maxid=int(row_max[0]) 
    curs_col.execute("select column_name,data_type,column_id,data_length from sys.all_tab_columns where owner='BANCS' and table_name='%s' \
                  order by column_id"%row_table[0])
    ctlfile=open(r"d:\temp\rlms\%s.ctl"%row_table[0],"w")
    ctlfile.write('LOAD DATA\n')
    ctlfile.write('CHARACTERSET zhs16GBK\n')
    ctlfile.write('append\n')
    ctlfile.write('into table %s\n'%row_table[0])
    ctlfile.write("FIELDS TERMINATED BY ' | '\n")
    ctlfile.write('(\n')
 #   ctlfile.write('filler_1 filler,\n')
 #   ctlfile.write('filler_2 filler,\n')
    for row_col in curs_col.fetchall():
        write_line='%s  '%row_col[0]
        ##if row_table[0] == 'BANCS_BOIS' and row_col[0]=='BOIS_LAST_MAINT_DATE1' :
        ##    write_line = write_line + ' TERMINATED BY WHITESPACE '
        ##if row_table[0] == 'BANCS_DEPP' and row_col[0]=='FILLER2' :
        ##    write_line = write_line + ' TERMINATED BY WHITESPACE '
        ##if row_col[1] == 'VARCHAR2':
        ##    write_line = write_line+'"trim(:%s)"'%row_col[0]
        ##if row_col[1] == 'CHAR':
        if row_col[3] > 200 :
            write_line = write_line+' char(%d) '%(row_col[3]+100)
        write_line = write_line+'"trim(:%s)"'%row_col[0]
        
        #if row_col[3] < 1000:
        #    write_line = write_line+'"trim(:%s)"'%row_col[0]
        #else:
        #    write_line = write_line+'"substr(trim(:%s),1,%d)"'%(row_col[0],row_col[3])
        if int(row_col[2]) == maxid :
            write_line = write_line+')\n'
        else:
            write_line = write_line+',\n'
        ctlfile.write(write_line)
    ctlfile.close()
conn.close()
  

