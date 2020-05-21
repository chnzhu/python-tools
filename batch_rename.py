import cx_Oracle
conn=cx_Oracle.connect("newdss/11111@query570")
curs_table=conn.cursor()
curs_col=conn.cursor()
curs_table.execute("select table_name, table_type   from sys.all_catalog where owner = 'NEWDSS'  and \
                   table_type = 'TABLE'   and table_name  like 'GYJX%'")
ctlfile=open(r"c:\temp\ctl\batch_rename","w")
for row_table in curs_table.fetchall():
    curs_col.execute("select column_name,comments from all_col_comments where owner='NEWDSS' and table_name='%s'"%row_table[0])

    for row_col in curs_col.fetchall():
        write_line='alter table %s rename column %s to "%s";\n'%(row_table[0],row_col[0],row_col [1])
        ctlfile.write(write_line)
ctlfile.close()
conn.close()
  

