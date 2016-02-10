import _mssql
conn = _mssql.connect(server='balt-sql-fc', user='dpw_obc_intranet', password='MO864gEPS%?D', database='DPW_OBC_Prequal')
sub="Naizi"
conn.execute_non_query("INSERT INTO persons(name) VALUES(%s)","12:20:46")
conn.execute_query("SELECT * FROM persons")
for row in conn:
    print("ID=", row['id'], "Name=", row['name']) 
conn.close()
