import _mssql
conn = _mssql.connect(server='balt-sql-fc', user='dpw_obc_intranet', password='MO864gEPS%?D', database='DPW_OBC_Prequal')
conn.execute_non_query('CREATE TABLE persons(id INT, name VARCHAR(100))')
conn.execute_non_query("INSERT INTO persons VALUES(1, 'John Doe')")
conn.execute_non_query("INSERT INTO persons VALUES(2, 'Jane Doe')")
conn.execute_query("SELECT * FROM persons")
for row in conn:
    print("ID=", row['id'], "Name=", row['name']) 
conn.close()
