# -*- coding: utf-8 -*-
# Copy table data from MSSQL DB to MySQL DB
# USE: Fix config and query, at line 8-23, 26-28, and 40-45
import pymssql
import pymysql

#========= FIX HERE =========#
msdb = pymssql.connect(
    server='MSSQL.SERVER.DOMAIN.OR.IP',
    user='LOGIN_ID',
    password='LOGIN_PASSWORD',
    database='TARGET_DATABASE'
)
mydb = pymysql.connect(
    host='MYSQL.SERVER.DOMAIN.OR.IP',
    port=3306,
    user='LOGIN_ID',
    password='LOGIN_PASSWORD',
    db='TARGET_DATABASE',
    charset='utf8'
)
#============================#

with msdb.cursor() as cur:
#========= FIX HERE =========#
    q = '''SELECT * FROM MSSQL_TARGET_TABLE'''
#============================#
    cur.execute(q)
    table = cur.fetchall()
    cur.close()
msdb.close()

logcnt = len(table)
q=''
try:
    with mydb.cursor() as cur:
        for l in range(0, logcnt, 10000):
            llrange = min(10000, logcnt-l)
#========= FIX HERE =========#
            q = '''INSERT INTO MSSQL_TARGET_TABLE 
            (STRING_COLUMN1, STRING_COLUMN2, INT_COLUMN1)
            VALUES(%s,%s,%s)'''
#============================#
            cur.executemany(q, table[l:l+llrange])
            
            mydb.commit()
        cur.close()
        print('%d/%d'%(l+llrange, logcnt))
except:
    mydb.rollback()
    print('insert failed. rollback')
    print(q)

mydb.close()
print('complete')
