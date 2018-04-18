'''
File: triangle_count.py
Description: A program that counts the number of triangles from a given graph dataset using SQL queries
Author: Sikder Tahsin Al-Amin
Update: 4/9/18 - initial program, counts the number of triangles from a select statement.
'''

import vertica_python
import string
import sys
import math
import time


######connection string to vertica #####
try:
    conn_info = {'host':'192.168.1.11',
                 'port': 5433,
                 'user': 'vertica',
                 'password': '12512Marlive',
                 'database':'graphdb',
                 'read_timeout': 60000000,
                 'unicode_error': 'strict',
                 'ssl': False,
                 'connection_timeout': 50000
                 }
 
    # simple connection, with manual close
    connection = vertica_python.connect(**conn_info)
    cur = connection.cursor()

 
except:
    print("Database connection error")
    sys.exit()

#sys.argv = ["triangle_count.py", "dataset=web-Google.csv"]


####check the command line arguments ####
if len(sys.argv) != 2:
    print("Not correct arguments. ");
    sys.exit()

arg2=sys.argv[1]
arg2=arg2.split('=')
input_dataset=arg2[1]


###initialize the variables####
file = open("triangle_query.sql","w")
#input_dataset='web-Google.csv'

#####drop table######
def drop_table(table_name):
    sql_string="DROP TABLE IF EXISTS "+table_name+" ;"
    cur.execute(sql_string)
    file.write(sql_string+'\n')
 

####Create E####
drop_table('E')
sql_string="CREATE TABLE E (i int, j int, v int);"
cur.execute(sql_string)
file.write(sql_string+'\n')

####Load Dataset#####
print("Loading the CSV file..")
sql_string="COPY E FROM '/home/vertica/tahsin/graph/"+input_dataset+"' parser fcsvparser();"
cur.execute(sql_string)
file.write(sql_string+'\n')

start_time=time.time()
currnet_time=start_time

#####Counting the number of triangles ######
#drop_table('traingle_count')
print("Counting the trianlges...")
sql_string="SELECT COUNT(*) FROM E e1 JOIN E e2 ON e1.j=e2.i JOIN E e3 ON e2.j=e3.i AND e3.j=e1.i WHERE e1.i<e2.j AND e2.i<e2.j ;" 
cur.execute(sql_string)
total_triangle = cur.fetchone()
file.write(sql_string+'\n')
print("Total Triangles=",total_triangle[0])


print('Total time=',time.time()-start_time)

 
file.close()
connection.close()
