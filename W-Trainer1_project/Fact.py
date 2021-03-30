import json
import os
import snowflake.connector
import pandas as pd
from time import sleep
import logging 
from os import path



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename= os.path.join("fact.log"),
                    filemode='a')

conn=snowflake.connector.connect(
      user= 'uvindu',
      password= 'Kalutara12345',
      account= 'hv41099.ap-south-1',
      warehouse= 'COMPUTE_WH',
      database=  'WILEY_DB',
      schema='PUBLIC'
                )



f = open('course_data.json',)
data = json.load(f)

num_subscribers     = data['num_subscribers']
num_subscribers2     = []   
  
for key in num_subscribers :
   num_subscribers2.append(num_subscribers[key]) 





try:
    curs1=conn.cursor()
    curs1.execute("USE ROLE SYSADMIN") 
    sql1 =  'SELECT PAYID  from "WILEY_DB"."PUBLIC"."PAYMENT_DIM" '
    curs1.execute( sql1 )
    cnewlist1 = [x for x in curs1]  
    
except:
       print("Connection failed. Auothor details didn't  inseted in to table")
       logging.info("Connection failed. Auothor details didn't  inseted in to table")        



try:
    curs2=conn.cursor()
    curs2.execute("USE ROLE SYSADMIN") 
    sql2 =  'SELECT AUTHOR_ID  from "WILEY_DB"."PUBLIC"."AUTHOR_DIM";'
    curs2.execute( sql2 )
    cnewlist2 = [y for y in curs2]  
 

except:
       print("Connection failed. Auothor details didn't  inseted in to table") 
       logging.info("Connection failed. Auothor details didn't  inseted in to table")    




try:
    curs3=conn.cursor()
    curs3.execute("USE ROLE SYSADMIN") 
    sql3 =  'SELECT DATE_ID from "WILEY_DB"."PUBLIC"."DATE_DIM";'
    curs3.execute( sql3 )
    cnewlist3 = [y for y in curs3]  
 

 
except:
       print("Connection failed. Auothor details didn't  inseted in to table")
       logging.info("Connection failed. Auothor details didn't  inseted in to table")    


try:

    curs4=conn.cursor()
    curs4.execute("USE ROLE SYSADMIN") 
    sql4 =  'SELECT COURSE_ID from "WILEY_DB"."PUBLIC"."COURSE_DIM";'
    curs4.execute( sql4 )
    cnewlist4 = [y for y in curs4]  
  

except:
       print("Connection failed. Auothor details didn't  inseted in to table")
       logging.info("Connection failed. Auothor details didn't  inseted in to table")      





k = num_subscribers2

num = 0  

u = int(num)
curs5=conn.cursor()
curs5.execute("USE ROLE SYSADMIN")     
    
    
for x in cnewlist1:

    curs5=conn.cursor()
    curs5.execute("USE ROLE SYSADMIN")     

    pay    = cnewlist1[num][0]
    auth   = cnewlist2[num][0]
    date   = cnewlist3[num][0]
    course = cnewlist4[num][0]
    sub    = k[num]

    sub = int(sub)
    course = int(course)
    date  = str(date)
    pay = str(pay)
    auth  = str(auth)
        
          
    
        
    sql5 = "INSERT INTO PUBLIC.SALES(NUM_SUBSCRIBERS,COURSE_ID,PAYID,AUTHOR_ID,DATE_ID) VALUES " + f"('{sub}','{course}','{pay}','{auth}','{date}')"

    curs5.execute( sql5 )

    
    print("inserted in to sales_fact table",num)
   
    num = num+1 


conn.close()     

     










     


