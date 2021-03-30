import json
import os
import snowflake.connector
import pandas as pd
from time import sleep
import logging 
from os import path






logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename= os.path.join("dimension.log"),
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

level               = data['level']
course_id           = data['course_id']
course_title        = data['course_title']
is_paid             = data['is_paid']
price               = data['price']
num_subscribers     = data['num_subscribers']
num_reviews         = data['num_reviews']
num_lectures        = data['num_lectures']
content_duration    = data['content_duration']
published_timestamp = data['published_timestamp']
subject             = data['subject']
author              = data['author']



level2               = []
course_id2           = [] 
course_title2        = []   
is_paid2             = [] 
price2               = [] 
num_subscribers2     = []   
num_reviews2         = []    
num_lectures2        = [] 
content_duration2    = [] 
published_timestamp2 = [] 
subject2             = [] 
author2              = [] 


try:
    list_num = 0


    curs=conn.cursor()
    curs.execute("USE ROLE SYSADMIN")                


    
    for key in level:
        
        course_id2.append(course_id [key])
        course_title2.append(course_title[key])
        is_paid2.append(is_paid[key])
        price2.append(price[key]) 
        num_subscribers2.append(num_subscribers[key]) 
        num_reviews2.append(num_reviews[key])
        num_lectures2.append(num_lectures[key])
        level2.append(level[key])
        content_duration2.append(content_duration[key]) 
        published_timestamp2.append(published_timestamp[key]) 
        subject2.append(subject[key]) 
        author2.append(author[key])


        x1 = course_id2[list_num]
        x2 = course_title2[list_num]    
        x3 = is_paid2[list_num]
        x4 = price2[list_num]
        x5 = num_subscribers2[list_num]
        x6 = num_reviews2[list_num]
        x7 = num_lectures2[list_num]
        x8 = level2[list_num]
        x9 = content_duration2[list_num]
        x10 = published_timestamp2[list_num]
        x11 = subject2[list_num]
        x12 = author2[list_num]




        sql = "INSERT INTO PUBLIC.COURSE_DIM (COURSE_ID,SUBJECT,COURSE_TITLE,NUM_REVIEWS,NUM_LECTURES,CONTENT_DURATION,LEVEL) VALUES " + f"('{x1}', '{x11}','{x2}','{x6}','{x7}','{x9}','{x8}')"
        curs.execute( sql )
        print("inserted in to Course table",list_num)
        list_num = list_num +1
    

except:
       print("course  didn't insert to table")
       logging.info("course  didn't insert to table") 






x = author2
             
try:

    list_num = 0


    curs1=conn.cursor()
    curs1.execute("USE ROLE SYSADMIN")     
    
    for key in x:


        
        authordetails = key.split("_", 2)
        a1 = authordetails[0]
        a2 = authordetails[1]
        a3 = authordetails[2]
        


        sql1 = "INSERT INTO PUBLIC.AUTHOR_DIM (AUTHOR_ID,AUTHOR_FNAME,AUTHOR_LNAME2) VALUES " + f"('{a3}', '{a1}','{a2}')"
        curs1.execute( sql1 )

    
        print("inserted in to Author table",list_num)
        list_num = list_num +1

except:
       print("Connection failed. Auothor details didn't  inseted in to table")
       logging.info("Connection failed. Auothor details didn't  inseted in to table") 

y1  =  is_paid2
y2  =  price2   


try:

    list_num = 0


    curs2=conn.cursor()
    curs2.execute("USE ROLE SYSADMIN")  

    
    for key2 in y1:

        p1 = key2
        p2 = y2[list_num]
        p3 = "pay_id" + str(list_num)


        sql2 = "INSERT INTO PUBLIC.PAYMENT_DIM (PAYID,IS_PAID,PRICE) VALUES " + f"('{p3}', '{p1}','{p2}')"
        curs2.execute( sql2 )
    
        print("inserted in to Payment table",list_num)

        list_num = list_num +1

  
except:
       print("Connection failed. payment details  did'nt inserted") 
       logging.info("Connection failed. payment details  did'nt inserted") 




j = published_timestamp2
              
try:

    list_num = 0


    curs6=conn.cursor()
    curs6.execute("USE ROLE SYSADMIN")     
    
    for key in j:


        
        authordetails = key.split("-", 2)

        j1 = authordetails[0]
        j2 = authordetails[1]
        j3 =  "Date_id" + str(list_num)
        #print(a1)
        #print(a2)
        #print(a3)

    


        
        sql6 = "INSERT INTO PUBLIC.DATE_DIM (DATE_ID,YEAR,MONTH) VALUES " + f"('{j3}', '{j1}','{j2}')"

        
        curs6.execute( sql6)

    
        print("inserted in to date table",list_num)
        list_num = list_num +1


except:
       print("date values didn't inseted in to table ") 
       logging.info("date values didn't inseted in to table")
          

















      













    






