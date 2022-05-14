#!/usr/bin/python
import pymysql
import pandas
import boto3
# Connect to the database
conn = pymysql.connect(host='HOST_URL_FROM_FROM AWS_RDS',
                             user='admin',#default admin if using main account
                             password='PASSWORD',
                             charset='utf8mb4',#default UTF8 
                             database='NAME_OF_DATABASE',#if database is not created then first use the CREATE_COMMAND FOR dB without this database option in conn and after the creation use this to connect to dB
                             cursorclass=pymysql.cursors.DictCursor)

cursor=conn.cursor()

s3 = boto3.resource(   # CONNECTING TO AWS BUCKET 
    service_name='s3',  
    region_name='REGION_NAME(OF BUCKET)',  
    aws_access_key_id='ACCESS_KEY_ID FROM IAM ACCOUNT',
    aws_secret_access_key='SECRET_ACCESS_KEY FROM IAM ACCOUNT'
)

#CREATE_COMMAND FOR dB(if not created execute this statement without the database parameter in conn and comment everything else )
#CREATE DATABASE databasename;

#Create Table Inside databse
# _____creating table (already commit-ed so commented)
# createcommand = ("create table TableName(sno int NOT NULL,name varchar(255),roll_no int,age int,City varchar(255));")

# cursor.execute(createcommand)

#DUMMY_VALUES_________________________________________________________________________________________________________________

sno=[1,2,3]
names=['anyname1','anyname2','anyname3']
rolls=[10,20,30]
age=[14,15,16]
city=['anyplace','anyplace','anyplace']
for i in range(3):
    cursor.execute(f'INSERT INTO TableName VALUES ({sno[i]},"{names[i]}",{rolls[i]},{age[i]},"{city[i]}")')


#______________________________________________________________________________________________________________PRINT AND STORE 

cursor.execute("SELECT * FROM TableName") #to fetch all the data present in the table
result = cursor.fetchall()

#__writing dataframe to bucket
df = pandas.DataFrame(result, columns = ['name','roll_no','age','City'])
df.to_csv('datafile.csv')
s3.Bucket('AWS_BUCKETNAME').upload_file(Filename='datafile.csv', Key='demoupload.csv') #uploading file to the AWS BUCKET IN CSV FORMAT (upload the datafile as demoupload(name))


#CHECK____printing the files in the aws bucket
for obj in s3.Bucket('AWS_BUCKETNAME').objects.all():
    print(obj)