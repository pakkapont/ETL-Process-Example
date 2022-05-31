import os
from numpy import int64
from dotenv import load_dotenv
import pandas as pd
import requests
import pymysql
import pandas_gbq

load_dotenv('.env')

class Config:
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT"))
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
    MYSQL_DB = os.getenv("MYSQL_DB")
    MYSQL_CHARSET = os.getenv("MYSQL_CHARSET")
    GBQ_DESTINATION = os.getenv("GBQ_DESTINATION")
    PROJECT_ID = os.getenv("PROJECT_ID")
    API = os.getenv("API")

#import data from MySQL database
connection = pymysql.connect(host=Config.MYSQL_HOST,
                             port=Config.MYSQL_PORT,
                             user=Config.MYSQL_USER,
                             password=Config.MYSQL_PASSWORD,
                             db=Config.MYSQL_DB,
                             charset=Config.MYSQL_CHARSET,
                             cursorclass=pymysql.cursors.DictCursor)

audible_data = pd.read_sql("SELECT * FROM audible_data", connection)
audible_data = audible_data.set_index("Book_ID")
audible_transaction = pd.read_sql("SELECT * FROM audible_transaction", connection)

transaction = pd.merge(left = audible_transaction, 
					   right = audible_data, 
					   how="left", 
					   left_on="book_id", 
					   right_on="Book_ID")

r = requests.get(Config.API)  #เรียกใช้ REST API มาเก็บไว้ในตัวแปร r
result_conversion_rate = r.json()  #ทำให้รูปแบบของ dictionary
conversion_rate = pd.DataFrame(result_conversion_rate)  #สร้าง dataframe
conversion_rate = conversion_rate.reset_index().rename(columns={"index": "date"})

#join transaction กับ conversion_rate ด้วยวันที่
transaction['date'] = transaction['timestamp']  #ก็อปปี้ column timestamp เก็บเอาไว้ใน column ใหม่ชื่อ date เพื่อที่จะแปลงวันที่เป็น date เพื่อที่จะสามารถนำมา join กับข้อมูลค่าเงินได้
transaction['date'] = pd.to_datetime(transaction['date']).dt.date
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

final_df = pd.merge(left = transaction,
									  right = conversion_rate,
                    how = "left", 
                    left_on = "date", 
                    right_on = 'date')

#เอาเครื่องหมาย $ ออกจาก final_df["Price"]
final_df["Price"] = final_df["Price"].apply(lambda x : x.replace("$",""))

# แปลง final_df["Price"] ให้เป็น float
final_df["Price"] = final_df["Price"].astype(float)

#convert USD tp THB
final_df['THBPrice'] = final_df["Price"]*final_df["conversion_rate"]

#แปลง column data["Audio Runtime"] ที่เป็น string ให้เป็นตัวเลขมีหน่วยเป็นนาที
final_df["hr"] = final_df["Audio Runtime"].str.extract("([0-9]+).[h]")
final_df["mins"] = final_df["Audio Runtime"].str.extract("([0-9]+).[m]")
final_df["hr"] = final_df["hr"].fillna("0")
final_df["mins"] = final_df["mins"].fillna("0")
final_df["hr"] = final_df["hr"].astype(int)
final_df["mins"] = final_df["mins"].astype(int)
final_df["Audio Runtime mins"] = (final_df["hr"]*60) + final_df["mins"]

# เปลี่ยนชื่อ columns
final_df.rename(columns={ "Book Title" : "Book_Title",	
                          "Book Subtitle" : "Book_Subtitle",	
                          "Book Author" : "Book_Author",	
                          "Book Narrator" : "Book_Narrator",	
                          "Audio Runtime" : "Audio_Runtime",
                          "Total No. of Ratings" : "Total_No_of_Ratings", 
                          "Audio Runtime mins" : "Audio_Runtime_mins"}, inplace=True)

#drop column ที่ไม่จำเป็น
final_df = final_df.drop(["date","book_id", "Price","conversion_rate"], axis=1)

#import to Google BigQuery
final_df.to_gbq(destination_table=Config.GBQ_DESTINATION+"example_audible_data_to_gbq" , project_id=Config.PROJECT_ID, if_exists="fail")