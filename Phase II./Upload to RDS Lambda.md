from sqlalchemy import create_engine
import pandas as pd
import csv
import psycopg2
import os
import time
import boto3

s3 = boto3.client("s3")

ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

def lambda_handler(event, context):
    
    try:
        print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    
    # Auto commit is very important
    conn.set_session(autocommit=True)
    
    #change name of the table from test to smth else 
    cur.execute("CREATE TABLE IF NOT EXISTS reddit(title varchar, num_comment int, score int ,url varchar, subreddit varchar, polarity float,  dt date, tm time, neg float, neu float, pos float, compound float, Sentiment text)")
    
    bucket = "redditcsv" # from bucket
    timestr = time.strftime("%Y%m%d")
    filename = "redditprocessed" + timestr + ".csv" # name of the csv file 
    
    
    obj = s3.get_object(Bucket = bucket, Key = filename)
    content = obj["Body"]
    df = pd.read_csv(content)
    df = df.drop(["Unnamed: 0"], axis=1)

    try:
        engine = create_engine('postgresql://Mira:Reddit2022@redditdatalake2.c21awcbm8s9m.us-east-1.rds.amazonaws.com:5432/redditdatalake2')
        df.to_sql('reddit', engine, if_exists = "append", index=False) #imput name of the table 
    except psycopg2.Error as e:
        print("Error: Inserting Data")
        print (e)
        
    print("upload " + filename + " complete") 
    
    cur.close()
    conn.close()
