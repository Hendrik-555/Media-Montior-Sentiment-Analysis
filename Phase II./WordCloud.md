from sqlalchemy import create_engine
import pandas as pd
import csv
import psycopg2
import os
import time
import boto3
import numpy as np
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS
from sqlalchemy.inspection import inspect

engine = create_engine('postgresql://Mira:Reddit2022@redditdatalake2.c21awcbm8s9m.us-east-1.rds.amazonaws.com:5432/redditdatalake2')

# IMPORT THE SQALCHEMY LIBRARY's CREATE_ENGINE METHOD
from sqlalchemy import create_engine
  
# DEFINE THE DATABASE CREDENTIALS
user = 'Mira'
password = 'Reddit2022'
host = 'redditdatalake2.c21awcbm8s9m.us-east-1.rds.amazonaws.com'
port = 5432
database = 'redditdatalake2'
  
# PYTHON FUNCTION TO CONNECT TO THE POSTGRESQL DATABASE AND
# RETURN THE SQLACHEMY ENGINE OBJECT
def get_connection():
    return create_engine(
        url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )
  
  
if __name__ == '__main__':
  
    try:
        # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
        engine = get_connection()
        print(
            f"Connection to the {host} for user {user} created successfully.")
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)
        
        
inspector = inspect(engine)
table_names = inspector.get_table_names()
table_names

table_name=str(input("enter table name:  "))
query = "SELECT * FROM " + table_name
misc = pd.read_sql_query(query, engine)
df = pd.DataFrame(misc, columns = ['title', 'dt'])

df.head()

stopwords = set(STOPWORDS)
stopwords.add("russia")
stopwords.add("russian")
stopwords.add("ukrainian")
stopwords.add("ukraine")
stopwords.add("say")
stopwords.add("says")
stopwords.add("will")


np.random.seed(1)

figure, axes = plt.subplots(2, 3, figsize=(18, 14))
cluster_groups = list(i.str.cat() for k, i in df.groupby(['dt'])['title'])
cluster_titles = list(k for k, i in df.groupby(['dt'])['title'])
i = 0
for r in range(2):
    for c in range(3):
        df_cluster = cluster_groups[i]
        wordcloud_image = WordCloud(width = 500, height = 500, 
                background_color ='white', stopwords=stopwords,
                min_font_size = 10).generate(df_cluster) 
        ax = axes[r][c]
        ax.imshow(wordcloud_image,
                  interpolation="bilinear")
        ax.set_title(cluster_titles[i], fontsize=20)
        ax.set_xticks([])
        ax.set_yticks([])
        i = i + 1

plt.subplots_adjust(left=0.07, right=0.93, wspace=0.08, hspace=0.0,top=0.9,bottom=0.09)
plt.tight_layout()
plt.show(20)

# Word clouds for all dates

figure, axes = plt.subplots(50,2, figsize=(18, 100))
cluster_groups = list(i.str.cat() for k, i in df.groupby(['dt'])['title'])
cluster_titles = list(k for k, i in df.groupby(['dt'])['title'])
i = 0
for r in range(50):
    for c in range(2):
        df_cluster = cluster_groups[i]
        wordcloud_image = WordCloud(width = 500, height = 500, 
                background_color ='white', stopwords=stopwords, 
                min_font_size = 10).generate(df_cluster) 
        ax = axes[r][c]
        ax.imshow(wordcloud_image,
                  interpolation="bilinear")
        ax.set_title(cluster_titles[i], fontsize=20)
        ax.set_xticks([])
        ax.set_yticks([])
        i = i + 1

plt.subplots_adjust(left=0.07, right=0.93, wspace=0.08, hspace=0.0,top=0.9,bottom=0.09)
plt.tight_layout()
plt.show()



