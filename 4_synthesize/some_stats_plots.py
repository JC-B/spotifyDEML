# coding: utf-8

# In[1]:

from datetime import datetime, timedelta
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import matplotlib.pyplot as plt
import pandas as pd
import io
import boto3

# In[2]:

spark = SparkSession(sc)

# In[3]:

filedate = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d%H%M')

# In[4]:

sqlDF = spark.sql(
    "CREATE OR REPLACE TEMPORARY VIEW albums      USING org.apache.spark.sql.parquet      OPTIONS (      path 's3a://spotifybuck/output/parquet/albums/*/*/*/*/*/'    )")

# In[5]:

sqlDF = spark.sql(
    "CREATE OR REPLACE TEMPORARY VIEW artists      USING org.apache.spark.sql.parquet      OPTIONS (      path 's3a://spotifybuck/output/parquet/artists/*/*/*/*/*/'    )")

# In[7]:

sqlDF = spark.sql(
    "CREATE OR REPLACE TEMPORARY VIEW tracks      USING org.apache.spark.sql.parquet      OPTIONS (      path 's3a://spotifybuck/output/parquet/tracks/*/*/*/*/*/'    )")

# In[8]:

sqlDF = spark.sql(
    "CREATE OR REPLACE TEMPORARY VIEW album_tracks      USING org.apache.spark.sql.parquet      OPTIONS (      path 's3a://spotifybuck/output/parquet/tracks/*/*/*/*/*/'    )")

# In[9]:

sqlDF = spark.sql(
    "CREATE OR REPLACE  TEMPORARY VIEW media_artists      USING org.apache.spark.sql.parquet      OPTIONS (      path 's3a://spotifybuck/output/parquet/media_artists/*/*/*/*/*/*/'    )")

# In[10]:

sqlDF = spark.sql(
    "CREATE OR REPLACE TEMPORARY VIEW media_available_markets      USING org.apache.spark.sql.parquet      OPTIONS (      path 's3a://spotifybuck/output/parquet/media_available_markets/*/*/*/*/*/*/'    )")

# In[14]:

top_10_markets = spark.sql(
    "select mam.market, count(1) AS release_count                   from media_available_markets mam                   join albums a                   on mam.media_id = a.media_id                   and mam.media_type = 'album'                   group by mam.market                   order by 2 desc                   limit 10"
    )

# In[15]:

top_10_markets.write.save("s3n://spotifybuck/output/reporting/data/top10markets" + filedate, format="csv")

# In[16]:

top_10_markets.toPandas().plot(kind='bar')

# In[48]:

img_data = io.BytesIO()

# In[18]:

plt.figure(figsize=(6, 6))

# In[105]:

top_10_markets.toPandas().plot(kind='bar')

# In[19]:

plt.savefig(img_data, format='png')

# In[20]:

img_data.seek(0)

# In[21]:

s3 = boto3.resource('s3')

# In[22]:

bucket = s3.Bucket("spotifybuck")

# In[23]:

bucket.put_object(Body=img_data, ContentType='image/png', Key='/output/reporting/plots/top10markets' + filedate)

# In[29]:

plt.show()

# In[45]:

top_albums = spark.sql(
    "select                            a.name as album_name,                            a.popularity,                             ar.name as artist_name                         from albums a                        join media_artists aa                         on a.media_id = aa.media_id                         and aa.media_type = 'album'                         join artists ar                         on aa.artist_id = ar.artist_id  order by popularity desc limit 10"
    )

# In[46]:

top_albums.show()

# In[47]:

top_albums.write.save("s3n://spotifybuck/output/reporting/data/top10albums" + filedate, format="csv")


# In[ ]:
