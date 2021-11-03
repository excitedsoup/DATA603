#!/usr/bin/env python
# coding: utf-8

# In[20]:
import time
import geopandas as gpd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, MapType

schema = StructType([
    StructField('message', StringType()),
    StructField('timestamp', StringType()),
    StructField('iss_position', MapType(StringType(), StringType(), False), False)])

spark = (SparkSession
	     .builder
		 .appName("streamingSpark")
		 .getOrCreate()
		 )
spark.sparkContext.setLogLevel("ERROR")

lines = (spark.readStream
	     .format("socket")
	     .option("host", "localhost")
	     .option("port", 22223)
	     .load()
	     )

iss = (((lines.select(from_json(col('value'), schema)
        .alias('position'))
        .select('position.*'))
        .select('iss_position'))
        .withColumn('latitude', F.col('iss_position').getItem('latitude'))
        .withColumn('longitude', F.col('iss_position').getItem('longitude')))

writer = (iss
	      .writeStream
	      .format("memory")
          .queryName('latlong')
          #.format('console')
	      .outputMode("append")
)



streamer = writer.start()

#while streamer.isActive:
#    time.sleep(3)
#    spark.sql('select latitude, longitude from latlong').show()


#plt.plot(

streamer.awaitTermination(timeout=3600)

df = spark.sql('select latitude, longitude from latlong').toPandas()
gdf = gpd.GeoDataFrame(df, geometry = gpd.points_from_xy(df.longitude, df.latitude))

world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
fig, ax = plt.subplots()
ax.set_aspect('equal')

world.plot(ax = ax, color = 'white', edgecolor = 'black')
gdf.plot(ax = ax, marker = 'o', color = 'red', markersize = 5)

plt.savefig('plot.pdf')