from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

HADOOP_HOME= 'C:\hadoop'

spark = (SparkSession
	     .builder
		 .appName("streamingSpark")
		 .getOrCreate()
		 )
spark.sparkContext.setLogLevel("ERROR")

lines = (spark.readStream
	     .format("socket")
	     .option("host", "localhost")
	     .option("port", 22222)
	     .load()
	     )

words = lines.select(
 	explode(split(lines.value, " "))
    .alias("word")
)

wordCounts = words.groupBy('word').count()

writer = (lines
	      .writeStream
	      .format("console")
	      .outputMode("complete")
)


streamer = writer.start()


streamer.awaitTermination()