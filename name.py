import sys,os
from pyspark.sql import SparkSession, types
from pyspark.sql.types import Row


spark = SparkSession.builder.appName("test").getOrCreate()

rdd = spark.read.csv('crime.csv', inferSchema=True, header=True).rdd


def location(data):
	tmp = data.asDict() ## RDD to Dictionary
	loc = map(float,tmp['Location'][1:-1].split(','))
	tmp['Latitude'] = loc[0]
	tmp['Longtitude'] = loc[1]
	del tmp['Location']
	result=Row(**tmp)
	return result

df = rdd.map(location)  #1RDD to 1RDD
(train, val, serving) = df.randomSplit([0.6,0.3,0.1]) # Randomly Split Data
tcsv=spark.createDataFrame(train, samplingRatio=0.1)   #RDD to DataFrame
vcsv=spark.createDataFrame(val, samplingRatio=0.1)
scsv=spark.createDataFrame(serving, samplingRatio=0.1)


tcsv.coalesce(1).write.format('com.databricks.spark.csv').save('output/train',header = 'true')
vcsv.coalesce(1).write.format('com.databricks.spark.csv').save('output/eval',header = 'true')
scsv.coalesce(1).write.format('com.databricks.spark.csv').save('output/serving',header = 'true')


spark.stop()
