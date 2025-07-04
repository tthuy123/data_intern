from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Read MinIO").getOrCreate()

df = spark.read.csv("s3a://rawdata/data/user/2025_06_29_1751218226640_0.csv", header=True)
df.show()
