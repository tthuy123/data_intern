from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Push Insight Data to PostgreSQL") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# === 2. Đọc dữ liệu từ MinIO (đã transform và lưu trước đó) ===
dim_user = spark.read.parquet("s3a://insightdata/dim_user/part-00000-461ddbbe-962a-4915-9ff2-26ec1759021c-c000.snappy.parquet")
dim_address = spark.read.parquet("s3a://insightdata/dim_address/part-00000-3a61718e-cf74-4e5d-864d-faae38ea5bbd-c000.snappy.parquet")
dim_occupation = spark.read.parquet("s3a://insightdata/dim_occupation/part-00000-947a6f5b-949a-4ad4-968d-195029f09f06-c000.snappy.parquet")
dim_age_group = spark.read.parquet("s3a://insightdata/dim_age_group/part-00000-b92835c0-79c3-4816-85b3-811c26a9ecfd-c000.snappy.parquet")
fct_user = spark.read.parquet("s3a://insightdata/fct_user/part-00000-de7b79de-57f3-419e-b226-253187c4f60d-c000.snappy.parquet")

# === 3. Cấu hình PostgreSQL JDBC ===
jdbc_url = "jdbc:postgresql://db:5432/user"
jdbc_props = {
    "user": "postgres",
    "password": "example",
    "driver": "org.postgresql.Driver"
}

fct_user.write.jdbc(
    url=jdbc_url,
    table="fct_user",
    mode="overwrite",
    properties=jdbc_props
)

dim_user.write.jdbc(
    url=jdbc_url,
    table="dim_user",
    mode="overwrite",  # hoặc "append"
    properties=jdbc_props
)

dim_address.write.jdbc(
    url=jdbc_url,
    table="dim_address",
    mode="overwrite",
    properties=jdbc_props
)

dim_occupation.write.jdbc(
    url=jdbc_url,
    table="dim_occupation",
    mode="overwrite",
    properties=jdbc_props
)

dim_age_group.write.jdbc(
    url=jdbc_url,
    table="dim_age_group",
    mode="overwrite",
    properties=jdbc_props
)

# fct_user.write.jdbc(
#     url=jdbc_url,
#     table="fct_user",
#     mode="overwrite",
#     properties=jdbc_props
# )

print("Push to PostgreSQL complete.")
