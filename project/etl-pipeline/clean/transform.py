from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, monotonically_increasing_id, create_map, coalesce, count, current_timestamp
from pyspark.sql.types import IntegerType
from itertools import chain
import json
from datetime import datetime

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Transform User Data") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

today_str = datetime.today().strftime('%Y-%m-%d')
print(f"Processing data for date: {today_str}")
path = f"s3a://cleandata/user_cleaned/dt={today_str}/*.parquet"
df = spark.read.parquet(path)

# Tạo bảng dim_user
dim_user = df.select(
    col("_id").alias("user_id"),
    col("name"),
    col("email")
).dropDuplicates()

# Tạo bảng dim_address
with open("province_region_map.json", "r", encoding="utf-8") as f:
    province_region_map = json.load(f)

region_map_expr = create_map([lit(x) for x in chain(*province_region_map.items())])

# Tạo dim_address
dim_address = df.select("address").dropna().dropDuplicates() \
    .withColumnRenamed("address", "province") \
    .withColumn("address_id", monotonically_increasing_id()) \
    .withColumn("region", coalesce(region_map_expr.getItem(col("province")), lit("Khác")))

# Nhóm nghề

with open("occupation_category_map.json", "r", encoding="utf-8") as f:
    occupation_category_map = json.load(f)

occupation_map_expr = create_map([lit(x) for x in chain(*occupation_category_map.items())])

dim_occupation = df.select("occupation").dropna().dropDuplicates() \
    .withColumnRenamed("occupation", "occupation_name") \
    .withColumn("occupation_id", monotonically_increasing_id()) \
    .withColumn("category", coalesce(occupation_map_expr.getItem(col("occupation_name")), lit("Khác")))

# Tạo bảng dim_age_group
def age_to_group(age):
    if 0 < age < 18:
        return "Under 18"
    elif age <= 30:
        return "18-30"
    elif age <= 45:
        return "31-45"
    elif age <= 60:
        return "46-60"
    else:
        return "Over 60"

udf_age_group = spark.udf.register("age_group", lambda x: age_to_group(int(x)) if x is not None else "Unknown")

df = df.withColumn("age_group", udf_age_group(col("age")))
dim_age_group = df.select("age_group").distinct() \
    .withColumnRenamed("age_group", "description") \
    .withColumn("age_group_id", monotonically_increasing_id())

# Tạo bảng fct_user
fct_user = df.alias("d") \
    .join(dim_user.alias("u"), col("d._id") == col("u.user_id")) \
    .join(dim_address.alias("a"), col("d.address") == col("a.province"), "left") \
    .join(dim_occupation.alias("o"), col("d.occupation") == col("o.occupation_name"), "left") \
    .join(dim_age_group.alias("ag"), col("d.age_group") == col("ag.description"), "left") \
    .select(
        col("d._id").alias("user_id"),
        col("o.occupation_id"),
        col("a.address_id"),
        col("ag.age_group_id"),
        col("d.gender")
    )



age_insight = fct_user.join(dim_age_group, on="age_group_id", how="left") \
    .groupBy("description") \
    .agg(count("user_id").alias("user_count")) \
    .withColumn("segment_type", lit("age")) \
    .withColumnRenamed("description", "description") \
    .select("segment_type", "description", "user_count")

# 2. Insight theo vùng (region)
region_insight = fct_user.join(dim_address, on="address_id", how="left") \
    .groupBy("region") \
    .agg(count("user_id").alias("user_count")) \
    .withColumn("segment_type", lit("region")) \
    .withColumnRenamed("region", "description") \
    .select("segment_type", "description", "user_count")

# 3. Insight theo nghề (occupation)
occupation_insight = fct_user.join(dim_occupation, on="occupation_id", how="left") \
    .groupBy("category") \
    .agg(count("user_id").alias("user_count")) \
    .withColumn("segment_type", lit("occupation_group")) \
    .withColumnRenamed("category", "description") \
    .select("segment_type", "description", "user_count")

# 4. Insight theo giới tính (gender)
gender_insight = fct_user \
    .groupBy("gender") \
    .agg(count("user_id").alias("user_count")) \
    .withColumn("segment_type", lit("gender")) \
    .withColumnRenamed("gender", "description") \
    .select("segment_type", "description", "user_count")

# Union tất cả lại
user_segment_insight = age_insight.union(region_insight).union(occupation_insight).union(gender_insight)

user_segment_insight = user_segment_insight \
    .withColumn("id", monotonically_increasing_id()) \
    .withColumn("updated_at", current_timestamp()) \
    .select("id", "segment_type", "description", "user_count", "updated_at")
# Ghi ra Parquet
output_path = "s3a://insightdata"

dim_user.write.mode("overwrite").parquet(f"{output_path}/dim_user")
dim_address.write.mode("overwrite").parquet(f"{output_path}/dim_address")
dim_occupation.write.mode("overwrite").parquet(f"{output_path}/dim_occupation")
dim_age_group.write.mode("overwrite").parquet(f"{output_path}/dim_age_group")
fct_user.write.mode("overwrite").parquet(f"{output_path}/fct_user")
user_segment_insight.write.mode("overwrite").parquet(f"{output_path}/user_segment_insight")
print("Transformations completed and data written to MinIO.")

spark.stop()
