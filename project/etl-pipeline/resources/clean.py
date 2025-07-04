from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, lit, current_date, from_json, schema_of_json
from pyspark.sql.types import StringType, IntegerType
import json
import re
import csv

# Tạo SparkSession
spark = SparkSession.builder.appName("Clean User Data").getOrCreate()

# Danh sách tỉnh/thành phố Việt Nam hợp lệ
def load_vn_cities(filename="vn_cities.csv"):
    with open(filename, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row["city"] for row in reader]

def clean_data(raw_json):
    try:
        data = json.loads(raw_json)
        if not isinstance(data, dict):
            raise ValueError("Not a dict")
    except:
        return json.dumps({"id": "N/A", "name": "N/A", "email": "N/A", "phone": "N/A",
                           "gender": "N/A", "age": "N/A", "address": "N/A", "occupation": "N/A"})

    # ID
    data["_id"] = str(data.get("_id", "N/A"))

    # Name
    data["name"] = data.get("name", "N/A")

    # Email
    email = data.get("email", "N/A")
    if email == "N/A" or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        email = "N/A"
    data["email"] = email

    # Phone
    data["phone"] = data.get("phone", "N/A")

    # Gender
    gender = data.get("gender", "N/A")
    if gender.lower() == "boy":
        gender = "Male"
    elif gender.lower() == "girl":
        gender = "Female"
    elif gender not in {"Male", "Female", "Other"}:
        gender = "N/A"
    data["gender"] = gender

    # Age
    try:
        age = int(data.get("age", -1))
        if 0 < age <= 120:
            data["age"] = age
        else:
            data["age"] = "N/A"
    except:
        data["age"] = "N/A"

    # Address
    valid_provinces = load_vn_cities()
    address = data.get("address", "N/A")
    if address == "N/A":
        data["address"] = "N/A"
    elif address.strip() in valid_provinces:
        data["address"] = address.strip()
    else:
        data["address"] = "Invalid"

    # Occupation
    data["occupation"] = data.get("occupation", "N/A")

    return json.dumps(data)

clean_udf = udf(clean_data, StringType())

# Đọc từ MinIO (giả sử file ở dạng CSV)
df = spark.read.option("header", "true").option("multiLine", True).option("escape", "\"").option("mode", "PERMISSIVE").csv("s3a://rawdata/data/user/2025_06_29_1751218226640_1.csv")

# df = spark.read.option("header", "true").csv("s3a://rawdata/data/user/2025_06_29_1751218226640_1.csv")

# Áp dụng hàm cleaning
df.select("_airbyte_data").show(5, truncate=False)
df.selectExpr("typeof(_airbyte_data)").distinct().show()
df_cleaned = df.withColumn("cleaned_data", clean_udf(col("_airbyte_data")))

# Có thể tách các trường ra nếu muốn:
sample_json = df_cleaned.select("cleaned_data").first()["cleaned_data"]
json_schema = schema_of_json(lit(sample_json))

df_final = df_cleaned.withColumn("parsed", from_json("cleaned_data", json_schema)).select("parsed.*")

df_final = df_cleaned.withColumn("dt", current_date())
df_final.select("cleaned_data").show(5, truncate=False)


output_path = "s3a://cleandata/user_cleaned/"
print(f"Output path: {output_path}")
df_final.write.mode("overwrite").partitionBy("dt").parquet(output_path)