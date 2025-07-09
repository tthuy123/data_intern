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
            return None
    except:
        return None

    try:
        # Danh sách tỉnh/thành Việt Nam hợp lệ
        vn_cities = load_vn_cities()

        # _id: thiếu thì gán ""
        _id = str(data.get("_id", "") or "")

        # name: thiếu thì gán ""
        name = data.get("name", "") or ""

        # email: bắt buộc phải có
        email = data.get("email", None)
        if not email:
            return None

        # phone: thiếu thì None
        phone = data.get("phone", None)

        # gender: thiếu thì None, nếu có mà sai → drop
        gender = data.get("gender", None)
        if gender:
            gender_l = gender.lower()
            if gender_l == "boy":
                gender = "Male"
            elif gender_l == "girl":
                gender = "Female"
            elif gender_l in {"male", "female", "other", "unknown"}:
                gender = gender_l.capitalize()
            else:
                return None
        else:
            gender = None

        # age: nếu có mà sai định dạng hoặc out-of-range → drop, thiếu thì None
        age = data.get("age", None)
        if age is not None:
            try:
                age = int(age)
                if not (0 < age <= 120):
                    return None
            except:
                return None
        else:
            age = None

        # address: thiếu thì None, nếu có mà không thuộc danh sách tỉnh thì drop
        address = data.get("address", None)
        if address:
            address = address.strip()
            if address not in vn_cities:
                return None
        else:
            address = None

        # occupation: thiếu thì gán ""
        occupation = data.get("occupation", "") or ""

        # Trả về bản ghi đã làm sạch (dưới dạng JSON string)
        clean_record = {
            "_id": _id,
            "name": name,
            "email": email,
            "phone": phone,
            "gender": gender,
            "age": age,
            "address": address,
            "occupation": occupation
        }

        return json.dumps(clean_record)

    except:
        return None



clean_udf = udf(clean_data, StringType())

df = spark.read.option("header", "true").option("multiLine", True).option("escape", "\"").option("mode", "PERMISSIVE").csv("s3a://rawdata/data/user/*.csv")

df_cleaned = df.withColumn("cleaned_data", clean_udf(col("_airbyte_data")))

# filter out rows where cleaned_data is None
df_cleaned = df_cleaned.filter(col("cleaned_data").isNotNull())

sample_json = df_cleaned.select("cleaned_data").first()["cleaned_data"]
json_schema = schema_of_json(lit(sample_json))

df_final = df_cleaned.withColumn("parsed", from_json("cleaned_data", json_schema)).select("parsed.*")
df_final = df_final.withColumn("dt", current_date())

df_final = df_final.dropDuplicates(["email"])

output_path = "s3a://cleandata/user_cleaned/"
print(f"Output path: {output_path}")
df_final.write.mode("overwrite").partitionBy("dt").parquet(output_path)