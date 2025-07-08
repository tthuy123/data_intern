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
        # ID & Name: thiếu thì gán "N/A"
        _id = str(data.get("_id", None))
        name = data.get("name", None)

        # Email: thiếu => drop
        email = data.get("email", None)

        # Phone: thiếu thì None
        phone = data.get("phone", None)

        # Gender: nếu có nhưng không hợp lệ => drop, còn nếu thiếu gán Unkown
        gender = data.get("gender", "N/A")
        gender_l = gender.lower()
        if gender_l == "boy":
            gender = "Male"
        elif gender_l == "girl":
            gender = "Female"
        elif gender_l not in {"male", "female", "other", "unknown"}:
            return None

        # Age: nếu sai định dạng (không ép được int hoặc không hợp lệ) => drop, thiếu thì gán -1
        if "age" in data:
            try:
                age = int(data["age"])
                if not (0 < age <= 120):
                    return None
            except:
                return None
        else:
            age = -1

        # Address: nếu có nhưng không thuộc danh sách VN_CITIES thì drop, thiếu thì gán "Unknown"
        vn_cities = load_vn_cities()
        address = data.get("address", "Unknown")
        if address != "Unknown" and address.strip() not in vn_cities:
            return None

        # Occupation: thiếu thì gán "N/A"
        occupation = data.get("occupation", "Unknown")

        clean_record = {
            "_id": _id,
            "name": name,
            "email": email,
            "phone": phone,
            "gender": gender,
            "age": age,
            "address": address.strip(),
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