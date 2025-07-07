from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, current_date, from_json, schema_of_json
from pyspark.sql.types import StringType
import json
import re

# Tạo SparkSession
spark = SparkSession.builder.appName("Clean User Data").getOrCreate()

VN_CITIES = {
    "Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Hải Phòng", "Cần Thơ",
    "An Giang", "Bà Rịa - Vũng Tàu", "Bắc Giang", "Bắc Kạn", "Bạc Liêu",
    "Bắc Ninh", "Bến Tre", "Bình Định", "Bình Dương", "Bình Phước",
    "Bình Thuận", "Cà Mau", "Cao Bằng", "Đắk Lắk", "Đắk Nông",
    "Điện Biên", "Đồng Nai", "Đồng Tháp", "Gia Lai", "Hà Giang",
    "Hà Nam", "Hà Tĩnh", "Hải Dương", "Hậu Giang", "Hòa Bình",
    "Hưng Yên", "Khánh Hòa", "Kiên Giang", "Kon Tum", "Lai Châu",
    "Lâm Đồng", "Lạng Sơn", "Lào Cai", "Long An", "Nam Định",
    "Nghệ An", "Ninh Bình", "Ninh Thuận", "Phú Thọ", "Phú Yên",
    "Quảng Bình", "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị",
    "Sóc Trăng", "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên",
    "Thanh Hóa", "Thừa Thiên Huế", "Tiền Giang", "Trà Vinh", "Tuyên Quang",
    "Vĩnh Long", "Vĩnh Phúc", "Yên Bái"
}

# Hàm làm sạch dữ liệu
def clean_data(raw_json):
    try:
        data = json.loads(raw_json)
        if not isinstance(data, dict):
            raise ValueError("Not a dict")
    except:
        return json.dumps({
            "id": "N/A", "name": "N/A", "email": "N/A", "phone": None,
            "gender": "N/A", "age": -1, "address": "N/A", "occupation": "N/A"
        })

    data["_id"] = str(data.get("_id", "N/A"))
    data["name"] = data.get("name", "N/A")

    # Email
    email = data.get("email", "N/A")
    if email == "N/A" or not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        email = "N/A"
    data["email"] = email

    # Phone
    data["phone"] = data.get("phone", None)
    
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
        data["age"] = age if 0 < age <= 120 else -1
    except:
        data["age"] = -1 # lỗi định dạng hoặc không có

    # Address
    address = data.get("address", "N/A")
    if address.strip() in VN_CITIES:
        data["address"] = address.strip()
    elif address == "N/A":
        data["address"] = "N/A"
    else:
        data["address"] = "Invalid"

    # Occupation
    data["occupation"] = data.get("occupation", "N/A")

    return json.dumps(data)

clean_udf = udf(clean_data, StringType())

df = spark.read.option("header", "true").option("multiLine", True).option("escape", "\"").option("mode", "PERMISSIVE").csv("s3a://rawdata/data/user/*.csv")

df_cleaned = df.withColumn("cleaned_data", clean_udf(col("_airbyte_data")))

sample_json = df_cleaned.select("cleaned_data").first()["cleaned_data"]
json_schema = schema_of_json(lit(sample_json))

df_final = df_cleaned.withColumn("parsed", from_json("cleaned_data", json_schema)).select("parsed.*")
df_final = df_final.withColumn("dt", current_date())

df_final = df_final.dropDuplicates(["email"])

output_path = "s3a://cleandata/user_cleaned/"
print(f"Output path: {output_path}")
df_final.write.mode("overwrite").partitionBy("dt").parquet(output_path)
