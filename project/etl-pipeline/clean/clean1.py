from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, current_date, from_json, schema_of_json
from pyspark.sql.types import StringType
import json

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

        # Gender: nếu có nhưng không hợp lệ => drop, còn nếu thiếu => gán "N/A"
        gender = data.get("gender", "N/A")
        gender_l = gender.lower()
        if gender_l == "boy":
            gender = "Male"
        elif gender_l == "girl":
            gender = "Female"
        elif gender_l not in {"male", "female", "other", "Unkown"}:
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

        # Address: nếu có nhưng không thuộc danh sách VN_CITIES thì drop, thiếu thì gán "Unkown"
        address = data.get("address", "Unkown")
        if address != "N/A" and address.strip() not in VN_CITIES:
            return None

        # Occupation: thiếu thì gán "N/A"
        occupation = data.get("occupation", "Unkown")

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
