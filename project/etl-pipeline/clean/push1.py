from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
from pyspark.sql.types import IntegerType

# === 1. Tạo SparkSession ===
spark = SparkSession.builder \
    .appName("Push Insight Data to PostgreSQL with Upsert") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# === 2. Đọc dữ liệu từ MinIO ===
dim_user = spark.read.parquet("s3a://insightdata/dim_user/")
dim_user = dim_user.withColumn("user_id", dim_user["user_id"].cast(IntegerType()))

dim_address = spark.read.parquet("s3a://insightdata/dim_address/")
dim_occupation = spark.read.parquet("s3a://insightdata/dim_occupation/")
dim_age_group = spark.read.parquet("s3a://insightdata/dim_age_group/")
fct_user = spark.read.parquet("s3a://insightdata/fct_user/")
fct_user = fct_user.withColumn("user_id", fct_user["user_id"].cast(IntegerType()))


# === 3. JDBC config ===
jdbc_url = "jdbc:postgresql://db:5432/user"
jdbc_props = {
    "user": "postgres",
    "password": "example",
    "driver": "org.postgresql.Driver"
}

# === 4. Ghi vào các bảng staging ===
dim_user.write.jdbc(jdbc_url, "dim_user_staging", mode="overwrite", properties=jdbc_props)
dim_address.write.jdbc(jdbc_url, "dim_address_staging", mode="overwrite", properties=jdbc_props)
dim_occupation.write.jdbc(jdbc_url, "dim_occupation_staging", mode="overwrite", properties=jdbc_props)
dim_age_group.write.jdbc(jdbc_url, "dim_age_group_staging", mode="overwrite", properties=jdbc_props)
fct_user.write.jdbc(jdbc_url, "fct_user_staging", mode="overwrite", properties=jdbc_props)

# === 5. Tạo engine SQLAlchemy để upsert ===
engine = create_engine("postgresql+psycopg2://postgres:example@db:5432/user")

upsert_queries = [
    text("""
        INSERT INTO dim_user (user_id, name, email)
        SELECT user_id, name, email FROM dim_user_staging
        ON CONFLICT (user_id) DO UPDATE SET
            name = EXCLUDED.name,
            email = EXCLUDED.email;
    """),
    text("""
        INSERT INTO dim_address (address_id, province, region)
        SELECT address_id, province, region FROM dim_address_staging
        ON CONFLICT (address_id) DO UPDATE SET
            province = EXCLUDED.province,
            region = EXCLUDED.region;
    """),
    text("""
        INSERT INTO dim_occupation (occupation_id, occupation_name, category)
        SELECT occupation_id, occupation_name, category FROM dim_occupation_staging
        ON CONFLICT (occupation_id) DO UPDATE SET
            occupation_name = EXCLUDED.occupation_name,
            category = EXCLUDED.category;
    """),
    text("""
        INSERT INTO dim_age_group (age_group_id, description)
        SELECT age_group_id, description FROM dim_age_group_staging
        ON CONFLICT (age_group_id) DO UPDATE SET
            description = EXCLUDED.description;
    """),
    text("""
        INSERT INTO fct_user (user_id, occupation_id, address_id, age_group_id, gender)
        SELECT user_id, occupation_id, address_id, age_group_id, gender FROM fct_user_staging
        ON CONFLICT (user_id) DO UPDATE SET
            occupation_id = EXCLUDED.occupation_id,
            address_id = EXCLUDED.address_id,
            age_group_id = EXCLUDED.age_group_id,
            gender = EXCLUDED.gender;
    """)
]

# === 6. Thực thi các câu lệnh UPSERT ===
with engine.begin() as conn:
    for query in upsert_queries:
        conn.execute(query)

print("✅ Upsert to PostgreSQL complete.")
