from pymongo import MongoClient
import csv
import time

client = MongoClient("mongodb://mongo:27017/?replicaSet=rs0")
db = client["project1"]
collection = db["user"]

batch_size = 10000
batch = []

start_time = time.time()
with open("/data/final_user.csv", newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        document = {}

        for key, value in row.items():
            if value.strip() == "":
                continue

            # Nếu là trường "id", gán thành _id
            if key == "id":
                document["_id"] = value  # Nếu muốn ép kiểu ObjectId: ObjectId(value)
            else:
                document[key] = value

        batch.append(document)

        if len(batch) == batch_size:
            try:
                collection.insert_many(batch, ordered=False)
            except Exception as e:
                print("Lỗi khi insert batch:", e)
            batch = []

if batch:
    try:
        collection.insert_many(batch, ordered=False)
    except Exception as e:
        print("Lỗi khi insert batch cuối:", e)

end_time = time.time()
import_time = end_time - start_time

print(f"Import hoàn tất. Thời gian thực thi: {import_time:.2f} giây")