import csv
import random
import re
from faker import Faker
import argparse
from datetime import datetime, timedelta

fake = Faker('vi_VN')

ho_list = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Đặng"]
ten_dem_nam = ["Văn", "Hữu", "Đức", "Công", "Quang"]
ten_dem_nu = ["Thị", "Ngọc", "Thu", "Mai", "Diệu"]

occupations = [
    "Kỹ sư phần mềm", "Giáo viên", "Bác sĩ", "Nông dân", "Kế toán",
    "Nhân viên bán hàng", "Sinh viên", "Y tá", "Công an", "Công nhân xây dựng"
]

valid_genders = ["Male", "Female", "Other"]

def load_vn_cities(filename="vn_cities.csv"):
    try:
        with open(filename, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [row["city"] for row in reader]
    except FileNotFoundError:
        print(f"Không tìm thấy file: {filename}")
        return ["Hà Nội", "TP HCM", "Đà Nẵng"]

def generate_vn_name(gender):
    ho = random.choice(ho_list)
    ten_dem = random.choice(ten_dem_nam if gender == "Male" else ten_dem_nu if gender == "Female" else ten_dem_nam + ten_dem_nu)
    ten = fake.name()
    return f"{ho} {ten_dem} {ten}"

def generate_vn_email(name):
    name = name.lower()
    name = re.sub(r"[àáảãạăắằẳẵặâầấẩẫậ]", "a", name)
    name = re.sub(r"[èéẻẽẹêềếểễệ]", "e", name)
    name = re.sub(r"[ìíỉĩị]", "i", name)
    name = re.sub(r"[òóỏõọôồốổỗộơờớởỡợ]", "o", name)
    name = re.sub(r"[ùúủũụưừứửữự]", "u", name)
    name = re.sub(r"[ỳýỷỹỵ]", "y", name)
    name = re.sub(r"đ", "d", name)
    name = re.sub(r"\s+", ".", name)
    return name + "@gmail.com"

def generate_vn_phone():
    prefixes = ["03", "05", "07", "08", "09"]
    prefix = random.choice(prefixes)
    number = f"{prefix}{random.randint(10000000, 99999999)}"
    return f"{number[:4]} {number[4:7]} {number[7:]}"

def generate_register_date():
    random_days = random.randint(0, 5 * 365)
    return (datetime.now() - timedelta(days=random_days)).strftime("%d/%m/%Y")

def generate_user(user_id, cities, error_type=None):
    gender = random.choice(valid_genders)
    name = generate_vn_name(gender)
    email = generate_vn_email(name)
    phone = generate_vn_phone()
    age = random.randint(0, 120)
    address = random.choice(cities)
    occupation = random.choice(occupations)
    register_date = generate_register_date()

    if error_type == "bad_gender":
        gender = random.choice(["", "malee", "??", "123", "unknow", 0, -1])
    elif error_type == "bad_age":
        age = random.choice(["", "abc", -5, 999, "mười tám"])
    elif error_type == "missing_fields":
        if random.choice([True, False]):
            address = ""
        else:
            occupation = ""
    elif error_type == "bad_register_date":
        register_date = random.choice([
            "", "32/13/2025", "2025-07-04", "Ngày 4 tháng 7", "abc"
        ])
    return {
        "id": user_id,
        "name": name,
        "email": email,
        "phone": phone,
        "gender": gender,
        "age": age,
        "address": address,
        "occupation": occupation,
        "register_date": register_date
    }

def generate_users(output_file, city_file, valid=0, bad_gender=0, bad_age=0, missing_fields=0, duplicate=0, bad_register_date=0, multi_error=0):
    cities = load_vn_cities(city_file)
    total = valid + bad_gender + bad_age + missing_fields + duplicate + bad_register_date + multi_error
    all_users = []
    user_id = 1
    email_list = []

    for _ in range(valid):
        user = generate_user(user_id, cities)
        all_users.append(user)
        email_list.append(user["email"])
        user_id += 1

    for _ in range(bad_gender):
        all_users.append(generate_user(user_id, cities, error_type="bad_gender"))
        user_id += 1

    for _ in range(bad_age):
        all_users.append(generate_user(user_id, cities, error_type="bad_age"))
        user_id += 1

    for _ in range(missing_fields):
        all_users.append(generate_user(user_id, cities, error_type="missing_fields"))
        user_id += 1

    for _ in range(duplicate):
        user = generate_user(user_id, cities)
        if email_list:
            user["email"] = random.choice(email_list)
        all_users.append(user)
        user_id += 1

    # Multi-error: apply multiple issues manually
    possible_errors = ["bad_gender", "bad_age", "missing_fields", "bad_register_date"]
    for _ in range(multi_error):
        gender = random.choice(valid_genders)
        name = generate_vn_name(gender)
        email = generate_vn_email(name)
        phone = generate_vn_phone()
        age = random.randint(0, 120)
        address = random.choice(cities)
        occupation = random.choice(occupations)
        register_date = generate_register_date()

        selected_errors = random.sample(possible_errors, random.randint(2, len(possible_errors)))
        if "bad_gender" in selected_errors:
            gender = random.choice(["", "malee", "??", "123", "unknow", 0, -1])
        if "bad_age" in selected_errors:
            age = random.choice(["", "abc", -5, 999, "mười tám"])
        if "missing_fields" in selected_errors:
            if random.choice([True, False]):
                address = ""
            else:
                occupation = ""
        if "bad_register_date" in selected_errors:
            register_date = random.choice([
                "", "32/13/2025", "2025-07-04", "Ngày 4 tháng 7", "abc"
            ])

        user = {
            "id": user_id,
            "name": name,
            "email": email,
            "phone": phone,
            "gender": gender,
            "age": age,
            "address": address,
            "occupation": occupation,
            "register_date": register_date
        }
        all_users.append(user)
        user_id += 1

    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "email", "phone", "gender", "age", "address", "occupation", "register_date"])
        writer.writeheader()
        for row in all_users:
            writer.writerow(row)

    print(f" Đã sinh tổng cộng {total} bản ghi:")
    print(f"    Valid: {valid}")
    print(f"    Bad gender: {bad_gender}")
    print(f"    Bad age: {bad_age}")
    print(f"    Missing fields: {missing_fields}")
    print(f"    Duplicate email: {duplicate}")
    print(f"    Bad register date: {bad_register_date}")
    print(f"    Multi-error: {multi_error}")
    print(f" File xuất: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sinh dữ liệu người dùng Việt Nam có lỗi theo kịch bản")
    parser.add_argument("--valid", type=int, default=0, help="Số bản ghi hợp lệ")
    parser.add_argument("--bad-gender", type=int, default=0, help="Số bản ghi lỗi giới tính")
    parser.add_argument("--bad-age", type=int, default=0, help="Số bản ghi lỗi tuổi")
    parser.add_argument("--missing-fields", type=int, default=0, help="Số bản ghi thiếu address hoặc occupation")
    parser.add_argument("--duplicate", type=int, default=0, help="Số bản ghi bị trùng email")
    parser.add_argument("--bad-register-date", type=int, default=0, help="Số bản ghi lỗi ngày đăng ký")
    parser.add_argument("--multi-error", type=int, default=0, help="Số bản ghi có nhiều lỗi ngẫu nhiên")
    parser.add_argument("-o", "--output", type=str, default="users.csv", help="File đầu ra")
    parser.add_argument("-c", "--cityfile", type=str, default="vn_cities.csv", help="File chứa danh sách tỉnh/thành")

    args = parser.parse_args()

    generate_users(
        output_file=args.output,
        city_file=args.cityfile,
        valid=args.valid,
        bad_gender=args.bad_gender,
        bad_age=args.bad_age,
        missing_fields=args.missing_fields,
        duplicate=args.duplicate,
        bad_register_date=args.bad_register_date,
        multi_error=args.multi_error
    )

# python generate_users.py \
#   --valid 700000 \
#   --bad-gender 60000 \
#   --bad-age 60000 \
#   --missing-fields 60000 \
#   --duplicate 60000 \
#   --multi-error 60000 \
#   -o users_1m.csv \
#   -c vn_cities.csv
