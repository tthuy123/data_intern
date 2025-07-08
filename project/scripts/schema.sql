-- Bảng người dùng (thông tin cơ bản)
CREATE TABLE dim_user (
    user_id     INT PRIMARY KEY,
    name        VARCHAR(255),
    email       VARCHAR(255) UNIQUE
);

-- Bảng nghề nghiệp
CREATE TABLE dim_occupation (
    occupation_id    INT PRIMARY KEY,
    occupation_name  VARCHAR(255),
    category         VARCHAR(100)
);

-- Bảng độ tuổi
CREATE TABLE dim_age_group (
    age_group_id     INT PRIMARY KEY,
    description      VARCHAR(100)
);

-- Bảng địa chỉ
CREATE TABLE dim_address (
    address_id       INT PRIMARY KEY,
    province         VARCHAR(100),
    region           VARCHAR(50)
);

-- Bảng fact_user: bảng chính chứa khóa ngoại đến các bảng dim
CREATE TABLE fct_user (
    user_id         INT PRIMARY KEY,
    occupation_id   INT,
    address_id      INT,
    age_group_id    INT,
    gender          VARCHAR(20),

    CONSTRAINT fk_user_id       FOREIGN KEY (user_id)      REFERENCES dim_user(user_id),
    CONSTRAINT fk_occupation_id FOREIGN KEY (occupation_id) REFERENCES dim_occupation(occupation_id),
    CONSTRAINT fk_address_id    FOREIGN KEY (address_id)    REFERENCES dim_address(address_id),
    CONSTRAINT fk_age_group_id  FOREIGN KEY (age_group_id)  REFERENCES dim_age_group(age_group_id)
);
