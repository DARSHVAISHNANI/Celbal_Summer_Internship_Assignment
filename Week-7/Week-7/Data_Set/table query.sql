USE project_data;

-- 1️⃣ Customer Master Table
CREATE TABLE CUST_MSTR (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    customer_email VARCHAR(100),
    signup_date DATE,
    filename_date DATE
);

-- 2️⃣ Master Child Table
CREATE TABLE master_child (
    parent_id INT,
    child_id INT,
    relation_type VARCHAR(50),
    filename_date DATE,
    PRIMARY KEY (parent_id, child_id)
);

-- 3️⃣ E-Commerce Orders Table
CREATE TABLE H_ECOM_Orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10,2),
    status VARCHAR(50),
    filename_date DATE
);
