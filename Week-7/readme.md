# 📂 Data Lake to MySQL ETL Pipeline with PySpark

This project demonstrates a simple ETL (Extract, Transform, Load) process that reads structured CSV files from a Data Lake-like folder, performs minimal transformations, and loads the data into a MySQL database using PySpark and JDBC.

---

## 📁 Folder Structure

```
Data_Set/
│
└───data_csv/
    ├───CUST_MSTR_YYYYMMDD.csv
    ├───master_child_export-YYYYMMDD.csv
    └───H_ECOM_ORDER_YYYYMMDD.csv
```

---

## 📦 Components

### 1. **CSV Generators**

These scripts generate dummy CSV files using `Faker` for testing:

- `generate_cust_mstr_csv.py`  
- `generate_master_child_csv.py`  
- `generate_h_ecom_order_csv.py`

Each script generates 3 CSV files for different dates and saves them under `Data_Set/data_csv/`.

---

### 2. **Main ETL Script: `load_all_csvs.py`**

This PySpark script:

- Scans the folder `Data_Set/data_csv/` for files.
- Dynamically detects file types by name:
  - `CUST_MSTR_YYYYMMDD.csv` → loads into table `CUST_MSTR`
  - `master_child_export-YYYYMMDD.csv` → loads into table `master_child`
  - `H_ECOM_ORDER_YYYYMMDD.csv` → loads into table `H_ECOM_Orders`
- Extracts date from filename and adds an `Upload_Date` column.
- Truncates existing table content before insert.
- Loads data into MySQL using JDBC.

---

## 🧪 Sample Columns

### ✅ CUST_MSTR_YYYYMMDD.csv
```
CustomerID,CustomerName,CustomerType
```

### ✅ master_child_export-YYYYMMDD.csv
```
MasterID,ChildID,ChildName
```

### ✅ H_ECOM_ORDER_YYYYMMDD.csv
```
OrderID,CustomerID,OrderValue,OrderDate
```

The final `Upload_Date` column is added during transformation.

---

## 💻 Requirements

- Python 3.8+
- PySpark
- Faker
- Java (for JDBC)
- MySQL Server (running locally)

---

## ⚙️ Configuration

Update your MySQL connection in `load_all_csvs.py`:

```python
mysql_url = "jdbc:mysql://localhost:3306/datalakedb"
mysql_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.mysql.cj.jdbc.Driver"
}
```

Make sure MySQL tables are already created with matching column names.

---

## 🚀 How to Run

### 1. Install dependencies
```bash
pip install faker
```

### 2. Generate CSV files
```bash
python generate_cust_mstr_csv.py
python generate_master_child_csv.py
python generate_h_ecom_order_csv.py
```

### 3. Run the ETL pipeline
```bash
spark-submit load_all_csvs.py
```

---

## ✅ MySQL Table Structure

Make sure the following tables exist in the `datalakedb` database:

### Table: `CUST_MSTR`
```sql
CREATE TABLE CUST_MSTR (
    CustomerID VARCHAR(10),
    CustomerName VARCHAR(100),
    CustomerType VARCHAR(20),
    Upload_Date DATE
);
```

### Table: `master_child`
```sql
CREATE TABLE master_child (
    MasterID VARCHAR(10),
    ChildID VARCHAR(10),
    ChildName VARCHAR(100),
    Upload_Date DATE
);
```

### Table: `H_ECOM_Orders`
```sql
CREATE TABLE H_ECOM_Orders (
    OrderID VARCHAR(10),
    CustomerID VARCHAR(10),
    OrderValue FLOAT,
    OrderDate DATE,
    Upload_Date DATE
);
```
