import pandas as pd
import os
import re
from sqlalchemy import create_engine, text

# ✅ Setup: Change this to your actual credentials
USERNAME = "root"
PASSWORD = "Biocon%40mysql24"  # Escape '@' by URL encoding as %40 in connection string
HOST = "localhost"
PORT = 3306
DB_NAME = "new_datalake_db"

# ✅ JDBC Connection
DATABASE_URL = f"mysql+pymysql://{USERNAME}:{PASSWORD.replace('@', '%40')}@{HOST}:{PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# ✅ Folder containing your CSVs
DATA_DIR = "Data_Set/data_csv"  # Make sure these files are inside this folder or adjust path

# ✅ Helper to extract date from filename
def extract_date(filename):
    match = re.search(r"\d{8}", filename)
    if match:
        return pd.to_datetime(match.group(), format="%Y%m%d").date()
    return None

# ✅ Load CSV with transformation
def load_csv_to_table(filename, table_name, required_columns):
    file_path = os.path.join(DATA_DIR, filename)
    extracted_date = extract_date(filename)

    df = pd.read_csv(file_path)
    df["Extracted_Date"] = extracted_date

    df = df[required_columns]

    print(f"Inserting into {table_name}...")
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
    print(f"{table_name} ✅ Loaded.")

# ✅ Truncate table before load
def truncate_table(table_name):
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))

def process_all_files():
    truncate_table("CUST_MSTR")
    truncate_table("master_child")
    truncate_table("H_ECOM_Orders")

    load_csv_to_table(
        "CUST_MSTR_20191112.csv",
        "CUST_MSTR",
        ["Customer_ID", "Customer_Name", "Email", "Join_Date", "Extracted_Date"]
    )

    load_csv_to_table(
        "master_child_export-20191114.csv",
        "master_child",
        ["Master_ID", "Child_ID", "Relation_Type", "Extracted_Date"]
    )

    load_csv_to_table(
        "H_ECOM_ORDER_20191114.csv",
        "H_ECOM_Orders",
        ["Order_ID","Product_Name","Quantity","Order_Date","Extracted_Date"]
    )

if __name__ == "__main__":
    process_all_files()