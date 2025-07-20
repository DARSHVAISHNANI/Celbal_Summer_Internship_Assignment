import pandas as pd
from sqlalchemy import create_engine, text
import subprocess
import sys

# --- ⚙️ DATABASE CONFIGURATIONS ---
# Replace with your actual source and local MySQL database credentials
SOURCE_DB_URL = "mysql+pymysql://root:Biocon%40mysql24@localhost:3306/source_database"
LOCAL_DB_URL = "mysql+pymysql://root:Biocon%40mysql24@localhost:3306/local_database"

def run_customer_pipeline():
    try:
        source_engine = create_engine(SOURCE_DB_URL)
        local_engine = create_engine(LOCAL_DB_URL)

        # 1. Get customer count from the source database
        with source_engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM customers"))
            customer_count = result.scalar_one()
        print(f"ℹ️ Found {customer_count} customer records in source DB.")

        # 2. Condition: Copy only if record count > 500
        if customer_count > 500:
            print("➡️ Condition met (count > 500). Copying customer data...")
            customers_df = pd.read_sql("SELECT * FROM customers", source_engine)
            customers_df.to_sql('customers', local_engine, if_exists='replace', index=False)
            print("✅ Successfully copied customer data to local DB.")

            # 3. Call the child pipeline and pass the customer count as a parameter
            print("🚀 Calling child (product) pipeline...")
            subprocess.run(
                [sys.executable, "child_product_pipeline.py", str(customer_count)], 
                check=True
            )
        else:
            print("⏹️ Condition not met (count <= 500). Halting process.")

    except Exception as e:
        print(f"❌ An error occurred in the customer pipeline: {e}")

if __name__ == "__main__":
    run_customer_pipeline()