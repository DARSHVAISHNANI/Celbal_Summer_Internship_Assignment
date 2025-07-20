import pandas as pd
from sqlalchemy import create_engine, text
import sys

# --- ⚙️ DATABASE CONFIGURATIONS ---
# These should match the parent pipeline's configurations
SOURCE_DB_URL = "mysql+pymysql://root:Biocon%40mysql24@localhost:3306/source_database"
LOCAL_DB_URL = "mysql+pymysql://root:Biocon%40mysql24@localhost:3306/local_database"

def run_product_pipeline(customer_count):
    try:
        print(f"ℹ️ Child pipeline received customer count: {customer_count}")

        # 1. Condition: Copy product data if customer count > 600
        if customer_count > 600:
            print("➡️ Condition met (customer count > 600). Copying product data...")
            source_engine = create_engine(SOURCE_DB_URL)
            local_engine = create_engine(LOCAL_DB_URL)
            
            products_df = pd.read_sql("SELECT * FROM products", source_engine)
            products_df.to_sql('products', local_engine, if_exists='replace', index=False)
            
            print("✅ Successfully copied product data to local DB.")
        else:
            print("⏹️ Condition not met (customer count <= 600). Skipping product data copy.")

    except Exception as e:
        print(f"❌ An error occurred in the product pipeline: {e}")

if __name__ == "__main__":
    # 2. Read the customer count passed as a command-line argument
    if len(sys.argv) > 1:
        try:
            passed_customer_count = int(sys.argv[1])
            run_product_pipeline(passed_customer_count)
        except ValueError:
            print("❌ Error: Invalid customer count passed. Must be an integer.")
    else:
        print("❌ Error: No customer count provided. This script must be called by the parent pipeline.")