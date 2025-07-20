import pandas as pd
from sqlalchemy import create_engine, text

# --- ⚙️ DATABASE CONFIGURATIONS ---
# Replace with your actual source and local MySQL credentials
SOURCE_DB_URL ="mysql+pymysql://root:Biocon%40mysql24@localhost:3306/source_db"
LOCAL_DB_URL = "mysql+pymysql://root:Biocon%40mysql24@localhost:3306/local_db"

def run_incremental_pipeline():
    """
    Performs a robust incremental load (upsert) of customer data.
    Handles the initial full load and subsequent incremental updates.
    """
    try:
        source_engine = create_engine(SOURCE_DB_URL)
        local_engine = create_engine(LOCAL_DB_URL)
        last_load_timestamp = None

        # 1. Get the last update timestamp from the LOCAL 'customers' table
        try:
            with local_engine.connect() as connection:
                result = connection.execute(text("SELECT MAX(last_updated) FROM customers"))
                last_load_timestamp = result.scalar_one_or_none()
        except Exception:
            last_load_timestamp = None

        # 2. Extract records from SOURCE based on whether it's the first run
        if last_load_timestamp:
            print(f"ℹ️ Last load was at {last_load_timestamp}. Fetching newer records.")
            sql_query = text("SELECT * FROM customers WHERE last_updated > :last_timestamp")
            params = {"last_timestamp": last_load_timestamp}
        else:
            print("ℹ️ No previous load detected. Performing initial full load.")
            sql_query = text("SELECT * FROM customers")
            params = {}
            
        new_data_df = pd.read_sql(sql_query, source_engine, params=params)

        if new_data_df.empty:
            print("✅ Local database is already up-to-date.")
            return

        # 3. CHOOSE LOGIC: Initial Load vs. Incremental Upsert
        with local_engine.connect() as connection:
            with connection.begin(): # Use a transaction for safety
                if last_load_timestamp is None:
                    # ---- THIS IS THE NEW LOGIC FOR THE FIRST RUN ----
                    print(f"➡️ Performing initial load of {len(new_data_df)} records...")
                    new_data_df.to_sql(
                        'customers',
                        con=connection,
                        if_exists='replace', # Creates the table and loads data
                        index=False
                    )
                    print("✅ Initial load completed successfully.")
                else:
                    # ---- THIS IS THE EXISTING LOGIC FOR INCREMENTAL RUNS ----
                    print(f"➡️ Found {len(new_data_df)} new/updated records. Starting upsert process.")
                    # Load to staging table
                    new_data_df.to_sql(
                        'customers_staging',
                        con=connection,
                        if_exists='replace',
                        index=False
                    )

                    # Merge from staging to final table
                    upsert_query = text("""
                        INSERT INTO customers (id, name, email, last_updated)
                        SELECT id, name, email, last_updated FROM customers_staging
                        ON DUPLICATE KEY UPDATE
                            name = VALUES(name),
                            email = VALUES(email),
                            last_updated = VALUES(last_updated);
                    """)
                    connection.execute(upsert_query)
                    print("✅ Upsert process completed successfully.")

    except Exception as e:
        print(f"❌ An error occurred during the pipeline: {e}")

if __name__ == "__main__":
    run_incremental_pipeline()