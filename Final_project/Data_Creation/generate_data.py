def generate_insert_script(num_records=605, output_file='source_database.sql'):
    """
    Generates a SQL script to insert a specified number of customer records.
    """
    try:
        with open(output_file, 'w') as f:
            f.write("USE source_database;\n")
            # Clear out old data to ensure a fresh start
            f.write("TRUNCATE TABLE customers;\n\n")

            # Start the INSERT statement
            f.write("INSERT INTO customers (id, name, email) VALUES\n")

            # Generate value tuples
            for i in range(1, num_records + 1):
                name = f"User {i}"
                email = f"user{i}@example.com"
                
                # Write the value tuple, with a comma or semicolon at the end
                if i < num_records:
                    f.write(f"    ({i}, '{name}', '{email}'),\n")
                else:
                    # Last record gets a semicolon
                    f.write(f"    ({i}, '{name}', '{email}');\n")
        
        print(f"✅ Successfully created '{output_file}' with {num_records} records.")
        print("   -> Now, run this SQL file in your MySQL client.")

    except Exception as e:
        print(f"❌ An error occurred: {e}")

if __name__ == "__main__":
    generate_insert_script()