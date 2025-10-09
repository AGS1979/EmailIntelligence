import sqlite3
import csv
import os

# --- Configuration: Make sure these filenames match yours ---
DB_FILE = "Master_Company_List.db"
CSV_FILE = "companies_FINAL_CLEAN.csv"
TABLE_NAME = "master_companies"

def import_csv_to_db():
    """
    Reads data from a CSV file and inserts it into an SQLite database table.
    """
    # --- Safety Check: Ensure the required files exist ---
    if not os.path.exists(DB_FILE):
        print(f"❌ ERROR: Database file not found at '{os.path.abspath(DB_FILE)}'")
        return
    if not os.path.exists(CSV_FILE):
        print(f"❌ ERROR: CSV file not found at '{os.path.abspath(CSV_FILE)}'")
        return

    try:
        # --- 1. Connect to the database ---
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        print(f"✅ Connected to database: '{DB_FILE}'")

        # --- 2. Clear any old data from the table (to prevent errors on re-runs) ---
        cursor.execute(f"DELETE FROM {TABLE_NAME};")
        print(f"🧹 Cleared existing data from table: '{TABLE_NAME}'")

        # --- 3. Open and read the clean CSV file ---
        with open(CSV_FILE, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            
            # Skip the header row
            header = next(csv_reader)
            
            # Prepare the data for insertion
            data_to_insert = [tuple(row) for row in csv_reader if len(row) == 6]

        # --- 4. Insert all data in a single, efficient transaction ---
        sql = f"INSERT INTO {TABLE_NAME} (short_name, ticker, company_id, country, sector, sub_industry) VALUES (?, ?, ?, ?, ?, ?)"
        cursor.executemany(sql, data_to_insert)
        
        # --- 5. Commit the changes to the database ---
        conn.commit()
        print(f"🚀 Success! {cursor.rowcount} rows were imported into '{TABLE_NAME}'.")

    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        # --- 6. Close the connection ---
        if conn:
            conn.close()
            print("✅ Database connection closed.")

# --- Run the import function ---
if __name__ == "__main__":
    import_csv_to_db()