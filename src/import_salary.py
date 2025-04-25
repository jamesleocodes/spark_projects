import mysql.connector
import csv
import os
from getpass import getpass

def main():
    # CSV file path
    csv_file = "data/salary.csv"
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found.")
        exit(1)
    
    # Get MySQL connection info
    print("Connecting to MySQL...")
    host = "localhost"
    user = "user"
    password = "password"
    
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password
        )
        cursor = connection.cursor()
        
        # Create database
        print("Creating database...")
        cursor.execute("CREATE DATABASE IF NOT EXISTS employee_db")
        cursor.execute("USE employee_db")
        
        # Create table
        print("Creating table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS salary (
            id INT,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            salary DECIMAL(10,2),
            department_id INT,
            PRIMARY KEY (id, department_id, salary)
        )
        """)
        
        # Clear existing data
        cursor.execute("TRUNCATE TABLE salary")
        
        # Load data from CSV
        print(f"Loading data from {csv_file}...")
        with open(csv_file, 'r') as f:
            csv_reader = csv.reader(f)
            next(csv_reader)  # Skip header row
            
            # Prepare data for batch insert
            data = []
            for row in csv_reader:
                if len(row) == 5:
                    data.append((
                        int(row[0]),      # id
                        row[1],           # first_name
                        row[2],           # last_name
                        float(row[3]),    # salary
                        int(row[4])       # department_id
                    ))
            
            # Insert data in batches
            insert_query = """
            INSERT INTO salary (id, first_name, last_name, salary, department_id)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, data)
            connection.commit()
            
            print(f"Successfully loaded {len(data)} rows into MySQL")
        
        # Verify data
        cursor.execute("SELECT COUNT(*) FROM salary")
        count = cursor.fetchone()[0]
        print(f"Total records in database: {count}")
        
        cursor.execute("SELECT * FROM salary LIMIT 10")
        print("\nSample data:")
        for row in cursor.fetchall():
            print(row)
        
        # Close connection
        cursor.close()
        connection.close()
        print("Process completed.")
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        exit(1)

if __name__ == "__main__":
    main() 