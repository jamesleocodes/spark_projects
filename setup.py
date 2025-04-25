#!/usr/bin/env python3

import os
import sys
import subprocess
import platform

def print_header(message):
    print("\n" + "=" * 60)
    print(f" {message}")
    print("=" * 60)

def install_requirements():
    print_header("Installing Python Requirements")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Requirements installed successfully")
    except subprocess.CalledProcessError:
        print("❌ Failed to install requirements. Please install them manually.")
        print("   Use: pip install -r requirements.txt")
        return False
    
    return True

def check_spark_installation():
    print_header("Checking Apache Spark Installation")
    try:
        # Import PySpark to check if it's installed
        import pyspark
        print(f"✅ PySpark is installed (version: {pyspark.__version__})")
        
        # Try to initialize a SparkSession to verify the installation
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("SparkSetupTest").master("local[1]").getOrCreate()
        spark.stop()
        print("✅ Successfully created a Spark session")
        
        return True
    except ImportError:
        print("❌ PySpark is not installed correctly.")
        print("   Please make sure Apache Spark is installed properly.")
        return False
    except Exception as e:
        print(f"❌ Error initializing Spark: {e}")
        return False

def verify_data_files():
    print_header("Verifying Data Files")
    
    # Check for salary data
    salary_data_path = os.path.join("data", "salary.csv")
    if os.path.exists(salary_data_path):
        print(f"✅ Found salary data: {salary_data_path}")
    else:
        print(f"❌ Missing salary data file: {salary_data_path}")
    
    # Check for telco churn data
    churn_data_path = os.path.join("data", "WA_Fn-UseC_-Telco-Customer-Churn.csv")
    if os.path.exists(churn_data_path):
        print(f"✅ Found telco churn data: {churn_data_path}")
    else:
        print(f"❌ Missing telco churn data file: {churn_data_path}")
        print("   Please make sure to download the dataset to the data/ directory.")

def check_mysql():
    print_header("Checking MySQL (Optional for Salary Analysis)")
    
    try:
        import mysql.connector
        print("✅ MySQL Connector is installed")
        
        # Ask if user wants to test MySQL connection
        ans = input("Do you want to test MySQL connection? (y/n): ")
        if ans.lower() == 'y':
            host = input("MySQL host (default: localhost): ") or "localhost"
            user = input("MySQL username: ")
            password = input("MySQL password: ")
            
            try:
                conn = mysql.connector.connect(
                    host=host,
                    user=user,
                    password=password
                )
                print("✅ Successfully connected to MySQL")
                conn.close()
            except Exception as e:
                print(f"❌ Failed to connect to MySQL: {e}")
                print("   You will need to fix this connection to run the salary analysis.")
        else:
            print("Skipping MySQL connection test.")
    except ImportError:
        print("❌ MySQL Connector is not installed correctly.")

def main():
    print_header("Spark Projects Setup")
    print(f"System: {platform.system()} {platform.release()}")
    print(f"Python version: {platform.python_version()}")
    
    # Make sure data directory exists
    if not os.path.exists("data"):
        print("Creating data directory...")
        os.makedirs("data")
    
    # Install requirements
    if not install_requirements():
        return
    
    # Check Spark installation
    if not check_spark_installation():
        return
    
    # Verify data files
    verify_data_files()
    
    # Check MySQL
    check_mysql()
    
    print_header("Setup Complete")
    print("You're ready to run the Spark projects!")
    print("\nTo run the salary analysis:")
    print("1. python src/import_salary.py  # Import data to MySQL")
    print("2. python src/spark_salary.py   # Analyze data with Spark")
    print("\nTo run the churn prediction:")
    print("python src/run_analysis.py check  # Verify data loading")
    print("python src/run_analysis.py model  # Run the full model")

if __name__ == "__main__":
    main() 