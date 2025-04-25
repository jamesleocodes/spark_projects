from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TelcoCustomerChurnPrediction") \
    .master("local[*]") \
    .getOrCreate()

# Set logging level
spark.sparkContext.setLogLevel("WARN")

# Get the current directory
current_dir = os.getcwd()
print(f"Current directory: {current_dir}")

# List files in data directory
data_dir = os.path.join(current_dir, "data")
print(f"Data directory: {data_dir}")
print("Files in data directory:")
if os.path.exists(data_dir):
    for file in os.listdir(data_dir):
        print(f"  - {file}")
else:
    print("  Data directory does not exist")

# Correct file path
file_path = os.path.join(current_dir, "data", "WA_Fn-UseC_-Telco-Customer-Churn.csv")
print(f"Full file path: {file_path}")
print(f"File exists: {os.path.exists(file_path)}")

# Load data
if os.path.exists(file_path):
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print("Dataset loaded successfully.")
        print(f"Number of rows: {df.count()}")
        print("First 5 rows:")
        df.show(5)
    except Exception as e:
        print(f"Error loading dataset: {e}")
else:
    print(f"File does not exist at path: {file_path}")

# Stop Spark session
spark.stop() 