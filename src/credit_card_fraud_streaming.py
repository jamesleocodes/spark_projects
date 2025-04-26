from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegressionModel
import time
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("CreditCardFraudDetection-Streaming") \
    .master("local[*]") \
    .getOrCreate()

# Path to the sample data to get schema
file_path = "data/creditcard.csv"
model_path = "models/fraud_detection_model"

# Load a sample to get the schema
sample_df = spark.read.csv(file_path, header=True, inferSchema=True)
print("Loaded schema from sample data")

# Define all features and the feature vector assembler (same as training)
feature_columns = [col for col in sample_df.columns if col not in ["Class"]]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features", handleInvalid="skip")

# Load the model if it exists
if os.path.exists(model_path):
    loaded_model = LogisticRegressionModel.load(model_path)
    print(f"Model loaded from {model_path}")
else:
    print(f"Model not found at {model_path}. Using a simple example instead.")
    # For demonstration, we'll just proceed with a data pipeline without making predictions
    
# Define the streaming source
print("Setting up streaming source...")
streaming_data = spark.readStream \
    .schema(sample_df.schema) \
    .option("maxFilesPerTrigger", 1) \
    .option("header", "true") \
    .csv("data/stream_source")

# Process streaming data - add feature vector
streaming_data_with_features = assembler.transform(streaming_data)

# Define the output transformation - we'll just select a subset of columns
# Don't include the complex feature vector in the output display
streaming_output = streaming_data_with_features.select(
    "Time", "Amount", "Class"
)

# If model exists, add predictions
if 'loaded_model' in locals():
    # Apply the model and select prediction columns
    streaming_predictions = loaded_model.transform(streaming_data_with_features)
    streaming_output = streaming_predictions.select(
        "Time", "Amount", "Class", 
        col("prediction").cast("integer").alias("predicted_fraud"),
        "probability"
    )

# Start the streaming query
print("Starting streaming query...")
query = streaming_output \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Run for 30 seconds
print("Streaming started. Processing files for 30 seconds...")
try:
    query.awaitTermination(30)  # wait for 30 seconds
finally:
    print("Stopping stream...")
    query.stop()
    
print("Streaming demo complete.")
spark.stop() 