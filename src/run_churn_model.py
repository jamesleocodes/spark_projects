from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TelcoCustomerChurnPrediction") \
    .master("local[*]") \
    .getOrCreate()

# Set logging level
spark.sparkContext.setLogLevel("WARN")

# Get full path to data file
current_dir = os.getcwd()
file_path = os.path.join(current_dir, "data", "WA_Fn-UseC_-Telco-Customer-Churn.csv")

print(f"Looking for data file at: {file_path}")
print(f"File exists: {os.path.exists(file_path)}")

# 2. Load the Telco Customer Churn dataset
def load_data(file_path):
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print(f"Dataset loaded successfully with {df.count()} rows.")
        print("Sample data:")
        df.show(5)
        return df
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return None

# 3. Data Preprocessing
def preprocess_data(df):
    print("Preprocessing data...")
    # Drop customerID as it's not needed for prediction
    df = df.drop("customerID")
    
    # Handle missing values (TotalCharges may have empty strings or nulls)
    df = df.filter(col("TotalCharges") != " ")  # Remove rows with empty TotalCharges
    df = df.withColumn("TotalCharges", col("TotalCharges").cast("float"))
    
    # Define categorical and numerical columns
    categorical_columns = [
        "gender", "SeniorCitizen", "Partner", "Dependents", "PhoneService",
        "MultipleLines", "InternetService", "OnlineSecurity", "OnlineBackup",
        "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies",
        "Contract", "PaperlessBilling", "PaymentMethod"
    ]
    numerical_columns = ["tenure", "MonthlyCharges", "TotalCharges"]
    
    # Index categorical columns
    indexed_columns = [f"{col}_indexed" for col in categorical_columns]
    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_indexed", handleInvalid="keep")
        for col in categorical_columns
    ]
    
    for indexer in indexers:
        df = indexer.fit(df).transform(df)
    
    # Index target variable (Churn)
    df = StringIndexer(inputCol="Churn", outputCol="label").fit(df).transform(df)
    
    # Assemble features
    feature_columns = numerical_columns + indexed_columns
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="raw_features", handleInvalid="skip")
    df = assembler.transform(df)
    
    # Scale features
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    df = scaler.fit(df).transform(df)
    
    print("Data preprocessing completed.")
    return df, feature_columns

def main():
    # Load data
    df = load_data(file_path)
    if df is None:
        print("Exiting due to data loading error.")
        return
    
    # Preprocess data
    df_processed, feature_columns = preprocess_data(df)
    
    # Train-test split
    print("Splitting data into training and test sets...")
    train_df, test_df = df_processed.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set size: {train_df.count()} rows")
    print(f"Test set size: {test_df.count()} rows")
    
    # Train Random Forest model
    print("Training Random Forest classifier...")
    rf = RandomForestClassifier(
        featuresCol="features", 
        labelCol="label", 
        numTrees=100, 
        maxDepth=5,
        seed=42
    )
    model = rf.fit(train_df)
    print("Model training completed.")
    
    # Make predictions
    print("Making predictions on test set...")
    predictions = model.transform(test_df)
    predictions.select("prediction", "label", "probability").show(10)
    
    # Evaluate model
    print("Evaluating model performance...")
    evaluator_accuracy = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    evaluator_precision = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
    )
    evaluator_recall = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="weightedRecall"
    )
    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    
    accuracy = evaluator_accuracy.evaluate(predictions)
    precision = evaluator_precision.evaluate(predictions)
    recall = evaluator_recall.evaluate(predictions)
    auc = evaluator_auc.evaluate(predictions)
    
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"Area Under ROC: {auc:.4f}")
    
    # Feature importance
    print("Calculating feature importance...")
    feature_importance = list(zip(feature_columns, model.featureImportances.toArray()))
    feature_importance.sort(key=lambda x: x[1], reverse=True)
    
    # Convert to pandas DataFrame for easier visualization
    feature_imp_df = pd.DataFrame(feature_importance, columns=["feature", "importance"])
    print("Top 10 most important features:")
    print(feature_imp_df.head(10))
    
    # Plot feature importance
    try:
        plt.figure(figsize=(12, 8))
        sns.barplot(x="importance", y="feature", data=feature_imp_df.head(15))
        plt.title("Feature Importance in Telco Customer Churn Prediction")
        plt.tight_layout()
        
        # Save plot
        output_file = os.path.join(current_dir, "feature_importance.png")
        plt.savefig(output_file)
        print(f"Feature importance plot saved to: {output_file}")
    except Exception as e:
        print(f"Could not create plot: {e}")
    
    # Save model
    try:
        model_path = os.path.join(current_dir, "churn_model")
        model.write().overwrite().save(model_path)
        print(f"Model saved to: {model_path}")
    except Exception as e:
        print(f"Could not save model: {e}")
        
    # Stop Spark session
    spark.stop()
    print("Analysis completed successfully!")

if __name__ == "__main__":
    main() 