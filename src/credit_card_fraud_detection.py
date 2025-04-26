from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os

# Initialize Spark Session
def init_spark():
    spark = SparkSession.builder \
        .appName("FraudDetection") \
        .master("local[*]") \
        .getOrCreate()
    return spark

# Load the credit card dataset
def load_data(spark, file_path):
    try:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        print("Dataset loaded successfully.")
        return df
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return None

# Check for null values
def check_nulls(df):
    df.select([col(c).isNull().cast("int").alias(c) for c in df.columns]).groupBy().sum().show()

# Data preprocessing
def preprocess_data(df):
    # Print the schema
    print("Data Schema:")
    df.printSchema()
    
    # Check for null values
    print("Checking for null values:")
    check_nulls(df)
    
    # Define feature columns (all except the Class column)
    feature_columns = [col for col in df.columns if col != "Class"]
    
    # Create vector assembler
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features_raw",
        handleInvalid="skip"
    )
    df_assembled = assembler.transform(df)
    
    # Scale features
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    return df_scaled, feature_columns

# Train models
def train_models(df):
    # Split the data
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"Training data size: {train_df.count()}")
    print(f"Testing data size: {test_df.count()}")
    
    # Train Logistic Regression model
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="Class",
        maxIter=10,
        regParam=0.01
    )
    lr_model = lr.fit(train_df)
    print("Logistic Regression model trained")
    
    # Save the model
    model_dir = "../models/fraud_detection_model"
    lr_model.write().overwrite().save(model_dir)
    print(f"Model saved to {model_dir}")
    
    # Make predictions
    lr_predictions = lr_model.transform(test_df)
    
    # Evaluate model
    evaluator_accuracy = MulticlassClassificationEvaluator(labelCol="Class", predictionCol="prediction", metricName="accuracy")
    evaluator_precision = MulticlassClassificationEvaluator(labelCol="Class", predictionCol="prediction", metricName="weightedPrecision")
    evaluator_recall = MulticlassClassificationEvaluator(labelCol="Class", predictionCol="prediction", metricName="weightedRecall")
    evaluator_roc = BinaryClassificationEvaluator(labelCol="Class", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    
    accuracy = evaluator_accuracy.evaluate(lr_predictions)
    precision = evaluator_precision.evaluate(lr_predictions)
    recall = evaluator_recall.evaluate(lr_predictions)
    roc_auc = evaluator_roc.evaluate(lr_predictions)
    
    print(f"Accuracy: {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"ROC AUC: {roc_auc:.4f}")
    
    return lr_model, lr_predictions

# Analyze model results
def analyze_results(model, predictions, feature_columns):
    # Convert feature importances to a pandas DataFrame (if using RandomForest)
    if hasattr(model, 'featureImportances'):
        feature_importance = pd.DataFrame({
            'feature': feature_columns,
            'importance': model.featureImportances.toArray()
        }).sort_values('importance', ascending=False)
        
        # Plot feature importance
        plt.figure(figsize=(12, 8))
        sns.barplot(x='importance', y='feature', data=feature_importance)
        plt.title('Feature Importance in Credit Card Fraud Detection')
        plt.tight_layout()
        plt.savefig('../fraud_feature_importance.png')
        plt.close()
        
        # Save feature importance data
        feature_importance.to_csv('../fraud_feature_importance.csv', index=False)
        print("\nFeature importance plot saved as 'fraud_feature_importance.png'")
        print("Feature importance data saved as 'fraud_feature_importance.csv'")

# Main function
def main():
    # File path - use relative path to access file in the root data directory
    file_path = "../data/creditcard.csv"
    
    # Initialize Spark
    spark = init_spark()
    
    # Load data
    df = load_data(spark, file_path)
    if df is None:
        print("Exiting due to data loading error.")
        return
    
    # Show sample data
    print("Sample data:")
    df.show(5)
    
    # Preprocess data
    df_processed, feature_columns = preprocess_data(df)
    
    # Train and evaluate model
    model, predictions = train_models(df_processed)
    
    # Analyze results
    analyze_results(model, predictions, feature_columns)
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main() 