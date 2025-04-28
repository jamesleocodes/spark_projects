# Data Analysis Projects with Apache Spark

This repository contains multiple data analysis projects using Apache Spark:

1. Employee Salary Data Analysis
2. Telco Customer Churn Prediction
3. Credit Card Fraud Detection

For those new to ETL, a foundational article on the topic can be found [here](https://medium.com/@mr.zawmyowin.physics/data-engineering-simplified-techniques-tools-and-insights-for-aspiring-professionals-a8a4f29f78bb). The instructions for setting up MySQL are detailed in the [provided guide](https://github.com/jamesleocodes/ETL_projects/blob/main/setupSQL.md).

## Repository Structure

```
Spark/
├── README.md                         # Project documentation
├── LICENSE                           # MIT License
├── setup.py                          # Setup script for easy installation
├── requirements.txt                  # Python dependencies
├── src/                              # Source code directory
│   ├── import_salary.py              # Script to import CSV data into MySQL
│   ├── spark_salary.py               # Script for Spark analysis of MySQL data
│   ├── load_churn_data.py            # Script for data loading diagnostics
│   ├── run_churn_model.py            # Script for the full ML workflow
│   ├── credit_card_fraud_detection.py # Credit card fraud detection script
│   ├── credit_card_fraud_streaming.py # Real-time fraud detection script
│   └── generate_mock_stream.py        # Script to generate mock streaming data
├── data/                             # Data directory
│   ├── salary.csv                    # Employee salary data
│   ├── WA_Fn-UseC_-Telco-Customer-Churn.csv  # Telco customer churn data
│   └── creditcard.csv                # Credit card fraud detection data
├── salary_data.parquet/              # Parquet directory with employee data
└── salary_analysis_results.parquet/  # Parquet directory with analysis results
```

## Quick Start

1. Clone this repository:
   ```
   git clone https://github.com/yourusername/Spark.git
   cd Spark
   ```

2. Run the setup script to install dependencies and verify your environment:
   ```
   python setup.py
   ```
   
   This will:
   - Install all required Python packages
   - Check if Apache Spark is correctly installed
   - Verify data files are present
   - Check MySQL connection (optional for salary analysis)

3. Follow the specific instructions for each project below.

# Project 1: Employee Salary Data Analysis

## Project Overview

This project demonstrates a complete data workflow:

1. Load structured salary data from CSV into a MySQL database
2. Extract the data from MySQL into Apache Spark 
3. Perform data analysis and aggregations using Spark SQL and DataFrames

## Data Structure

The salary dataset contains employee records with the following fields:

- `id`: Employee ID
- `first_name`: Employee's first name
- `last_name`: Employee's last name
- `salary`: Employee salary value
- `department_id`: Department identifier

Each employee has multiple salary records in the dataset representing different salary points.

## Project Structure

```
employee-salary-analysis/
├── README.md                         # Project documentation
├── data/
│   └── salary.csv                    # Source data file (300 employee records)
├── import_salary.py                  # Script to import CSV data into MySQL
├── spark_salary.py                   # Script for Spark analysis of MySQL data
├── salary_data.parquet/              # Parquet directory with all employee data
└── salary_analysis_results.parquet/  # Parquet directory with analysis results
```

## Files

- `data/salary.csv`: The source CSV data file (300 rows of employee salary records)
- `src/import_salary.py`: Python script to load CSV data into MySQL
- `src/spark_salary.py`: Main script that connects to MySQL and performs Spark analysis

## Setup Instructions

### Prerequisites

- Python 3.x
- MySQL Server
- Apache Spark
- Python packages: 
  - mysql-connector-python
  - pyspark
  - pandas

### Installation

1. Clone this repository:
   ```
   git clone https://github.com/jamesleocodes/spark_projects.git
   cd spark_projects
   ```

2. Install required Python packages using the requirements.txt file:
   ```
   pip install -r requirements.txt
   ```

3. Set up the MySQL database:
   - Make sure MySQL server is running
   - Create a database named `employee_db` (or the script will create it for you)

### Data Import

1. To load the CSV data into MySQL:
   ```
   python src/import_salary.py
   ```
   This script will:
   - Create the `employee_db` database if it doesn't exist
   - Create the `salary` table
   - Import data from `data/salary.csv` into the table

### Spark Analysis

1. Run the Spark analysis script:
   ```
   python src/spark_salary.py
   ```
   This script will:
   - Connect to MySQL and extract the salary data
   - Create a Spark DataFrame from the data
   - Perform SQL analysis to identify maximum salaries by employee
   - Display the results

## Example Output

The analysis shows the maximum salary for each employee, grouped by ID, name, and department:

```
+---+-----------+----------+-----------+-------------+
| id|first_name |last_name |LatesSalary|department_id|
+---+-----------+----------+-----------+-------------+
|100|Dominic    |Rose      |115000.0   |1100         |
|99 |Melanie    |Crawford  |110500.0   |1099         |
|98 |Cooper     |Olson     |113500.0   |1098         |
...
```

## Customization

- To modify the MySQL connection parameters, edit the connection settings in `src/spark_salary.py`
- To change the number of records displayed, adjust the `show(n=10)` parameter in the Spark SQL query

## Data Flow

```
                       ┌─────────────┐          ┌─────────────┐         ┌─────────────┐
                       │             │          │             │         │             │
   ┌─────────┐         │   MySQL     │          │   Apache    │         │  Analysis   │
   │  CSV    │  Load   │  Database   │ Extract  │   Spark     │ Process │  Results    │
   │  Data   ├────────►│ (employee_db)├────────►│  DataFrame  ├────────►│ & Insights  │
   └─────────┘         │             │          │             │         │             │
                       └─────────────┘          └─────────────┘         └─────────────┘
                                 ▲                                             │
                                 │                                             │
                                 └─────────────────────────────────────────────┘
                                             Optional: Save back
```

## Technical Details

### MySQL Schema

```sql
CREATE TABLE salary (
    id INT,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    salary DECIMAL(10,2),
    department_id INT,
    PRIMARY KEY (id, department_id, salary)
)
```

### Spark SQL Queries

The main analysis query groups the data by employee information to find maximum salaries:

```sql
SELECT 
    id, first_name, last_name, 
    MAX(salary) AS LatestSalary, 
    department_id
FROM 
    tmpEmployee
GROUP BY 
    id, first_name, last_name, department_id
ORDER BY 
    id
```

### Data Optimization

- The imported data is stored in Spark's optimized columnar format (Parquet)
- Proper indexing is used in the MySQL table for efficient querying
- Batch processing is used when importing data from CSV to MySQL

### Parquet Data Storage

The script saves data in Parquet format, which provides:

- **Columnar Storage**: More efficient compression and faster reads for analytical queries
- **Schema Preservation**: Data types are preserved exactly as defined in Spark
- **Improved Performance**: Reading from Parquet is much faster than re-querying MySQL
- **Size Efficiency**: Parquet files are typically 2-4x smaller than equivalent CSV files

Two Parquet datasets are generated:
1. `salary_data.parquet/`: Contains all the raw employee salary data
2. `salary_analysis_results.parquet/`: Contains the analyzed results (max salary per employee)

To read data from these Parquet files in a future Spark session:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read Salary Data").getOrCreate()

# Read the raw data
salary_df = spark.read.parquet("salary_data.parquet")

# Read the analysis results
results_df = spark.read.parquet("salary_analysis_results.parquet")

# Show data
salary_df.show()
results_df.show()
```
# Project 2: Telco Customer Churn Prediction

A machine learning project that analyzes customer data to predict churn in a telecommunications company using Apache Spark's MLlib.

## Project Overview

This project demonstrates a complete machine learning workflow:

1. Load telco customer data from CSV
2. Process and prepare data for machine learning
3. Train a Random Forest model to predict customer churn
4. Evaluate model performance and analyze feature importance

## Data Structure

The telco customer dataset contains customer records with the following fields:

- `customerID`: Unique identifier for each customer
- Demographics: `gender`, `SeniorCitizen`, `Partner`, `Dependents`
- Services: `PhoneService`, `MultipleLines`, `InternetService`, etc.
- Account information: `Contract`, `PaperlessBilling`, `PaymentMethod`
- Financial data: `MonthlyCharges`, `TotalCharges`
- `Churn`: Target variable indicating whether the customer left the company

## Project Structure

```
telco-churn-prediction/
├── src/
│   ├── run_analysis.py               # Main script to run the analysis
│   ├── load_churn_data.py            # Script for data loading diagnostics
│   └── run_churn_model.py            # Script for the full ML workflow
├── data/
│   └── WA_Fn-UseC_-Telco-Customer-Churn.csv  # Source data file
├── churn_model/                      # Saved model directory
└── feature_importance.png            # Feature importance visualization
```

## Files

- `data/WA_Fn-UseC_-Telco-Customer-Churn.csv`: The source data file with customer records
- `src/load_churn_data.py`: Script to verify data loading works correctly
- `src/run_churn_model.py`: Main script that loads data and performs ML analysis
- `src/run_analysis.py`: Helper script to run either of the above scripts

## Setup Instructions

### Prerequisites

- Python 3.x
- Apache Spark
- Python packages: 
  - pyspark
  - pandas
  - matplotlib
  - seaborn

### Running the Analysis

1. To check if the dataset can be loaded correctly:
   ```
   python src/run_analysis.py check
   ```

2. To run the full churn prediction model:
   ```
   python src/run_analysis.py model
   ```

3. For help and available commands:
   ```
   python src/run_analysis.py help
   ```

## Example Output

The analysis provides various metrics to evaluate the model performance:

```
Accuracy: 0.7928
Precision: 0.7768
Recall: 0.7928
Area Under ROC: 0.8474
```

Additionally, a feature importance analysis shows which factors contribute most to customer churn:

```
                    feature  importance
0          Contract_indexed    0.285943
1                    tenure    0.182681
2       TechSupport_indexed    0.117928
3    OnlineSecurity_indexed    0.096509
4   InternetService_indexed    0.085970
```

## Technical Details

### Data Preprocessing

The preprocessing pipeline includes:
- Handling missing values in TotalCharges
- Converting categorical variables to numerical using StringIndexer
- Feature assembly and scaling

### Model Training

We use a Random Forest Classifier with the following parameters:
- 100 trees
- Maximum depth of 5
- Features selected: All customer attributes after indexing

### Model Evaluation

The model is evaluated using multiple metrics:
- Accuracy
- Precision
- Recall
- Area Under ROC curve

# Project 3: Credit Card Fraud Detection

## Project Overview

This project demonstrates a complete machine learning workflow for credit card fraud detection:

1. Load and preprocess credit card transaction data
2. Build and train a machine learning model to detect fraudulent transactions
3. Evaluate model performance with various metrics
4. Implement real-time fraud detection with Spark Structured Streaming

## Data Structure

The credit card dataset contains anonymized credit card transactions with the following fields:

- `Time`: Time elapsed between this transaction and the first transaction in the dataset
- `V1-V28`: Features from PCA dimension reduction (anonymized for privacy)
- `Amount`: Transaction amount
- `Class`: Target variable (1 for fraud, 0 for legitimate transaction)

## Project Structure

```
credit-card-fraud-detection/
├── src/
│   ├── credit_card_fraud_detection.py   # Main Python script for fraud detection
│   ├── credit_card_fraud_streaming.py   # Streaming implementation for real-time detection
│   └── generate_mock_stream.py          # Script to generate mock streaming data
├── data/
│   └── creditcard.csv                   # Credit card transaction dataset
└── models/
    └── fraud_detection_model/           # Saved ML model for reuse
```

## Files

- `src/credit_card_fraud_detection.py`: Complete pipeline for fraud detection (data loading, preprocessing, model training, evaluation)
- `src/credit_card_fraud_streaming.py`: Implementation of real-time fraud detection using Spark Structured Streaming
- `src/generate_mock_stream.py`: Utility to generate synthetic transaction data for testing the streaming pipeline

## Setup Instructions

### Prerequisites

- Python 3.x
- Apache Spark
- Python packages: 
  - pyspark
  - pandas
  - matplotlib
  - seaborn

### Installation

1. Ensure you have the dataset in the `data/` directory:
   - Download the credit card fraud dataset from Kaggle
   - Place it at `data/creditcard.csv`

2. Install required Python packages:
   ```
   pip install -r requirements.txt
   ```

### Running the Analysis

1. To train and evaluate the fraud detection model:
   ```
   python src/credit_card_fraud_detection.py
   ```
   This script will:
   - Load and preprocess the credit card transaction data
   - Train a Logistic Regression model
   - Evaluate model performance
   - Save the trained model

2. To test real-time fraud detection with streaming data:
   ```
   # First, generate mock streaming data
   python src/generate_mock_stream.py
   
   # Then run the streaming detection script
   python src/credit_card_fraud_streaming.py
   ```

## Model Performance

The model is evaluated using several metrics:
- Accuracy: Overall prediction accuracy
- Precision: Ability to detect actual fraud cases
- Recall: Ability to find all fraud cases
- ROC AUC: Area under the Receiver Operating Characteristic curve

## Technical Details

### Data Preprocessing
- Check for null values
- Vector assembly of features
- Feature scaling with StandardScaler

### Machine Learning Pipeline
- Feature engineering with VectorAssembler
- Logistic Regression classifier
- Model training with cross-validation
- Model persistence for reuse in streaming

### Streaming Implementation
- Real-time transaction monitoring
- Application of pre-trained model to new transactions
- Continuous fraud detection and alerting

## Real-time Fraud Detection

The streaming implementation allows for processing transactions as they occur:

```
                      ┌─────────────┐          ┌─────────────┐          ┌─────────────┐
                      │  Stream     │          │             │          │             │
   ┌─────────┐        │  Source     │          │  Feature    │          │  Fraud      │
   │ New     │ Input  │ (Transaction│ Process  │  Processing │ Predict  │  Detection  │
   │ Trans-  ├───────►│  Events)    ├─────────►│  Pipeline   ├─────────►│  Results    │
   │ actions │        │             │          │             │          │             │
   └─────────┘        └─────────────┘          └─────────────┘          └─────────────┘
                                                      ▲
                                                      │
                                              ┌───────┴───────┐
                                              │ Pre-trained   │
                                              │ Fraud Model   │
                                              └───────────────┘
```

## Future Enhancements

- Implement more sophisticated models (RandomForest, GBT)
- Add feature importance analysis and visualization
- Implement anomaly detection algorithms
- Enhance streaming with Kafka integration
- Add a dashboard for real-time monitoring

## License

This project is open source and available under the [MIT License](LICENSE).

## Contributors

- Zaw Myo Win
