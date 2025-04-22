# Employee Salary Data Analysis

A data pipeline that loads employee salary data from a CSV file into MySQL, then analyzes it using Apache Spark. For those new to ETL, a foundational article on the topic can be found [here](https://medium.com/@mr.zawmyowin.physics/data-engineering-simplified-techniques-tools-and-insights-for-aspiring-professionals-a8a4f29f78bb). The instructions for setting up MySQL are detailed in the [provided guide](https://github.com/jamesleocodes/ETL_projects/blob/main/setupSQL.md).

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
- `import_salary.py`: Python script to load CSV data into MySQL
- `spark_salary.py`: Main script that connects to MySQL and performs Spark analysis

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
   python import_salary.py
   ```
   This script will:
   - Create the `employee_db` database if it doesn't exist
   - Create the `salary` table
   - Import data from `data/salary.csv` into the table

### Spark Analysis

1. Run the Spark analysis script:
   ```
   python spark_salary.py
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

- To modify the MySQL connection parameters, edit the connection settings in `spark_salary.py`
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

## License

This project is open source and available under the [MIT License](LICENSE).

## Contributors

- Zaw Myo Win
