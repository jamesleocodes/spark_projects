# load mysql 
# import mysql library
import mysql.connector
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import round
import os
# connect to mysql
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="chosandarhtet",
    database="employee_db"
)

# create a cursor
mycursor = mydb.cursor()

# read the data from the table
mycursor.execute("SELECT * FROM salary")

# show the data in dataframe
df = mycursor.fetchall()
#df = pd.DataFrame(df, columns=['id', 'first_name', 'last_name', 'salary', 'department_id'])

# Initialize Spark Session
spark = SparkSession.builder.appName("Salary").getOrCreate()

# Create Spark DataFrame
df = spark.createDataFrame(df, schema=['id', 'first_name', 'last_name', 'salary', 'department_id'])

# round the salary to 2 decimal places
df = df.withColumn("salary", round(df["salary"], 2))
# use group by to get the max salary of each employee
df_max_salary = df.groupBy('id','first_name','last_name','department_id').max('salary').orderBy('id', ascending=False)  

# show the data in dataframe
#df_max_salary.show()

# create HIVE table
df.createOrReplaceTempView("tmpEmployee")

# Use spark.sql instead of sqlContext.sql
result_df = spark.sql("SELECT id,first_name,last_name,MAX(salary) AS LatesSalary,department_id \
                       FROM tmpEmployee \
                       GROUP BY id,first_name,last_name,department_id \
                       ORDER BY id")

# Show results (limit to 10 rows)
result_df.show(n=10)

# Save data as Parquet for better performance and reuse
parquet_output_path = "salary_data.parquet"

# Check if directory exists, if so delete it (Spark cannot overwrite by default)
if os.path.exists(parquet_output_path):
    import shutil
    shutil.rmtree(parquet_output_path)
    print(f"Removed existing Parquet directory: {parquet_output_path}")

# Save the original data DataFrame
print(f"Saving all data to Parquet: {parquet_output_path}")
df.write.parquet(parquet_output_path)

# Save the analysis results 
results_output_path = "salary_analysis_results.parquet"
if os.path.exists(results_output_path):
    import shutil
    shutil.rmtree(results_output_path)
    print(f"Removed existing results Parquet directory: {results_output_path}")

print(f"Saving analysis results to Parquet: {results_output_path}")
result_df.write.parquet(results_output_path)

# Example of reading back from Parquet
print("\nReading back data from Parquet file:")
parquet_df = spark.read.parquet(parquet_output_path)
print(f"Successfully loaded {parquet_df.count()} rows from Parquet")
parquet_df.printSchema()
parquet_df.show(5)

# close the connection
mydb.close()

# Stop Spark session to clean up resources
spark.stop()

print("\nProcess completed successfully. Data saved to Parquet format for faster future access.")