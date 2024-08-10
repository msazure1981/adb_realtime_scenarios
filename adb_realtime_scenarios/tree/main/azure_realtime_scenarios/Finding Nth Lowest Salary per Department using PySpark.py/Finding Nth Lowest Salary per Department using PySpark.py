# Databricks notebook source
employees_Salary = [
    ('James', 'Sales', 2000),
('sofy', 'Sales', 3000),
('Laren', 'Sales', 4000),
('Kiku', 'Sales', 5000),
('Kims', 'Sales', 5000),
('Luna', 'Sales', 6000),
('Sam', 'Finance', 6000),
('Samuel', 'Finance', 7000),
('Yash', 'Finance', 8000),
('Rabin', 'Finance', 9000),
('Lukasz', 'Marketing', 10000),
('Jolly', 'Marketing', 11000),
('Mausam', 'Marketing', 12000),
('Lamba', 'Marketing', 13000),
('Jogesh', 'HR', 14000),
('Mannu', 'HR', 15000),
('Sylvia', 'HR', 16000),
('Sama', 'HR', 17000),
]

#Create the spark Data frame and assign schema with it

employeesDF = spark.createDataFrame(employees_Salary,schema='employee_name STRING, dept_name STRING, salary INTEGER')
employeesDF.show()

# COMMAND ----------

#libraries for window and rank
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

#Create window specification for applying window function
windowPartition = Window.partitionBy('dept_name').orderBy('salary')

#Apply the window rank specification
rankDF = employeesDF.withColumn('rank_col', rank().over(windowPartition))
rankDF.show()

# COMMAND ----------

employeeDF.filter('rank_col=1').show()

# COMMAND ----------

from pyspark.sql.functions import dense_rank
#Apply the window dense rank specification
denseDF = employeesDF.withColumn('denserank_col', dense_rank().over(windowPartition))
denseDF.show()

# COMMAND ----------

from pyspark.sql.functions import row_number
#Apply the window row number  specification
rwnDF = employeesDF.withColumn('row_number', row_number().over(windowPartition))
rwnDF.show()