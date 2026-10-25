# Databricks notebook source
# DBTITLE 1,Importing Libraries
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

# DBTITLE 1,Write PySpark code to get employees with salary > 50000
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("dept", StringType(), True)
])

data = [
    (1, "Amit", 40000, "IT"),
    (2, "Neha", 60000, "HR"),
    (3, "Rahul", 55000, "IT"),
    (4, "Priya", 45000, "Finance"),
    (5, "Karan", 70000, "HR")
]

df = spark.createDataFrame(data, schema)
df.show()

###Answer
df1 = df.filter(col("salary") > 50000).select("emp_name", "salary")
display(df1)

# COMMAND ----------

# DBTITLE 1,Write PySpark code to get total amount per product
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("amount", IntegerType(), True)
])

data = [
    (1, "TV", 300),
    (2, "TV", 700),
    (3, "FRIDGE", 500),
    (4, "TV", 200),
    (5, "FRIDGE", 400)
]

df = spark.createDataFrame(data, schema)
df.show()

####Answer
df.groupBy("product").agg(sum("amount").alias("total_amount")).show()


# COMMAND ----------

# DBTITLE 1,Write PySpark code to add a column country = "India"
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True)
])

data = [
    (1, "Ravi", "Delhi"),
    (2, "Anita", "Mumbai"),
    (3, "Suresh", "Kolkata")
]

df = spark.createDataFrame(data, schema)
df.show()

####Answer
df.withColumn("Country", lit("India")).show()


# COMMAND ----------

# DBTITLE 1,Write PySpark code to get order count per customer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("status", StringType(), True)
])

data = [
    (101, 1, "DELIVERED"),
    (102, 1, "CANCELLED"),
    (103, 2, "DELIVERED"),
    (104, 2, "DELIVERED"),
    (105, 3, "DELIVERED")
]

df = spark.createDataFrame(data, schema)
df.show()

##Answer
df1 = (df.filter(col("status") == "DELIVERED").groupby("customer_id").count())
display(df1)

# COMMAND ----------

# DBTITLE 1,Write PySpark code to sort by salary (highest first)
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("salary", IntegerType(), True)
])

data = [
    (1, "Amit", 40000),
    (2, "Neha", 60000),
    (3, "Rahul", 55000),
    (4, "Priya", 45000)
]

df = spark.createDataFrame(data, schema)
df.show()

###Answer
df1 = df.orderBy(col("salary").desc())
df1.display()

# COMMAND ----------

# DBTITLE 1,Write PySpark code to remove duplicate rows
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True)
])

data = [
    (1, "Ravi", "Delhi"),
    (2, "Anita", "Mumbai"),
    (2, "Anita", "Mumbai"),
    (3, "Suresh", "Kolkata"),
    (1, "Ravi", "Delhi")
]

df = spark.createDataFrame(data, schema)
df.show()

###Answer
df1 = df.dropDuplicates()
df1.display()

