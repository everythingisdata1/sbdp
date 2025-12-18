import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

PYTHON_EXE = r"D:\codes\py-spark\spark-bulk-data-processing\.venv\Scripts\python.exe"

os.environ["PYSPARK_PYTHON"] = PYTHON_EXE

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("BasicDF")
    .getOrCreate()
)

data = [
    (1, "Bharat", "India", 10),
    (2, "Rahul", "USA", 5),
]
columns = ["id", "name", "country", "experience"]
df = spark.createDataFrame(data, schema=columns)
df.show()
df_with_struct = (df.withColumn("employee_details", f.struct(
    f.col("name").alias("Fname"), f.col("country").alias("COB"), f.col("experience").alias("Exp")
).alias("USER_DETAILS")))

df_with_struct.show(truncate=False)

df_with_struct.select(
    "id",
    "employee_details.Fname",
    "employee_details.COB",
    "employee_details.Exp"
).show()

df_with_struct.printSchema()
df_with_struct.write.parquet("testdata.csv")
