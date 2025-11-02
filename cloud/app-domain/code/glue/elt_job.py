import sys, json
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark = SparkSession.builder.appName(args['JOB_NAME']).getOrCreate()

raw_path = spark.conf.get("spark.etl.raw_path", "s3://my-cfn-artifacts-ap-south-1/raw/employees.csv")
out_path = spark.conf.get("spark.etl.out_path", "s3://my-cfn-artifacts-ap-south-1/curated/employees/")

df = spark.read.option("header", True).csv(raw_path)
rows_in = df.count()

df_clean = df.filter(col("employee_id").isNotNull()).withColumn("ingest_ts", current_timestamp())
rows_out = df_clean.count()

df_clean.write.mode("overwrite").parquet(out_path)

print("ETL_METRICS=" + json.dumps({"rows_in": rows_in, "rows_out": rows_out, "out_path": out_path}))

spark.stop()
