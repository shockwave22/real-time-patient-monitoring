from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("VitalsMonitor") \
    .getOrCreate()

schema = StructType() \
    .add("patient_id", StringType()) \
    .add("heart_rate", DoubleType()) \
    .add("bp_systolic", DoubleType()) \
    .add("bp_diastolic", DoubleType()) \
    .add("timestamp", LongType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vitals") \
    .load()

vitals = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Flag high risk vitals
alerts = vitals.withColumn("alert", expr("""
    heart_rate > 120 OR bp_systolic > 160 OR bp_diastolic > 100
"""))

query = alerts \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
