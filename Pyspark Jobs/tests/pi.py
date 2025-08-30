# /data/jobs/pi.py
from random import random
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkPiDelta").getOrCreate()

NUM_SAMPLES = 1000000
count = spark.sparkContext.parallelize(range(0, NUM_SAMPLES)).filter(
    lambda _: random() ** 2 + random() ** 2 < 1
).count()

pi = 4.0 * count / NUM_SAMPLES
print(f"Pi is roughly {pi}")
spark.stop()
