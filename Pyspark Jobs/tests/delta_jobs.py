# delta_job.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .getOrCreate()

# 1. Création d'un DataFrame simple
data = [("Ala", 25), ("Sara", 30), ("Nour", 28)]
df = spark.createDataFrame(data, ["name", "age"])

# 2. Sauvegarde en format Delta
df.write.format("delta").mode("overwrite").save("/data/delta/people")

# 3. Lecture du Delta
df2 = spark.read.format("delta").load("/data/delta/people")
df2.show()

