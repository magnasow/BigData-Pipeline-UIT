# scripts/clean_transform.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder \
    .appName("CleanTransform") \
    .getOrCreate()

# 🔐 Connexion MinIO
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# 📥 Charger les données UIT
df = spark.read.option("header", "true").parquet("s3a://bigdata-pipeline/connectivite_mobile_internet.parquet")

# 🧹 Nettoyage de base
df_clean = df.dropna(subset=["annee", "pays", "taux_internet"]) \
             .withColumn("annee", col("annee").cast("int")) \
             .withColumn("taux_internet", col("taux_internet").cast("float"))

# 💾 Sauvegarde dans PostgreSQL
df_clean.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/superset_db") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "connectivite_clean") \
    .option("user", "superset") \
    .option("password", "superset_password") \
    .mode("overwrite") \
    .save()

spark.stop()
