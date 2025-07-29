from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 🔹 Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("NettoyageConnectivityUIT") \
    .getOrCreate()

# 🔐 Configuration MinIO (accès S3-compatible)
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# 📥 Lecture du fichier CSV depuis le volume monté (/data)
input_path = "/data/connectivite_mobile_internet.csv"
df = spark.read.option("header", "true").csv(input_path)

# 🧼 Nettoyage simple : suppression des lignes avec des valeurs manquantes
df_cleaned = df.dropna()

# (Optionnel) Exemple de transformation supplémentaire
# df_cleaned = df_cleaned.withColumnRenamed("OldColumn", "NewColumn")

# 📤 Écriture en format Parquet dans le bucket MinIO (S3A)
output_path = "s3a://bigdata-pipeline/connectivite_mobile_internet.parquet"
df_cleaned.write.mode("overwrite").parquet(output_path)

print(f"✅ Parquet sauvegardé dans MinIO : {output_path}")

# 🛑 Arrêt de la session Spark
spark.stop()
