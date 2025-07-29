# scripts/analysis.py
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName("MLAnalysis").getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/superset_db") \
    .option("dbtable", "connectivite_clean") \
    .option("user", "superset") \
    .option("password", "superset_password") \
    .load()

# ✅ K-means clustering
features_kmeans = VectorAssembler(inputCols=["taux_internet"], outputCol="features")
df_kmeans = features_kmeans.transform(df)
kmeans = KMeans(k=3, seed=1)
model_kmeans = kmeans.fit(df_kmeans)
df_clustered = model_kmeans.transform(df_kmeans)

df_clustered.select("pays", "annee", "taux_internet", "prediction").write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/superset_db") \
    .option("dbtable", "clusters") \
    .option("user", "superset") \
    .option("password", "superset_password") \
    .mode("overwrite").save()

# ✅ Régression linéaire
df_reg = df.dropna(subset=["taux_internet", "investissements"])
assembler = VectorAssembler(inputCols=["investissements"], outputCol="features")
df_train = assembler.transform(df_reg).select("features", col("taux_internet").alias("label"))

lr = LinearRegression()
model_lr = lr.fit(df_train)
predictions = model_lr.transform(df_train)

predictions.select("features", "label", "prediction").write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/superset_db") \
    .option("dbtable", "regression_results") \
    .option("user", "superset") \
    .option("password", "superset_password") \
    .mode("overwrite").save()

spark.stop()
