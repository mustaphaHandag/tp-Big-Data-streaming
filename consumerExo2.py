from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import pyspark.sql.functions as F
import pyspark.sql.types as T


spark = SparkSession.builder.appName("AdidasStreamingApp").getOrCreate()

stream_data_path = "TD4/data/adidas/stream/"

# Définition du shéma de données
schema = StructType([
    StructField("url", StringType()),
    StructField("name", StringType()),
    StructField("sku", StringType()),
    StructField("selling_price", DoubleType()),
    StructField("original_price", StringType()),
    StructField("currency", StringType()),
    StructField("availability", StringType()),
    StructField("color", StringType()),
    StructField("category", StringType()),
    StructField("source", StringType()),
    StructField("source_website", StringType()),
    StructField("breadcrumbs", StringType()),
    StructField("description", StringType()),
    StructField("brand", StringType()),
    StructField("images", StringType()),
    StructField("country", StringType()),
    StructField("language", StringType()),
    StructField("average_rating", DoubleType()),
    StructField("reviews_count", IntegerType()),
    StructField("crawled_at", StringType())
])

# Lecture des données en continu
# Les données se trouvant dans le dossier "TD4/data/adidas/stream/" sont lues au format CSV
# On précise que la première ligne des données représente l'en-tête.
stream_df = spark.readStream\
    .schema(schema)\
    .csv(stream_data_path, header=True, inferSchema=True)

# Supprimer le symbole "$" de la colonne "original_price"
stream_df = stream_df.withColumn("original_price", regexp_replace(col("original_price"), "\\$", ""))
# Conversion de la colonne "original_price" en double pour le calcul
stream_df = stream_df.withColumn("original_price", col("original_price").cast("double"))


# Calcul du pourcentage de remise
# On vérifie si la valeur de "original_price" n'est pas absente, pour discount_percentage null
stream_df = stream_df\
    .withColumn("discount_percentage", F.when(F.col("original_price") != 0, (F.col("original_price") - F.col("selling_price")) / F.col("original_price")).otherwise(F.lit(None)))

# Requête pour trouver les 5 produits avec les meilleures évaluations et les moins chers
agg_top_reviews_and_least_expensive = stream_df\
    .select("name", "selling_price", "original_price", "reviews_count")\
    .groupBy("name")\
    .agg(F.max("reviews_count").alias("reviews_count_max"), F.min("selling_price").alias("selling_price_min"))\
    .orderBy(F.desc("reviews_count_max"), F.asc("selling_price_min"))\
    .limit(5)

# Requête pour trouver les 5 produits avec la plus grande remise
agg_top_discounted_products = stream_df\
    .select("name", "selling_price", "original_price", "reviews_count", "discount_percentage")\
    .groupBy("name")\
    .agg(F.max("discount_percentage").alias("discount_percentage_max"))\
    .orderBy(F.desc("discount_percentage_max"))\
    .limit(5)



query_top_reviews = agg_top_reviews_and_least_expensive.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="2 second")\
    .start()

query_top_discounted = agg_top_discounted_products.writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="2 second")\
    .start()

query_top_reviews.awaitTermination()
query_top_discounted.awaitTermination()

