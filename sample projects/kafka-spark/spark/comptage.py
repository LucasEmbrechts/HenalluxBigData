"""Job Spark : lit le topic "wikipedia" dans Kafka et calcule en continu,
pour chaque wiki, le nombre de modifications et la part faite par des robots.

Le resultat est affiche dans le terminal toutes les 10 secondes.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, from_json, round

KAFKA = os.environ.get("KAFKA", "localhost:9092")

spark = (
    SparkSession.builder
    .appName("Wikipedia - comptage en continu")
    # 200 par defaut : bien trop pour une seule machine, chaque lot serait lent.
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

# 1. LIRE KAFKA
# readStream (et non read) : le DataFrame n'a pas de fin, il grandit a chaque
# nouveau message. Colonnes fournies par Kafka : key, value, topic, partition,
# offset, timestamp.
messages = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", "wikipedia")
    .option("startingOffsets", "earliest")   # commencer au debut du topic
    .load()
)

# 2. DECODER LES MESSAGES
# La valeur est du JSON envoye par producteur.py. On decrit ses champs, et
# Spark en fait des colonnes.
SCHEMA = "wiki STRING, titre STRING, utilisateur STRING, robot BOOLEAN"

modifications = (
    messages
    .select(from_json(col("value").cast("string"), SCHEMA).alias("m"))
    .select("m.*")
)

# 3. CALCULER
# Exactement le meme code qu'en batch sur un fichier.
resultat = (
    modifications
    .groupBy("wiki")
    .agg(
        count("*").alias("modifications"),
        round(avg(col("robot").cast("int")) * 100).cast("int").alias("pct_robots"),
    )
    .orderBy(col("modifications").desc())
)

# 4. AFFICHER, toutes les 10 secondes
# "complete" : a chaque fois, on reaffiche le tableau entier mis a jour.
requete = (
    resultat.writeStream
    .outputMode("complete")
    .format("console")
    .option("numRows", 10)
    .option("truncate", False)
    .trigger(processingTime="10 seconds")
    .start()
)

requete.awaitTermination()
