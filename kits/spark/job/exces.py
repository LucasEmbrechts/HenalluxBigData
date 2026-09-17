"""Compte les exces de vitesse par camion.

Le meme calcul que ExcesSparkSQL.java dans le kit kits/hadoop/spark,
ecrit en Python.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = (
    SparkSession.builder
    .appName("Exces de vitesse")
    # Chaque worker n'offre que 1 Go : on demande 512 Mo par executor.
    .config("spark.executor.memory", "512m")
    .getOrCreate()
)

# 1. LIRE le fichier
releves = (
    spark.read
    .option("header", True)       # la premiere ligne contient les noms des colonnes
    .option("inferSchema", True)  # deviner les types (texte, nombre...)
    .csv("/data/trajets.csv")
)

releves.printSchema()

# 2. CALCULER le nombre d'exces (vitesse > 90) par camion
exces = (
    releves
    .filter(col("vitesse") > 90)
    .groupBy("camion_id")
    .count()
    .orderBy(col("count").desc())
)

# 3. AFFICHER : c'est ici que le calcul demarre vraiment
exces.show(25)

# La vitesse moyenne par camion
(
    releves
    .groupBy("camion_id")
    .agg(avg("vitesse").alias("vitesse_moyenne"))
    .orderBy(col("vitesse_moyenne").desc())
    .show(5)
)

spark.stop()
