"""Compte les exces de vitesse par camion, avec l'API DataFrame et en SQL.

Lancement (depuis spark-client) :
    spark-submit --master yarn /job/exces_sql.py <entree_hdfs> <sortie_hdfs>
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

if len(sys.argv) != 3:
    print("Usage : exces_sql.py <entree_hdfs> <sortie_hdfs>")
    sys.exit(1)

entree, sortie = sys.argv[1], sys.argv[2]

spark = SparkSession.builder.appName("Exces de vitesse - DataFrame").getOrCreate()

# 1. LIRE le fichier depuis HDFS
releves = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(entree)
)

releves.printSchema()

# 2. CALCULER : les exces de vitesse par camion
resultat = (
    releves
    .filter(col("vitesse") > 90)
    .groupBy("camion_id")
    .count()
    .orderBy(col("count").desc())
)

resultat.show(50)   # ACTION : c'est ici que le calcul demarre

# 3. LE MEME CALCUL, EN SQL
releves.createOrReplaceTempView("releves")

spark.sql("""
    SELECT camion_id, COUNT(*) AS nb_exces
    FROM releves
    WHERE vitesse > 90
    GROUP BY camion_id
    ORDER BY nb_exces DESC
""").show(50)

# 4. LA VITESSE MOYENNE par camion
(
    releves
    .groupBy("camion_id")
    .agg(avg("vitesse").alias("vitesse_moyenne"))
    .orderBy(col("vitesse_moyenne").desc())
    .show(50)
)

# 5. ECRIRE le resultat dans HDFS
resultat.write.mode("overwrite").option("header", True).csv(sortie)

spark.stop()
