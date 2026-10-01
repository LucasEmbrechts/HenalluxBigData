"""Compte les exces de vitesse par camion, de deux facons :
d'abord avec l'API DataFrame, puis en SQL.

Le meme calcul que ExcesSparkSQL.java dans le kit kits/hadoop/spark,
ecrit en Python.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, round

spark = SparkSession.builder.appName("Exces de vitesse").getOrCreate()

# 1. LIRE le fichier : il devient un DataFrame, c'est-a-dire un tableau
releves = (
    spark.read
    .option("header", True)       # la premiere ligne contient les noms des colonnes
    .option("inferSchema", True)  # deviner les types (texte, nombre...)
    .csv("/data/trajets.csv")
)

releves.printSchema()
releves.show(5)

# 2. CALCULER le nombre d'exces (vitesse > 90) par camion
print(">>> Version DataFrame")

exces = (
    releves
    .filter(col("vitesse") > 90)
    .groupBy("camion_id")
    .count()
    .orderBy(col("count").desc())
)

exces.show(10)   # show() declenche vraiment le calcul

# 3. LE MEME CALCUL, EN SQL
# On donne d'abord un nom de table au DataFrame.
print(">>> Version SQL")

releves.createOrReplaceTempView("releves")

spark.sql("""
    SELECT camion_id, COUNT(*) AS nb_exces
    FROM releves
    WHERE vitesse > 90
    GROUP BY camion_id
    ORDER BY nb_exces DESC
""").show(10)

# 4. UN AUTRE CALCUL : les moyennes par camion
print(">>> Moyennes par camion")

(
    releves
    .groupBy("camion_id")
    .agg(
        round(avg("vitesse"), 1).alias("vitesse_moyenne"),
        round(avg("temp_moteur"), 1).alias("temperature_moyenne"),
    )
    .orderBy(col("vitesse_moyenne").desc())
    .show(5)
)

spark.stop()
