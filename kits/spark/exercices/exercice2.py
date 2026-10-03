"""Exercice 2 — Le meme calcul, en SQL.

Enonce : reecrivez l'exercice 1 avec spark.sql(...), et verifiez que le resultat
est identique.

Lancement :
    docker exec spark spark-submit /exercices/exercice2.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Exercice 2").getOrCreate()

releves = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/data/trajets.csv")
)

# Pour ecrire du SQL, il faut d'abord donner un nom de table au DataFrame.
releves.createOrReplaceTempView("releves")

# HAVING filtre APRES le regroupement, comme le .filter() place apres le .agg()
# de l'exercice 1. WHERE, lui, filtrerait les releves un par un, avant.
spark.sql("""
    SELECT camion_id, MAX(temp_moteur) AS temperature_max
    FROM releves
    GROUP BY camion_id
    HAVING MAX(temp_moteur) > 115
    ORDER BY temperature_max DESC
""").show()

spark.stop()
