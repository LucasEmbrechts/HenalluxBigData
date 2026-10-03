"""Exercice 5 — Ecrire, puis relire.

Enonce : ecrivez dans sortie/exercice5 la liste des camions avec leur nombre
d'exces de vitesse, au format CSV. Relisez ensuite le fichier ecrit, et affichez
son contenu pour verifier.

Lancement :
    docker exec spark spark-submit /exercices/exercice5.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Exercice 5").getOrCreate()

releves = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/data/trajets.csv")
)

exces = (
    releves
    .filter(col("vitesse") > 90)
    .groupBy("camion_id")
    .count()
    .orderBy(col("count").desc())
)

# 1. ECRIRE. "overwrite" remplace le dossier s'il existe deja : sans cette
# option, le programme echouerait au deuxieme lancement.
exces.write.mode("overwrite").option("header", True).csv("/sortie/exercice5")

print(">>> Ecrit dans sortie/exercice5")

# 2. RELIRE ce qui vient d'etre ecrit. Spark lit tout le dossier d'un coup,
# quel que soit le nombre de fichiers qu'il contient.
relu = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/sortie/exercice5")
)

print(">>> Relu depuis le fichier :", relu.count(), "lignes")
relu.orderBy(col("count").desc()).show(5)

spark.stop()
