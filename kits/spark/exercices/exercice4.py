"""Exercice 4 — Les categories, depot par depot.

Enonce : classez chaque releve en "exces" (plus de 90 km/h), "normal" (plus de
70) ou "lent", puis comptez les releves de chaque categorie DANS CHAQUE DEPOT.

Lancement :
    docker exec spark spark-submit /exercices/exercice4.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("Exercice 4").getOrCreate()


def lire(nom):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"/data/{nom}.csv")
    )


releves = lire("trajets")
camions = lire("camions")

# withColumn ajoute une colonne calculee ; when enchaine les cas.
classes = releves.withColumn(
    "categorie",
    when(col("vitesse") > 90, "exces")
    .when(col("vitesse") > 70, "normal")
    .otherwise("lent"),
)

# On peut regrouper sur PLUSIEURS colonnes a la fois.
resultat = (
    classes
    .join(camions, "camion_id")
    .groupBy("depot", "categorie")
    .count()
    .orderBy("depot", col("count").desc())
)

resultat.show(20)

# Variante : une colonne par categorie, plus lisible.
# pivot transforme les valeurs d'une colonne en colonnes.
(
    classes
    .join(camions, "camion_id")
    .groupBy("depot")
    .pivot("categorie")
    .count()
    .orderBy("depot")
    .show()
)

spark.stop()
