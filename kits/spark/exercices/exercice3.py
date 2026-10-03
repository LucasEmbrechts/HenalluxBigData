"""Exercice 3 — Les chauffeurs les plus rapides.

Enonce : affichez les 5 chauffeurs dont la vitesse moyenne est la plus elevee,
avec leur depot. Le nom du chauffeur se trouve dans camions.csv : il faut donc
joindre les deux fichiers.

Lancement :
    docker exec spark spark-submit /exercices/exercice3.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, round

spark = SparkSession.builder.appName("Exercice 3").getOrCreate()


def lire(nom):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"/data/{nom}.csv")
    )


releves = lire("trajets")
camions = lire("camions")

# join rapproche les deux tableaux par leur colonne commune, camion_id.
resultat = (
    releves
    .join(camions, "camion_id")
    .groupBy("chauffeur", "depot")
    .agg(
        round(avg("vitesse"), 1).alias("vitesse_moyenne"),
        count("*").alias("nb_releves"),
    )
    .orderBy(col("vitesse_moyenne").desc())
)

resultat.show(5)

spark.stop()
