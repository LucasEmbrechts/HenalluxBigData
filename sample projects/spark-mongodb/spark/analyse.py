"""Job Spark : lit les fichiers magasins.csv et ventes.csv, calcule le chiffre
d'affaires par ville, et enregistre le resultat dans MongoDB.

Un document par ville, qui contient aussi la liste de ses magasins.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, round, struct, sum

MONGO = os.environ.get("MONGO", "mongodb://localhost:27017")

spark = (
    SparkSession.builder
    .appName("Chiffre d'affaires par ville - MongoDB")
    .getOrCreate()
)


def lire_csv(nom):
    """Lit un fichier CSV du dossier donnees/ et le renvoie sous forme de DataFrame."""
    return (
        spark.read
        .option("header", True)       # la premiere ligne contient les noms des colonnes
        .option("inferSchema", True)  # deviner les types (texte, nombre, date...)
        .csv(f"/donnees/{nom}.csv")
    )


# 1. LIRE les deux fichiers
magasins = lire_csv("magasins")
ventes = lire_csv("ventes")

print(f"{ventes.count()} ventes lues dans {magasins.count()} magasins")

# 2. CALCULER

# a) Le chiffre d'affaires de chaque magasin
par_magasin = (
    ventes
    .join(magasins, "magasin_id")
    .groupBy("ville", "nom")
    .agg(
        count("*").alias("nb_ventes"),
        sum("montant").alias("chiffre_affaires"),
    )
)

# b) Le chiffre d'affaires de chaque ville, avec la liste de ses magasins
par_ville = (
    par_magasin
    .groupBy("ville")
    .agg(
        sum("nb_ventes").alias("nb_ventes"),
        round(sum("chiffre_affaires"), 2).alias("chiffre_affaires"),
        # Rassemble les magasins de la ville dans une liste
        collect_list(
            struct(
                "nom",
                "nb_ventes",
                round("chiffre_affaires", 2).alias("chiffre_affaires"),
            )
        ).alias("magasins"),
    )
    .orderBy(col("chiffre_affaires").desc())
)

par_ville.show(truncate=False)

# 3. ECRIRE le resultat dans MongoDB : une ligne du DataFrame = un document.
# "overwrite" : si la collection existe deja (job relance), elle est remplacee.
(
    par_ville.write
    .format("mongodb")
    .option("connection.uri", MONGO)
    .option("database", "bigdata")
    .option("collection", "ventes_par_ville")
    .mode("overwrite")
    .save()
)

print(">>> Resultat enregistre dans MongoDB : base bigdata, collection ventes_par_ville\n")

spark.stop()
