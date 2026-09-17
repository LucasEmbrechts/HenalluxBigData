"""Job Spark : lit les tables "magasins" et "ventes" dans une base SQL,
calcule le chiffre d'affaires par ville, et ecrit le resultat dans une
nouvelle table "ca_par_ville" de la meme base.

Lancement :
    docker compose run --rm spark postgres
    docker compose run --rm spark mysql
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, sum

# ---------------------------------------------------------------------------
# LES DEUX VERSIONS : PostgreSQL et MySQL
# Seules ces lignes different d'une base a l'autre. Tout le reste du
# programme est identique.
# ---------------------------------------------------------------------------
CONNEXIONS = {
    "postgres": {
        "url": "jdbc:postgresql://postgres:5432/bigdata",
        "driver": "org.postgresql.Driver",
        "user": "bigdata",
        "password": "bigdata",
    },
    "mysql": {
        "url": "jdbc:mysql://mysql:3306/bigdata",
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "bigdata",
        "password": "bigdata",
    },
}

base = sys.argv[1] if len(sys.argv) > 1 else "postgres"
if base not in CONNEXIONS:
    print(f"Base inconnue : '{base}'. Choisissez postgres ou mysql.")
    sys.exit(1)
connexion = CONNEXIONS[base]
print(f"\n>>> Base utilisee : {base}\n")

spark = (
    SparkSession.builder
    .appName(f"Chiffre d'affaires par ville - {base}")
    .getOrCreate()
)


def lire_table(nom):
    """Lit une table SQL entiere et la renvoie sous forme de DataFrame."""
    return (
        spark.read
        .format("jdbc")
        .options(**connexion)
        .option("dbtable", nom)
        .load()
    )


# 1. LIRE les deux tables dans la base SQL
magasins = lire_table("magasins")
ventes = lire_table("ventes")

print(f"{ventes.count()} ventes lues dans {magasins.count()} magasins")

# 2. CALCULER : joindre les ventes a leur magasin, puis regrouper par ville

# Version DataFrame : des methodes chainees
resultat = (
    ventes
    .join(magasins, "magasin_id")
    .groupBy("ville")
    .agg(
        count("*").alias("nb_ventes"),
        round(sum("montant"), 2).alias("chiffre_affaires"),
    )
    .orderBy(col("chiffre_affaires").desc())
)

print("Version DataFrame :")
resultat.show()

# Version Spark SQL : le meme calcul, ecrit en SQL.
# On donne d'abord un nom de table a chaque DataFrame.
ventes.createOrReplaceTempView("ventes")
magasins.createOrReplaceTempView("magasins")

resultat_sql = spark.sql("""
    SELECT ville, COUNT(*) AS nb_ventes, ROUND(SUM(montant), 2) AS chiffre_affaires
    FROM ventes JOIN magasins USING (magasin_id)
    GROUP BY ville
    ORDER BY chiffre_affaires DESC
""")

print("Version Spark SQL :")
resultat_sql.show()

# 3. ECRIRE le resultat dans une nouvelle table de la meme base.
# "overwrite" : si la table existe deja (job relance), elle est remplacee.
(
    resultat.write
    .format("jdbc")
    .options(**connexion)
    .option("dbtable", "ca_par_ville")
    .mode("overwrite")
    .save()
)

print(f">>> Resultat enregistre dans la table ca_par_ville ({base})\n")

spark.stop()
