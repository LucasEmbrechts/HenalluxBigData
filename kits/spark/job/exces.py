"""Les exces de vitesse des camions, avec Spark.

Le programme montre les operations les plus courantes :
lire, explorer, ajouter une colonne, filtrer, regrouper, joindre, ecrire.
Le calcul principal est le meme que ExcesSparkSQL.java du kit kits/hadoop/spark.
"""

from pyspark.sql import SparkSession
# "sum" est renomme : Python a deja une fonction sum(), on evite la confusion
from pyspark.sql.functions import avg, col, countDistinct, round, when
from pyspark.sql.functions import sum as somme

spark = SparkSession.builder.appName("Exces de vitesse").getOrCreate()

# =============================================================================
# 1. LIRE les donnees : chaque fichier devient un DataFrame, c'est-a-dire un tableau
# =============================================================================
releves = (
    spark.read
    .option("header", True)       # la premiere ligne contient les noms des colonnes
    .option("inferSchema", True)  # deviner les types (texte, nombre...)
    .csv("/data/trajets.csv")
)

camions = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/data/camions.csv")
)

releves.printSchema()
releves.show(5)

# =============================================================================
# 2. EXPLORER : un premier coup d'oeil sur les donnees
# =============================================================================
print(">>> Statistiques des colonnes")
releves.describe().show()

print(">>> Nombre de releves et de camions")
print("releves :", releves.count())
releves.select(countDistinct("camion_id").alias("nb_camions")).show()

# =============================================================================
# 3. AJOUTER UNE COLONNE calculee a partir des autres
# =============================================================================
releves = releves.withColumn(
    "categorie",
    when(col("vitesse") > 90, "exces")
    .when(col("vitesse") > 70, "normal")
    .otherwise("lent"),
)

print(">>> Nombre de releves par categorie")
releves.groupBy("categorie").count().orderBy(col("count").desc()).show()

# =============================================================================
# 4. FILTRER, REGROUPER, TRIER : les exces de vitesse par camion
# =============================================================================
print(">>> Version DataFrame")

exces = (
    releves
    .filter(col("vitesse") > 90)
    .groupBy("camion_id")
    .count()
    .orderBy(col("count").desc())
)

exces.show(10)   # show() declenche vraiment le calcul

# Le meme calcul, ecrit en SQL. On donne d'abord un nom de table au DataFrame.
print(">>> Version SQL")

releves.createOrReplaceTempView("releves")

spark.sql("""
    SELECT camion_id, COUNT(*) AS nb_exces
    FROM releves
    WHERE vitesse > 90
    GROUP BY camion_id
    ORDER BY nb_exces DESC
""").show(10)

# =============================================================================
# 5. JOINDRE deux tableaux : qui conduit, et depuis quel depot ?
# =============================================================================
print(">>> Les 5 camions les plus en exces, avec leur chauffeur")

(
    exces
    .join(camions, "camion_id")     # la colonne commune aux deux tableaux
    .select("camion_id", "chauffeur", "depot", "count")
    .orderBy(col("count").desc())
    .show(5)
)

print(">>> Exces par depot")

exces_par_depot = (
    exces
    .join(camions, "camion_id")
    .groupBy("depot")
    .agg(somme("count").alias("nb_exces"))   # additionne les exces des camions du depot
    .orderBy(col("nb_exces").desc())
)

exces_par_depot.show()

# =============================================================================
# 6. CALCULER des moyennes par camion
# =============================================================================
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

# =============================================================================
# 7. ECRIRE le resultat dans des fichiers
# "overwrite" : si le dossier existe deja (programme relance), il est remplace.
# =============================================================================
exces_par_depot.write.mode("overwrite").option("header", True).csv("/sortie/exces-par-depot")
exces_par_depot.write.mode("overwrite").parquet("/sortie/exces-par-depot-parquet")

print(">>> Resultat ecrit dans le dossier sortie/")

spark.stop()
