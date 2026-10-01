"""Compte les exces de vitesse par camion, avec l'API RDD.

C'est l'equivalent direct du kit MapReduce : on traite des lignes de texte,
et on ecrit soi-meme chaque etape (map, reduce). L'API DataFrame de
exces_sql.py fait la meme chose en bien moins de lignes.

Lancement (depuis spark-client) :
    spark-submit --master yarn /job/exces_rdd.py <entree_hdfs> <sortie_hdfs>
"""

import sys

from pyspark import SparkContext

if len(sys.argv) != 3:
    print("Usage : exces_rdd.py <entree_hdfs> <sortie_hdfs>")
    sys.exit(1)

entree, sortie = sys.argv[1], sys.argv[2]

sc = SparkContext(appName="Exces de vitesse - RDD")

lignes = sc.textFile(entree)

# La premiere ligne du CSV contient les noms des colonnes : on l'ecarte.
entete = lignes.first()

resultat = (
    lignes
    .filter(lambda ligne: ligne != entete)
    .map(lambda ligne: ligne.split(","))            # camion_id, vitesse, temp_moteur
    .filter(lambda champs: float(champs[1]) > 90)   # MAP : garder les exces
    .map(lambda champs: (champs[0], 1))             # une paire (camion, 1)
    .reduceByKey(lambda a, b: a + b)                # REDUCE : additionner par camion
    .sortBy(lambda paire: paire[1], ascending=False)
)

for camion, nombre in resultat.take(25):            # ACTION
    print(camion, nombre)

resultat.saveAsTextFile(sortie)

sc.stop()
