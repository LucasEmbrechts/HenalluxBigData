"""Exercice 1 — Les camions qui chauffent.

Enonce : pour chaque camion, trouvez sa temperature moteur MAXIMALE.
Ne gardez que les camions qui ont depasse 115 degres, du plus chaud au moins chaud.

Lancement :
    docker exec spark spark-submit /exercices/exercice1.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as maximum

spark = SparkSession.builder.appName("Exercice 1").getOrCreate()

releves = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/data/trajets.csv")
)

# groupBy puis agg : une ligne par camion, avec le maximum de ses temperatures.
# "max" est importe sous le nom "maximum" : Python a deja une fonction max().
resultat = (
    releves
    .groupBy("camion_id")
    .agg(maximum("temp_moteur").alias("temperature_max"))
    .filter(col("temperature_max") > 115)
    .orderBy(col("temperature_max").desc())
)

resultat.show()

spark.stop()
