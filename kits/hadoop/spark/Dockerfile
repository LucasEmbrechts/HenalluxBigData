# Conteneur "client" : c'est de la que l'on soumet les jobs.
#
# Il n'execute aucun calcul. Il parle au ResourceManager, qui fait travailler
# le NodeManager. On part de l'image Hadoop officielle (pour heriter de la
# config HDFS/YARN generee a partir de ./config) et on y ajoute les binaires
# Spark officiels, pris directement dans l'image apache/spark.

FROM apache/hadoop:3.4.3

USER root

# Recupere /opt/spark depuis l'image Spark officielle : pas de tarball a
# telecharger ni a versionner dans le depot.
COPY --from=apache/spark:3.5.1 /opt/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${PATH}:/opt/spark/bin"

# HADOOP_CONF_DIR est deja defini dans l'image (/opt/hadoop/etc/hadoop).
# C'est la que spark-submit va lire yarn-site.xml pour trouver le
# ResourceManager. Ne pas definir SPARK_CONF_DIR ni d'autres variables
# contenant "_CONF_" : l'entrypoint de l'image Hadoop les prendrait pour des
# fichiers de configuration a generer.
