# Image Spark officielle + le connecteur MongoDB.
#
# Spark ne sait pas ecrire dans MongoDB "de base" : il lui faut le connecteur
# fourni par MongoDB, et le pilote MongoDB pour Java dont il depend. On les
# ajoute une fois pour toutes dans /opt/spark/jars, la ou Spark charge ses
# bibliotheques.

FROM apache/spark:3.5.1

USER root

ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/org/mongodb/spark/mongo-spark-connector_2.12/10.7.0/mongo-spark-connector_2.12-10.7.0.jar /opt/spark/jars/
ADD ${MAVEN}/org/mongodb/mongodb-driver-sync/5.1.4/mongodb-driver-sync-5.1.4.jar /opt/spark/jars/
ADD ${MAVEN}/org/mongodb/mongodb-driver-core/5.1.4/mongodb-driver-core-5.1.4.jar /opt/spark/jars/
ADD ${MAVEN}/org/mongodb/bson/5.1.4/bson-5.1.4.jar /opt/spark/jars/

# Les fichiers telecharges par ADD ne sont lisibles que par root.
RUN chmod 644 /opt/spark/jars/*.jar

# Spark affiche des centaines de messages INFO au demarrage : on ne garde que
# les avertissements, pour que le resultat reste lisible dans le terminal.
RUN mkdir -p /opt/spark/conf && printf '%s\n' \
    'rootLogger.level = warn' \
    'rootLogger.appenderRef.stdout.ref = console' \
    'appender.console.type = Console' \
    'appender.console.name = console' \
    'appender.console.target = SYSTEM_ERR' \
    'appender.console.layout.type = PatternLayout' \
    'appender.console.layout.pattern = %p %c{1}: %m%n' \
    > /opt/spark/conf/log4j2.properties

USER spark
