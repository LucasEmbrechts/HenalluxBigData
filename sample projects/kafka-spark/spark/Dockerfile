# Image Spark officielle + le connecteur Kafka.
#
# Spark ne sait pas lire Kafka "de base" : il lui faut le connecteur
# spark-sql-kafka et les bibliotheques dont il depend. On les ajoute une fois
# pour toutes dans /opt/spark/jars, la ou Spark charge ses bibliotheques.
# Les versions doivent correspondre a celle de Spark (3.5.1, Scala 2.12).

FROM apache/spark:3.5.1

USER root

ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD ${MAVEN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/

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
