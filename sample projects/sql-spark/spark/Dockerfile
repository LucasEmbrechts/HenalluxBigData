# Image Spark officielle + les pilotes JDBC de PostgreSQL et de MySQL.
#
# Spark parle aux bases SQL par JDBC, mais il lui faut le "pilote" de chaque
# base : une bibliotheque Java fournie par l'editeur. On les ajoute une fois
# pour toutes dans /opt/spark/jars, la ou Spark charge ses bibliotheques.

FROM apache/spark:3.5.1

USER root

ARG MAVEN=https://repo1.maven.org/maven2
ADD ${MAVEN}/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar /opt/spark/jars/
ADD ${MAVEN}/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar /opt/spark/jars/

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
