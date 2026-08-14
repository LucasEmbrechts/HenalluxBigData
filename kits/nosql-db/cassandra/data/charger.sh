#!/bin/sh
# Charge trajets.csv dans Cassandra.
#
# Ce script s'execute A L'INTERIEUR du conteneur cassandra-1, jamais sur votre
# machine. C'est ce qui le rend identique sous Windows, macOS et Linux.
#
# Format du CSV : camion_id,vitesse,temp_moteur
#
# Une difficulte propre a Cassandra : deux lignes qui ont la meme cle primaire
# n'en font qu'une, la seconde ecrase la premiere. Or un meme camion apparait
# 200 fois dans le fichier. Si la cle etait "camion_id" seul, les 5 000 releves
# se reduiraient a 25.
#
# On numerote donc les releves — comme le kit Redis le fait avec ses cles
# releve:1 ... releve:5000 — et la cle primaire devient le couple
# (camion_id, releve_id).

CSV=/kit/trajets.csv
TMP=/tmp/releves-numerotes.csv

echo "1/3 — Creation du keyspace et de la table..."

cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS bigdata
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

CREATE TABLE IF NOT EXISTS bigdata.releves (
  camion_id    text,
  releve_id    int,
  vitesse      double,
  temp_moteur  double,
  PRIMARY KEY ((camion_id), releve_id)
);"

echo "2/3 — Numerotation des releves..."

awk -F, 'NR>1 { print $1 "," NR-1 "," $2 "," $3 }' "$CSV" > "$TMP"

echo "3/3 — Import..."

cqlsh -e "COPY bigdata.releves (camion_id, releve_id, vitesse, temp_moteur) FROM '$TMP';"

echo
echo "Nombre de releves charges :"
cqlsh -e "SELECT COUNT(*) FROM bigdata.releves;"
