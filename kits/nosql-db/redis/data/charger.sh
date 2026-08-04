#!/bin/sh
# Charge trajets.csv dans Redis.
#
# Ce script s'execute A L'INTERIEUR du conteneur redis, jamais sur votre
# machine. C'est ce qui le rend identique sous Windows, macOS et Linux.
#
# Principe : Redis ne connait ni table, ni colonne, ni schema. On ne peut donc
# pas "importer" le CSV tel quel. On transforme chaque ligne en une commande
# Redis, et on envoie le tout d'un bloc avec "redis-cli --pipe".
#
# Format du CSV : camion_id,vitesse,temp_moteur
#
# Chaque releve devient un HASH (un objet a champs nommes) range sous une cle
# numerotee : releve:1, releve:2, ... releve:5000

CSV=/kit/trajets.csv

echo "Chargement des releves..."

awk -F, 'NR>1 {
    print "HSET releve:" NR-1 " camion_id " $1 " vitesse " $2 " temp_moteur " $3
}' "$CSV" | redis-cli --pipe

echo
echo "Termine. Nombre de cles dans la base :"
redis-cli DBSIZE
