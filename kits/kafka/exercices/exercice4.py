"""Exercice 4 — Un consommateur qui alerte.

Enonce : lisez le topic "cours" et n'affichez une ligne que lorsque le prix
d'une paire a change de plus de 0,05 % depuis le message precedent.

Il faut donc retenir le dernier prix de chaque paire : c'est le programme qui
garde cette memoire, Kafka ne fait que livrer les messages.

Lancement (pendant que l'exercice 3 tourne) :
    docker compose run --rm consommateur python /exercices/exercice4.py
"""

import json
import os

from confluent_kafka import Consumer

SEUIL = 0.05        # en pourcentage
KAFKA = os.environ.get("KAFKA", "localhost:9092")

consommateur = Consumer({
    "bootstrap.servers": KAFKA,
    "group.id": "alertes",
    "auto.offset.reset": "earliest",
})
consommateur.subscribe(["cours"])

dernier_prix = {}   # la memoire du programme : un prix par paire

print("Surveillance des cours... (Ctrl+C pour arreter)")

try:
    while True:
        message = consommateur.poll(1.0)
        if message is None:
            continue

        cours = json.loads(message.value())
        paire, prix = cours["paire"], cours["prix"]
        precedent = dernier_prix.get(paire)
        dernier_prix[paire] = prix

        if precedent is None:
            continue   # premier prix recu pour cette paire : rien a comparer

        variation = (prix - precedent) / precedent * 100
        if abs(variation) >= SEUIL:
            sens = "hausse" if variation > 0 else "baisse"
            print(f"{paire} : {precedent} -> {prix}  ({sens} de {abs(variation):.2f} %)")

except KeyboardInterrupt:
    pass
finally:
    consommateur.close()
