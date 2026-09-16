"""Archiveur : lit le topic "wikipedia" et enregistre chaque modification dans MongoDB.

consommateur.py compte en memoire : a l'arret, tout est perdu. Ce programme-ci
ecrit chaque message dans une base NoSQL, ou il reste apres l'arret et ou on
peut l'interroger.
"""

import json
import os
import time

from confluent_kafka import Consumer
from pymongo import MongoClient

TOPIC = "wikipedia"
KAFKA = os.environ.get("KAFKA", "localhost:9092")
# Dans Docker, docker-compose.yml fournit "mongodb://mongo:27017".
MONGO = os.environ.get("MONGO", "mongodb://localhost:27017")

# Cote MongoDB : la base "wikipedia", la collection "modifications".
# Elles sont creees automatiquement au premier document insere.
collection = MongoClient(MONGO)["wikipedia"]["modifications"]

consommateur = Consumer({
    "bootstrap.servers": KAFKA,
    # Un AUTRE groupe que consommateur.py ("compteur") : les deux programmes
    # recoivent donc chacun tous les messages.
    "group.id": "archiveur",
    "auto.offset.reset": "earliest",
})
consommateur.subscribe([TOPIC])

enregistres = 0
dernier_affichage = time.time()

print("En attente de messages... (Ctrl+C pour arreter)")

try:
    while True:
        message = consommateur.poll(1.0)

        if message is not None and not message.error():
            # Le message Kafka est du JSON : il devient tel quel un document MongoDB.
            modification = json.loads(message.value())
            collection.insert_one(modification)
            enregistres += 1

        if time.time() - dernier_affichage >= 5:
            print(f"{enregistres} modifications enregistrees dans MongoDB")
            dernier_affichage = time.time()

except KeyboardInterrupt:
    pass
finally:
    consommateur.close()
