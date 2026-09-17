"""Archiveur : lit le topic "wikipedia" et enregistre chaque modification dans MongoDB.

Kafka ne garde les messages que 24 heures et ne sait pas les interroger.
Une fois dans MongoDB, ils restent, et on peut les chercher, les trier, les compter.
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
    # Le nom du groupe. Kafka retient, pour ce nom, jusqu'ou on a lu.
    "group.id": "archiveur",
    # La toute premiere fois que ce groupe lit : partir du debut du topic.
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
