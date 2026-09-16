"""Consommateur : lit le topic "wikipedia" et compte les modifications par wiki.

Toutes les 5 secondes, il affiche les wikis les plus modifies et la part des
modifications faites par des robots.
"""

import json
import os
import time
from collections import Counter

from confluent_kafka import Consumer

TOPIC = "wikipedia"
KAFKA = os.environ.get("KAFKA", "localhost:9092")

consommateur = Consumer({
    "bootstrap.servers": KAFKA,
    # Le nom du groupe. Kafka retient, pour ce nom, jusqu'ou on a lu.
    "group.id": "compteur",
    # La toute premiere fois que ce groupe lit : partir du debut du topic.
    "auto.offset.reset": "earliest",
})
consommateur.subscribe([TOPIC])

par_wiki = Counter()
robots = 0
total = 0
dernier_affichage = time.time()

print("En attente de messages... (Ctrl+C pour arreter)")

try:
    while True:
        message = consommateur.poll(1.0)   # attend un message, au plus 1 seconde

        if message is not None and not message.error():
            modification = json.loads(message.value())
            par_wiki[modification["wiki"]] += 1
            robots += modification["robot"] is True
            total += 1

        if time.time() - dernier_affichage >= 5:
            # Les partitions que Kafka a confiees a ce consommateur
            partitions = sorted(p.partition for p in consommateur.assignment())
            print(f"\nPartitions lues : {partitions}")
            print(f"{total} modifications lues, dont {100 * robots // max(total, 1)} % par des robots")
            for wiki, nombre in par_wiki.most_common(5):
                print(f"  {wiki:<15} {nombre:>7}")
            dernier_affichage = time.time()

except KeyboardInterrupt:
    pass
finally:
    # Quitte proprement le groupe et enregistre jusqu'ou on a lu.
    consommateur.close()
