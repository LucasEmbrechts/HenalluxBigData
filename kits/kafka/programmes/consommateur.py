"""Consommateur : lit le topic "wikipedia" et affiche chaque message recu."""

import json
import os

from confluent_kafka import Consumer

# Adresse de Kafka : "kafka:19092" dans Docker, "localhost:9092" sur votre machine
KAFKA = os.environ.get("KAFKA", "localhost:9092")

consommateur = Consumer({
    "bootstrap.servers": KAFKA,
    "group.id": "lecteurs",             # Kafka retient, pour ce groupe, jusqu'ou on a lu
    "auto.offset.reset": "earliest",    # la toute premiere fois : partir du debut du topic
})
consommateur.subscribe(["wikipedia"])

try:
    while True:
        message = consommateur.poll(1.0)
            # attend un message, au plus 1 seconde
            # s'il y a un message : poll le renvoie immédiatement ;
            # s'il n'y en a pas : poll attend jusqu'à 1 seconde qu'un message arrive ;
            # si rien n'arrive pendant cette seconde : poll renvoie None.
        if message is None:
            continue

        modification = json.loads(message.value())

        # Pour pouvoir répartir le travail, un topic est découpé en plusieurs partitions.
        # Dans le kit, le topic wikipedia en a 3 (--partitions 3) :
        # L'offset est le numéro d'ordre d'un message dans sa partition :
        # 0 pour le premier, 1 pour le deuxième, etc.
        # Chaque partition a sa propre numérotation. 
        print(f"partition {message.partition()} | offset {message.offset()} | "
              f"{modification['wiki']} | {modification['titre']}")

except KeyboardInterrupt:
    pass
finally:
    # Quitte proprement le groupe et enregistre jusqu'ou on a lu
    consommateur.close()
