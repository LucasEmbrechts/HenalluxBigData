"""Exercice 5 — Plus de consommateurs que de partitions.

Enonce : le topic "wikipedia" a 3 partitions. Lancez QUATRE consommateurs dans
le meme groupe, et montrez ce que recoit le quatrieme.

Ce programme affiche les partitions que Kafka lui a attribuees, puis les
messages recus. Lancez-le quatre fois, dans quatre terminaux.

Lancement :
    docker compose run --rm consommateur python /exercices/exercice5.py
"""

import json
import os
import time

from confluent_kafka import Consumer

KAFKA = os.environ.get("KAFKA", "localhost:9092")

consommateur = Consumer({
    "bootstrap.servers": KAFKA,
    "group.id": "partage",
    "auto.offset.reset": "latest",   # ici, seuls les nouveaux messages nous interessent
})
consommateur.subscribe(["wikipedia"])

dernier_rappel = 0

print("En attente de l'attribution des partitions... (Ctrl+C pour arreter)")

try:
    while True:
        message = consommateur.poll(1.0)

        # assignment() donne les partitions attribuees a CE consommateur.
        # Une liste vide signifie que Kafka ne lui en a donne aucune : il y a
        # plus de consommateurs que de partitions, celui-ci attend sans rien faire.
        if time.time() - dernier_rappel >= 5:
            partitions = sorted(p.partition for p in consommateur.assignment())
            print(f"--- partitions attribuees : {partitions if partitions else 'aucune'}")
            dernier_rappel = time.time()

        if message is None:
            continue

        modification = json.loads(message.value())
        print(f"partition {message.partition()} | {modification['wiki']} | {modification['titre']}")

except KeyboardInterrupt:
    pass
finally:
    consommateur.close()
