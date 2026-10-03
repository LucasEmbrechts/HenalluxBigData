"""Exercice 2 — Tout relire depuis le debut.

Enonce : comptez combien de messages contient deja le topic "wikipedia", en le
relisant depuis le premier message, sans deranger le groupe "lecteurs".

Lancement :
    docker compose run --rm consommateur python /exercices/exercice2.py
"""

import os
import time

from confluent_kafka import Consumer

KAFKA = os.environ.get("KAFKA", "localhost:9092")

consommateur = Consumer({
    "bootstrap.servers": KAFKA,
    # UN AUTRE GROUPE : il n'a jamais rien lu, donc "earliest" le fait partir
    # du tout premier message. Le groupe "lecteurs" n'est pas affecte : Kafka
    # retient une position par groupe.
    "group.id": "relecture",
    "auto.offset.reset": "earliest",
})
consommateur.subscribe(["wikipedia"])

total = 0
par_partition = {}
dernier_message = time.time()

print("Relecture du topic depuis le debut... (Ctrl+C pour arreter)")

try:
    while True:
        message = consommateur.poll(1.0)
        if message is None:
            # Plus rien a lire depuis 5 secondes : on a rattrape la fin du topic.
            if time.time() - dernier_message > 5:
                break
            continue

        total += 1
        par_partition[message.partition()] = par_partition.get(message.partition(), 0) + 1
        dernier_message = time.time()

except KeyboardInterrupt:
    pass
finally:
    consommateur.close()

print(f"\n{total} messages relus")
for partition in sorted(par_partition):
    print(f"  partition {partition} : {par_partition[partition]}")
