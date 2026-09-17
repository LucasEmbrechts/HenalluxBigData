"""Producteur : envoie dans Kafka, en direct, les modifications faites sur Wikipedia.

Source : https://stream.wikimedia.org/v2/stream/recentchange
Chaque modification d'une page, n'importe ou dans le monde, arrive sur ce flux.
"""

import json
import os

import requests
from confluent_kafka import Producer

SOURCE = "https://stream.wikimedia.org/v2/stream/recentchange"
ENTETES = {"User-Agent": "HenalluxBigData-kit-kafka/1.0"}   # Wikimedia demande de se presenter

# Adresse de Kafka : "kafka:19092" dans Docker, "localhost:9092" sur votre machine
KAFKA = os.environ.get("KAFKA", "localhost:9092")

producteur = Producer({"bootstrap.servers": KAFKA})

# Sans topic, les messages seraient perdus sans aucun message d'erreur.
if "wikipedia" not in producteur.list_topics(timeout=10).topics:
    raise SystemExit("Le topic 'wikipedia' n'existe pas : creez-le d'abord (section 2 du README).")

flux = requests.get(SOURCE, stream=True, headers=ENTETES)

try:
    for ligne in flux.iter_lines():
        # Seules les lignes qui commencent par "data: " contiennent un evenement
        if not ligne.startswith(b"data: "):
            continue
        evenement = json.loads(ligne[6:])

        # Wikimedia envoie parfois des evenements de test, sans ces champs : on les ignore
        if "wiki" not in evenement:
            continue

        # On ne garde que quelques champs
        modification = {
            "wiki": evenement.get("wiki"),            # ex. frwiki = Wikipedia en francais
            "titre": evenement.get("title"),
            "utilisateur": evenement.get("user"),
            "robot": evenement.get("bot"),            # True si c'est un programme qui modifie
        }

        # L'envoi dans Kafka : un topic, une cle, une valeur
        producteur.produce(
            "wikipedia",
            key=modification["wiki"],
            value=json.dumps(modification, ensure_ascii=False),
        )
        producteur.poll(0)   # laisse Kafka traiter les envois en cours

        print("envoye :", modification["wiki"], "-", modification["titre"])

except KeyboardInterrupt:
    pass
finally:
    # produce() ne fait que mettre le message en attente :
    # flush() attend qu'ils soient tous vraiment partis vers Kafka.
    producteur.flush()
