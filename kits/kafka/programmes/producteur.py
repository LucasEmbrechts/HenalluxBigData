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


if "wikipedia" not in producteur.list_topics(timeout=10).topics:
    raise SystemExit("Le topic 'wikipedia' n'existe pas")

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
        # Attention : produce n'envoie pas directement le message.
        # Il le range dans une file d'attente en mémoire,
        # puis rend la main tout de suite.
        # Kafka applique toujours la même règle : une même clé va toujours dans la même partition. 
        producteur.produce(
            "wikipedia",
            key=modification["wiki"],
            value=json.dumps(modification, ensure_ascii=False),
        )

        # Kafka répond par un accusé de réception pour chaque message.
        # poll(0) traite ces accusés de réception.
        # Le 0 signifie « ne pas attendre » : on traite ceux qui sont déjà arrivés
        # poll(0) après chaque envoi vide la file au fur et à mesure.
        producteur.poll(0)

        print("envoye :", modification["wiki"], "-", modification["titre"])

except KeyboardInterrupt:
    pass
finally:
    
    # flush() attend que tous les messages soient partis vers Kafka.
    # C'est un poll qui attend : à l'arrêt, il patiente jusqu'à ce que tous les messages de la file soient partis et confirmés.
    # Sans lui, les derniers messages encore en file seraient perdus en quittant le programme.
    producteur.flush()
