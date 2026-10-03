"""Exercice 1 — Un producteur sans cle.

Enonce : envoyez les modifications de Wikipedia dans un nouveau topic
"wikipedia-sans-cle", mais SANS cle. Comparez ensuite le remplissage des
partitions des deux topics.

Avant de lancer, creez le topic :
    docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \\
        --create --topic wikipedia-sans-cle --partitions 3

Lancement :
    docker compose run --rm producteur python /exercices/exercice1.py

Pour comparer (dans un autre terminal) :
    docker exec kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic wikipedia
    docker exec kafka kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic wikipedia-sans-cle
"""

import json
import os

import requests
from confluent_kafka import Producer

SOURCE = "https://stream.wikimedia.org/v2/stream/recentchange"
ENTETES = {"User-Agent": "HenalluxBigData-kit-kafka/1.0"}
TOPIC = "wikipedia-sans-cle"
KAFKA = os.environ.get("KAFKA", "localhost:9092")

producteur = Producer({"bootstrap.servers": KAFKA})

if TOPIC not in producteur.list_topics(timeout=10).topics:
    raise SystemExit(f"Le topic '{TOPIC}' n'existe pas : creez-le d'abord (voir l'en-tete du fichier).")

flux = requests.get(SOURCE, stream=True, headers=ENTETES)

try:
    for ligne in flux.iter_lines():
        if not ligne.startswith(b"data: "):
            continue
        evenement = json.loads(ligne[6:])
        if "wiki" not in evenement:
            continue

        modification = {
            "wiki": evenement.get("wiki"),
            "titre": evenement.get("title"),
            "utilisateur": evenement.get("user"),
            "robot": evenement.get("bot"),
        }

        # LA SEULE DIFFERENCE avec producteur.py : pas de key=.
        # Sans cle, Kafka repartit les messages au hasard entre les partitions,
        # par petits paquets. Les partitions se remplissent donc de facon
        # equilibree... mais les modifications d'un meme wiki ne sont plus
        # rangees ensemble, et leur ordre n'est plus garanti.
        producteur.produce(TOPIC, value=json.dumps(modification, ensure_ascii=False))
        producteur.poll(0)

        print("envoye :", modification["wiki"], "-", modification["titre"])

except KeyboardInterrupt:
    pass
finally:
    producteur.flush()
