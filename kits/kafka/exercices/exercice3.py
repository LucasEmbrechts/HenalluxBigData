"""Exercice 3 — Une autre source : le cours des cryptomonnaies.

Enonce : ecrivez un producteur qui interroge toutes les 2 secondes l'API
publique de Coinbase pour trois paires, et envoie chaque prix dans un topic
"cours". La cle du message est la paire.

Avant de lancer, creez le topic :
    docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 \\
        --create --topic cours --partitions 3

Lancement :
    docker compose run --rm producteur python /exercices/exercice3.py
"""

import json
import os
import time

import requests
from confluent_kafka import Producer

PAIRES = ["BTC-EUR", "ETH-EUR", "SOL-EUR"]
API = "https://api.exchange.coinbase.com/products/{}/ticker"
ENTETES = {"User-Agent": "HenalluxBigData-kit-kafka/1.0"}
TOPIC = "cours"
KAFKA = os.environ.get("KAFKA", "localhost:9092")

producteur = Producer({"bootstrap.servers": KAFKA})

if TOPIC not in producteur.list_topics(timeout=10).topics:
    raise SystemExit(f"Le topic '{TOPIC}' n'existe pas : creez-le d'abord (voir l'en-tete du fichier).")

print("Interrogation de Coinbase... (Ctrl+C pour arreter)")

try:
    while True:
        for paire in PAIRES:
            try:
                reponse = requests.get(API.format(paire), headers=ENTETES, timeout=10)
                reponse.raise_for_status()
                donnees = reponse.json()
            except requests.RequestException as erreur:
                print("appel echoue :", erreur)
                continue

            cours = {
                "paire": paire,
                "prix": float(donnees["price"]),
                "horodatage": donnees["time"],
            }

            # La cle est la paire : tous les prix du BTC iront dans la meme
            # partition, et resteront donc dans l'ordre.
            producteur.produce(
                TOPIC,
                key=paire,
                value=json.dumps(cours),
            )
            producteur.poll(0)

            print(f"envoye : {paire} = {cours['prix']}")

        time.sleep(2)

except KeyboardInterrupt:
    pass
finally:
    producteur.flush()
