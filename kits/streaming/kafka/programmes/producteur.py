"""Producteur : recopie en direct les modifications de Wikipedia dans Kafka.

La source : https://stream.wikimedia.org/v2/stream/recentchange
Chaque fois que quelqu'un, n'importe ou dans le monde, modifie une page d'un
wiki de Wikimedia (Wikipedia, Wikidata, Wikimedia Commons...), un evenement
arrive sur ce flux. Il y en a plusieurs dizaines par seconde.

Le programme ne fait que trois choses, en boucle :
  1. lire un evenement sur le flux de Wikimedia
  2. n'en garder que quelques champs
  3. l'envoyer dans le topic "wikipedia" de Kafka
"""

import json
import os
import time

import requests
from confluent_kafka import Producer

SOURCE = "https://stream.wikimedia.org/v2/stream/recentchange"
TOPIC = "wikipedia"

# Dans Docker, docker-compose.yml fournit "kafka:19092".
# Lance directement sur votre machine, le programme utilise "localhost:9092".
KAFKA = os.environ.get("KAFKA", "localhost:9092")

# Wikimedia demande a chaque programme de se presenter.
ENTETES = {"User-Agent": "HenalluxBigData-kit-kafka/1.0 (https://github.com/LucasEmbrechts/HenalluxBigData)"}


producteur = Producer({"bootstrap.servers": KAFKA})

if TOPIC not in producteur.list_topics(timeout=10).topics:
    print(f"Le topic '{TOPIC}' n'existe pas. Creez-le d'abord (section 2 du README).")
    raise SystemExit(1)

print("Connexion a Wikimedia... (Ctrl+C pour arreter)")
envoyes = 0
dernier_affichage = time.time()

try:
    while True:
        try:
            with requests.get(SOURCE, headers=ENTETES, stream=True, timeout=30) as reponse:
                reponse.raise_for_status()

                for ligne in reponse.iter_lines():
                    # Le flux envoie des lignes de texte. Seules celles qui
                    # commencent par "data: " contiennent un evenement (en JSON).
                    ligne = ligne.decode("utf-8")
                    if not ligne.startswith("data: "):
                        continue
                    evenement = json.loads(ligne[len("data: "):])

                    # Wikimedia envoie parfois de faux evenements de test.
                    if evenement["meta"]["domain"] == "canary":
                        continue

                    modification = {
                        "wiki": evenement.get("wiki"),     # ex. frwiki = Wikipedia en francais
                        "titre": evenement.get("title"),
                        "utilisateur": evenement.get("user"),
                        "robot": evenement.get("bot"),     # True si c'est un programme qui modifie
                        "type": evenement.get("type"),     # edit, new, log, categorize
                        "horodatage": evenement.get("timestamp"),
                    }

                    # L'envoi dans Kafka : un topic, une cle, une valeur.
                    producteur.produce(
                        TOPIC,
                        key=modification["wiki"],
                        value=json.dumps(modification, ensure_ascii=False),
                    )
                    producteur.poll(0)   # laisse Kafka traiter les envois en cours
                    envoyes += 1

                    if time.time() - dernier_affichage >= 5:
                        print(f"{envoyes} modifications envoyees dans Kafka")
                        dernier_affichage = time.time()

        except requests.RequestException as erreur:
            # Le flux de Wikimedia coupe de temps en temps : on se reconnecte.
            print(f"Connexion a Wikimedia perdue ({erreur}), nouvel essai dans 5 s...")
            time.sleep(5)

except KeyboardInterrupt:
    pass
finally:
    # produce() ne fait que mettre le message en file d'attente : flush()
    # attend qu'ils soient tous vraiment partis vers Kafka.
    producteur.flush()
    print(f"\nArret. {envoyes} modifications envoyees au total.")
