# Image pour lancer producteur.py sans installer Python.
# Le programme n'est pas copie dans l'image : le dossier producteur/ est monte
# dans le conteneur (voir docker-compose.yml).

FROM python:3.13-slim

RUN pip install --no-cache-dir confluent-kafka==2.15.1 requests==2.34.2

WORKDIR /programmes

# Affiche les print() immediatement, sans les retenir en memoire
ENV PYTHONUNBUFFERED=1
