# Image pour lancer producteur.py et consommateur.py sans installer Python.
# Les programmes eux-memes ne sont pas copies dans l'image : le dossier
# programmes/ est monte dans le conteneur (voir docker-compose.yml), donc une
# modification des .py est prise en compte sans reconstruire l'image.

FROM python:3.13-slim

RUN pip install --no-cache-dir confluent-kafka==2.15.1 requests==2.34.2

WORKDIR /programmes

# Affiche les print() immediatement, sans les retenir en memoire
ENV PYTHONUNBUFFERED=1
