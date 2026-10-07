# Image pour lancer les programmes Python du kit sans installer Python.

FROM python:3.13-slim

RUN pip install --no-cache-dir confluent-kafka==2.15.1 requests==2.34.2

WORKDIR /programmes

# Affiche les print() immediatement, sans les retenir en memoire
ENV PYTHONUNBUFFERED=1
