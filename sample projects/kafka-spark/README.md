# Projet d'exemple — Kafka → Spark

**Le but** : calculer en continu, avec Spark, sur des messages qui arrivent dans Kafka.

Comme source de données, ce projet utilise les modifications faites en direct sur Wikipédia : Spark calcule quels wikis sont les plus modifiés, et quelle part de ces modifications est faite par des robots. La source se remplace facilement : voir [Changer de source de données](#changer-de-source-de-données).

```
Wikipédia  ──→  producteur.py  ──→  KAFKA  ──→  SPARK (comptage.py)  ──→  tableau
 (internet)       (envoie)                        (calcule)                mis à jour
                                                                         toutes les 10 s
```

Ce projet relie deux technologies vues séparément dans les kits :

- [kits/kafka](../../kits/kafka/) : faire circuler les données ;
- [kits/spark](../../kits/spark/) : calculer sur les données.

Faites ces deux kits d'abord : ce projet ne réexplique pas leurs bases.

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))
- Une connexion internet (le flux Wikipédia arrive en direct)
- ~2 Go de RAM disponibles pour Docker

> **Un seul projet ou kit à la fois.** Ce projet utilise le même nom de conteneur `kafka` que le kit Kafka. Faites `docker compose down` dans les autres dossiers avant de commencer.

Toutes les commandes tiennent **sur une seule ligne** et fonctionnent dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer Kafka

Ouvrez un terminal **dans le dossier de ce projet**, puis :

```bash
docker compose up -d
```

Un seul conteneur démarre : `kafka`. Spark, lui, sera lancé à l'étape 4.

---

## 2. Créer le topic

```bash
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wikipedia --partitions 3
```

Vous devez lire `Created topic wikipedia.` Si vous lisez `Connection refused`, attendez dix secondes et recommencez.

---

## 3. Lancer le producteur

```bash
docker compose run --rm producteur
```

C'est le même [producteur.py](producteur/producteur.py) que dans le kit Kafka : il envoie dans Kafka chaque modification de Wikipédia. **Laissez ce terminal ouvert.**

---

## 4. Lancer Spark

Ouvrez un **deuxième terminal** dans le dossier du projet :

```bash
docker compose run --rm spark
```

La première fois, Docker prépare l'image Spark : comptez quelques minutes.

Quelques lignes `WARN` s'affichent au démarrage : ce sont des avertissements sans importance. Puis, toutes les 10 secondes, un nouveau tableau :

```
-------------------------------------------
Batch: 3
-------------------------------------------
+------------+-------------+----------+
|wiki        |modifications|pct_robots|
+------------+-------------+----------+
|commonswiki |470          |57        |
|idwiki      |271          |99        |
|arwiktionary|181          |100       |
|wikidatawiki|143          |8         |
|urwiki      |69           |1         |
|hewiki      |59           |97        |
|enwiki      |58           |2         |
...
```

Chaque tableau (`Batch`) reprend le précédent et y ajoute les messages arrivés entre-temps.

Arrêtez Spark avec `Ctrl+C`.

---

## 5. Lire le job Spark

Ouvrez [spark/comptage.py](spark/comptage.py). Il tient en quatre étapes :

| Étape | Ce qu'elle fait |
|---|---|
| 1. Lire Kafka | `spark.readStream.format("kafka")` : le topic devient un DataFrame |
| 2. Décoder | le message est du JSON : `from_json` en fait des colonnes (`wiki`, `robot`…) |
| 3. Calculer | `groupBy("wiki")` puis `count` et `avg` |
| 4. Afficher | `writeStream` : afficher le résultat toutes les 10 secondes |

L'étape 3 est **exactement le code que vous écririez en batch** sur un fichier, comme dans le kit [kits/spark](../../kits/spark/). Seules la lecture et l'écriture changent :

| | Batch (kit Spark) | Streaming (ce projet) |
|---|---|---|
| Lire | `spark.read` | `spark.readStream` |
| Calculer | `groupBy(...)` | `groupBy(...)` — identique |
| Écrire | `show()`, `write` | `writeStream` |
| Le programme… | s'arrête quand le fichier est traité | tourne sans fin |

---

## Pourquoi relier Kafka et Spark ?

Chacun fait ce que l'autre ne sait pas faire :

| | Kafka | Spark |
|---|---|---|
| Recevoir des données en continu, à n'importe quel rythme | oui | — |
| Les garder en attendant qu'on les lise | oui | non |
| Calculer (regrouper, compter, moyenner…) | non | oui |
| Répartir ce calcul sur plusieurs machines | — | oui |

Kafka **transporte** les données, Spark **calcule**. Ici, Spark tourne sur une seule machine pour rester simple. Sur un vrai cluster, chaque partition du topic serait lue en parallèle par une machine différente.

> **Pourquoi Spark recompte-t-il tout à chaque lancement ?** Contrairement aux consommateurs Python du kit Kafka, ce job ne demande pas à Kafka de retenir sa position : il relit le topic **depuis le début** à chaque démarrage (`startingOffsets: earliest`). Relancez-le : le premier tableau contient déjà tous les messages reçus depuis la création du topic.

---

## Changer de source de données

Deux fichiers sont concernés :

1. [producteur/producteur.py](producteur/producteur.py) : c'est le seul programme qui connaît Wikipédia. Réécrivez-le pour lire votre source, en envoyant toujours des messages **JSON** dans Kafka.
2. [spark/comptage.py](spark/comptage.py) : adaptez `SCHEMA` aux champs de vos messages (étape 2), puis le calcul (étape 3).

Les étapes 1 et 4 du job Spark (lire Kafka, afficher) ne changent pas, à part le nom du topic.

---

## Arrêter

Arrêtez d'abord le producteur et Spark (`Ctrl+C` dans chaque terminal), puis :

```bash
docker compose down          # arrête, garde les messages
docker compose down -v       # arrête et efface les messages
```
