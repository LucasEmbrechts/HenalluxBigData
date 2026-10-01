# Projet d'exemple — Kafka → MongoDB

**Le but** : enregistrer dans une base de données des messages qui arrivent en continu dans Kafka, pour pouvoir ensuite les interroger.

Comme source de données, ce projet utilise les modifications faites en direct sur Wikipédia. Elle se remplace facilement : voir [Changer de source de données](#changer-de-source-de-données).

```
Wikipédia  ──→  producteur.py  ──→  KAFKA  ──→  archiveur.py  ──→  MONGODB
 (internet)       (envoie)                       (enregistre)
```

Ce projet relie deux technologies vues séparément dans les kits :

- [kits/kafka](../../kits/kafka/) : faire circuler les données ;
- [kits/nosql-db/mongodb](../../kits/nosql-db/mongodb/) : les stocker et les interroger.

Faites ces deux kits d'abord : ce projet ne réexplique pas leurs bases.

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))
- Une connexion internet (le flux Wikipédia arrive en direct)

> **Un seul projet ou kit à la fois.** Ce projet utilise les mêmes noms de conteneurs (`kafka`, `mongo`…) que les kits. Faites `docker compose down` dans les autres dossiers avant de commencer.

Toutes les commandes tiennent **sur une seule ligne** et fonctionnent dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer

Ouvrez un terminal **dans le dossier de ce projet**, puis :

```bash
docker compose up -d
```

| Conteneur | Rôle |
|---|---|
| `kafka` | le serveur Kafka |
| `mongo` | la base MongoDB |
| `mongo-express` | l'interface web de MongoDB |

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

C'est le même [producteur.py](programmes/producteur.py) que dans le kit Kafka : il envoie dans Kafka chaque modification de Wikipédia. **Laissez ce terminal ouvert.**

---

## 4. Lancer l'archiveur

Ouvrez un **deuxième terminal** dans le dossier du projet :

```bash
docker compose run --rm archiveur
```

```
En attente de messages... (Ctrl+C pour arreter)
161 modifications enregistrees dans MongoDB
332 modifications enregistrees dans MongoDB
...
```

Ouvrez [programmes/archiveur.py](programmes/archiveur.py). C'est un consommateur Kafka ordinaire, avec **deux lignes** de plus :

```python
collection = MongoClient(MONGO)["wikipedia"]["modifications"]   # se connecter à MongoDB
...
collection.insert_one(modification)                              # enregistrer le message
```

Le message Kafka est déjà du JSON : il devient **tel quel** un document MongoDB. La base `wikipedia` et la collection `modifications` sont créées automatiquement au premier document.

---

## 5. Interroger MongoDB

Ouvrez un **troisième terminal**, et le shell de MongoDB :

```bash
docker exec -it mongo mongosh wikipedia
```

Tapez les requêtes une par une.

Le nombre de documents — relancez-la : il augmente pendant que l'archiveur tourne.

```js
db.modifications.countDocuments()
```

Une modification du Wikipédia en français :

```js
db.modifications.find({ wiki: "frwiki" }).limit(1)
```

```
[
  {
    _id: ObjectId('6aab7d659b793f96d7164075'),
    wiki: 'frwiki',
    titre: 'Jesse Bradford',
    utilisateur: 'DSisyphBot',
    robot: true
  }
]
```

Les 5 wikis les plus modifiés :

```js
db.modifications.aggregate([ { $group: { _id: "$wiki", total: { $sum: 1 } } }, { $sort: { total: -1 } }, { $limit: 5 } ])
```

```
[
  { _id: 'commonswiki', total: 263 },
  { _id: 'idwiki', total: 182 },
  { _id: 'wikidatawiki', total: 141 },
  { _id: 'arwiktionary', total: 107 },
  { _id: 'enwiki', total: 58 }
]
```

Quittez avec `exit`. Vous pouvez aussi parcourir les documents à la souris sur [http://localhost:8081](http://localhost:8081) : base **wikipedia**, collection **modifications**.

---

## Pourquoi relier Kafka et MongoDB ?

Chacun fait ce que l'autre ne sait pas faire :

| | Kafka | MongoDB |
|---|---|---|
| Recevoir des données en continu, à n'importe quel rythme | oui | — |
| Garder les données longtemps | non (ici 24 h) | oui |
| Les interroger (filtrer, trier, compter) | non | oui |

L'archiveur fait le pont entre les deux. Et comme c'est un consommateur Kafka, il en a les avantages : **arrêtez-le** (`Ctrl+C`) une minute puis **relancez-le**. Il reprend là où il s'était arrêté et enregistre les messages arrivés pendant son absence : rien n'est perdu.

---

## Changer de source de données

Seul [programmes/producteur.py](programmes/producteur.py) connaît Wikipédia. Pour utiliser une autre source (une autre API, un capteur, un fichier rejoué…), c'est le seul programme à réécrire. Il doit toujours envoyer des messages **JSON** dans Kafka.

`archiveur.py` n'a pas besoin de changer : il enregistre n'importe quel JSON tel quel dans MongoDB. Pensez seulement à renommer le topic, la base et la collection (`wikipedia`, `modifications`) pour qu'ils correspondent à vos données.

---

## Arrêter

Arrêtez d'abord le producteur et l'archiveur (`Ctrl+C` dans chaque terminal), puis :

```bash
docker compose down          # arrête, garde les données
docker compose down -v       # arrête et efface les données
```
