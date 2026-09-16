# Kit — Kafka (données en streaming)

Un serveur **Kafka** prêt à l'emploi, alimenté par un **vrai flux de données en direct** : toutes les modifications faites en ce moment sur Wikipédia et les autres wikis de Wikimedia, partout dans le monde — plusieurs dizaines par seconde.

Dans les autres kits, les données sont **au repos** : un fichier que l'on charge, puis que l'on interroge. Ici, elles sont **en mouvement** : elles arrivent sans arrêt, et des programmes les traitent au fur et à mesure.

**Kafka se place entre les deux** : d'un côté des programmes qui **envoient** des messages, de l'autre des programmes qui les **lisent**. Kafka garde les messages en attendant qu'on les lise.

```
                                          ┌──→  consommateur.py  (compte et affiche)
Wikipédia  ──→  producteur.py  ──→  KAFKA ┤
 (internet)       (envoie)                └──→  archiveur.py  ──→  MongoDB
                                                  (enregistre)      (base NoSQL)
```

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))
- Une connexion internet (le flux Wikipédia arrive en direct)
- ~2 Go de RAM disponibles pour Docker

> Si le kit `nosql-db/mongodb` tourne, arrêtez-le d'abord (`docker compose down` dans son dossier) : les deux kits utilisent les mêmes noms de conteneurs `mongo` et `mongo-express`.

### Note pour Windows

À l'installation, Docker Desktop demande d'activer **WSL 2** : acceptez.

Toutes les commandes de ce kit tiennent **sur une seule ligne** et fonctionnent telles quelles dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer Kafka

Ouvrez un terminal **dans le dossier de ce kit**, puis :

```bash
docker compose up -d
```

Quatre conteneurs démarrent :

| Conteneur | Rôle |
|---|---|
| `kafka` | le serveur Kafka : il reçoit les messages et les garde sur disque |
| `kafka-ui` | une interface web pour voir ce qui se passe dans Kafka |
| `mongo` | une base MongoDB, qui servira à la section 6 |
| `mongo-express` | une interface web pour explorer MongoDB |

---

## 2. Créer un topic

Le vocabulaire de base :

| Mot | Ce que c'est |
|---|---|
| **message** | une donnée envoyée dans Kafka — ici, une modification de Wikipédia |
| **topic** | une « boîte » qui regroupe des messages du même type, avec un nom |
| **producteur** | un programme qui **envoie** des messages dans un topic |
| **consommateur** | un programme qui **lit** les messages d'un topic |

Créez le topic `wikipedia` :

```bash
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wikipedia --partitions 3
```

Vous devez lire `Created topic wikipedia.`

> `--bootstrap-server localhost:9092` revient dans toutes les commandes : c'est l'adresse du serveur Kafka. Si vous lisez `Connection refused`, Kafka n'a pas fini de démarrer : attendez dix secondes et recommencez.

`--partitions 3` découpe le topic en **trois morceaux**, appelés **partitions**. On verra à la section 7 à quoi ça sert.

---

## 3. Envoyer le flux Wikipédia dans Kafka

Lancez le producteur :

```bash
docker compose run --rm producteur
```

La première fois, Docker prépare l'image Python : comptez une minute. Puis :

```
Connexion a Wikimedia... (Ctrl+C pour arreter)
152 modifications envoyees dans Kafka
395 modifications envoyees dans Kafka
618 modifications envoyees dans Kafka
...
```

**Laissez ce terminal ouvert** : le producteur tourne tant que vous ne l'arrêtez pas. Pour la suite, ouvrez un **nouveau terminal** dans le dossier du kit.

Ouvrez [programmes/producteur.py](programmes/producteur.py) et lisez-le. Il fait trois choses, en boucle :

1. il lit une modification sur le flux de Wikimedia ;
2. il n'en garde que quelques champs ;
3. il l'envoie dans Kafka avec `producteur.produce(...)`.

Chaque message envoyé ressemble à ceci :

```json
{"wiki": "frwiki", "titre": "Namur", "utilisateur": "Dupont", "robot": false, "type": "edit", "horodatage": 1789552327}
```

`frwiki` est le Wikipédia en français, `enwiki` en anglais, `wikidatawiki` est Wikidata, `commonswiki` la médiathèque Wikimedia Commons…

---

## 4. Lire les messages

Dans votre nouveau terminal, demandez à Kafka les 5 prochains messages qui arrivent :

```bash
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wikipedia --max-messages 5
```

```
{"wiki": "wikidatawiki", "titre": "Q16265162", "utilisateur": "MsMorale", "robot": false, "type": "edit", "horodatage": 1789552327}
{"wiki": "commonswiki", "titre": "Category:ISS photographs taken on 2026-07-14", "utilisateur": "OptimusPrimeBot", "robot": true, "type": "categorize", "horodatage": 1789552326}
...
Processed a total of 5 messages
```

> Kafka affiche peut-être d'abord une ligne `The consumer rebalance protocol (KIP-848) is production-ready!…` : c'est une simple information, ignorez-la.

Ajoutez maintenant `--from-beginning` pour lire **depuis le début** du topic :

```bash
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wikipedia --max-messages 5 --from-beginning
```

Relancez cette commande plusieurs fois : ce sont **toujours les mêmes 5 messages**.

C'est le point le plus important de Kafka : **lire un message ne le supprime pas**. Contrairement à une boîte mail, où un message lu puis archivé disparaît de la boîte de réception, Kafka garde tous les messages (ici pendant 24 heures). Plusieurs programmes peuvent donc lire les mêmes données, chacun à son rythme.

---

## 5. Un programme qui consomme

Lancez le consommateur :

```bash
docker compose run --rm consommateur
```

Toutes les 5 secondes, il affiche les wikis les plus modifiés depuis son lancement :

```
Partitions lues : [0, 1, 2]
1457 modifications lues, dont 47 % par des robots
  commonswiki         607
  wikidatawiki        256
  arwiktionary        163
  enwiki               70
  cewiki               41
```

Ouvrez [programmes/consommateur.py](programmes/consommateur.py). La partie Kafka tient en quelques lignes :

- `Consumer({...})` se connecte à Kafka ;
- `subscribe(["wikipedia"])` s'abonne au topic ;
- `poll(1.0)` attend le message suivant.

Tout le reste, c'est du Python ordinaire : compter, trier, afficher.

> **`Partitions lues : []` et 0 modification ?** Un consommateur précédent a été fermé sans `Ctrl+C` (terminal fermé, par exemple). Kafka le croit encore présent et lui réserve les partitions pendant environ 45 secondes. Attendez : elles arriveront toutes seules.

### Arrêter et reprendre

Arrêtez le consommateur (`Ctrl+C`), attendez une trentaine de secondes, puis relancez-le.

Le compteur repart de zéro — c'est une variable Python, elle est perdue à l'arrêt. Mais le programme **ne relit pas les anciens messages** : il reprend exactement là où il s'était arrêté, et récupère ceux qui sont arrivés pendant son absence.

C'est Kafka qui s'en souvient. Le consommateur appartient à un **groupe**, nommé `compteur` dans le code (`group.id`), et Kafka retient **jusqu'où ce groupe a lu**.

---

## 6. Enregistrer les messages dans une base NoSQL

`consommateur.py` compte en mémoire : dès qu'on l'arrête, tout est perdu. Dans un vrai projet, un consommateur **enregistre** les messages dans une base, où ils restent et où l'on peut les interroger.

C'est ce que fait [programmes/archiveur.py](programmes/archiveur.py) : il lit le topic `wikipedia` et écrit chaque modification dans **MongoDB**.

Laissez tourner le producteur. Dans un nouveau terminal, lancez l'archiveur :

```bash
docker compose run --rm archiveur
```

```
En attente de messages... (Ctrl+C pour arreter)
1828 modifications enregistrees dans MongoDB
2014 modifications enregistrees dans MongoDB
...
```

Ouvrez `archiveur.py`. Par rapport à `consommateur.py`, il n'y a que deux nouveautés :

```python
collection = MongoClient(MONGO)["wikipedia"]["modifications"]   # se connecter à MongoDB
...
collection.insert_one(modification)                              # enregistrer le message
```

Le message Kafka est déjà du JSON : il devient **tel quel** un document MongoDB. La base `wikipedia` et la collection `modifications` sont créées automatiquement au premier document.

> **Pourquoi l'archiveur a-t-il enregistré des milliers de messages dès le départ ?** Il appartient à un **autre groupe** que `consommateur.py` : `archiveur` au lieu de `compteur`. Kafka retient la position de chaque groupe séparément. Ce nouveau groupe n'avait encore rien lu, il a donc commencé au début du topic. Les deux programmes reçoivent **tous** les messages, chacun de leur côté, sans se gêner.

### Interroger la base

Ouvrez le shell de MongoDB, directement sur la base `wikipedia` :

```bash
docker exec -it mongo mongosh wikipedia
```

Puis tapez les requêtes suivantes, une par une.

Le nombre de modifications enregistrées — relancez-la, il augmente pendant que l'archiveur tourne :

```js
db.modifications.countDocuments()
```

Les 3 dernières modifications du Wikipédia en français :

```js
db.modifications.find({ wiki: "frwiki" }).sort({ horodatage: -1 }).limit(3)
```

```
[
  {
    _id: ObjectId('6aaa93fd899b48605e85160b'),
    wiki: 'frwiki',
    titre: 'Matthieu Pigasse',
    utilisateur: 'BoetBoet',
    robot: false,
    type: 'edit',
    horodatage: 1789563895
  },
  ...
]
```

Chaque document contient les champs du message Kafka, plus un `_id` ajouté automatiquement par MongoDB.

Les 5 wikis les plus modifiés — le calcul de `consommateur.py`, mais fait cette fois par la base, sur **toutes** les données enregistrées :

```js
db.modifications.aggregate([ { $group: { _id: "$wiki", total: { $sum: 1 } } }, { $sort: { total: -1 } }, { $limit: 5 } ])
```

```
[
  { _id: 'commonswiki', total: 2373 },
  { _id: 'wikidatawiki', total: 1108 },
  { _id: 'arwiktionary', total: 754 },
  { _id: 'enwiki', total: 583 },
  { _id: 'kowiki', total: 206 }
]
```

Quittez le shell avec `exit`.

Vous pouvez aussi parcourir les documents à la souris : ouvrez [http://localhost:8081](http://localhost:8081), puis la base **wikipedia** et la collection **modifications**.

Arrêtez l'archiveur (`Ctrl+C`) puis relancez-le : comme `consommateur.py`, il reprend là où il s'était arrêté. Mais cette fois, **rien n'est perdu** : les documents déjà enregistrés sont toujours dans MongoDB.

---

## 7. Plusieurs consommateurs

Quand un seul programme ne suffit plus à traiter tous les messages, on en lance plusieurs **dans le même groupe**, et Kafka répartit le travail entre eux.

Laissez tourner le producteur et le premier consommateur. Ouvrez un **troisième terminal** et lancez un deuxième consommateur :

```bash
docker compose run --rm consommateur
```

Regardez la ligne `Partitions lues` dans les deux terminaux. Par exemple :

```
Terminal 2 :  Partitions lues : [0, 1]
Terminal 3 :  Partitions lues : [2]
```

C'est à ça que servent les partitions : Kafka a donné **une partie du topic à chaque consommateur**. Un message n'est lu que par l'un des deux, jamais par les deux.

Arrêtez l'un des deux consommateurs : quelques secondes plus tard, l'autre récupère **toutes** les partitions.

### Pourquoi le deuxième consommateur reçoit-il si peu ?

Vous l'avez sans doute remarqué : l'un des deux consommateurs compte beaucoup plus de messages que l'autre.

Le producteur envoie chaque message avec une **clé** : le nom du wiki (`key=modification["wiki"]`). Kafka range **tous les messages d'une même clé dans la même partition**. Or `commonswiki` et `wikidatawiki` représentent à eux seuls plus de la moitié du flux, et ils sont tombés dans les partitions 0 et 1. La partition 2 ne reçoit que les « petits » wikis.

C'est une situation très courante avec de vraies données : le choix de la clé décide de la répartition du travail.

---

## 8. L'interface web

Ouvrez [http://localhost:8080](http://localhost:8080), puis cliquez sur le cluster **wikipedia** :

- **Topics → wikipedia → Messages** : les messages, en direct.
- **Topics → wikipedia → Overview** : le nombre de messages dans chaque partition. Vous y verrez le déséquilibre expliqué plus haut.
- **Consumers → compteur** : quel consommateur lit quelle partition, et son **retard** (colonne *Lag*) : le nombre de messages arrivés mais pas encore lus.

---

## Ce qu'il faut retenir

1. **Kafka transporte des données en mouvement**, entre des producteurs qui envoient et des consommateurs qui lisent. Les deux ne se connaissent pas.
2. **Kafka n'est pas une base de données.** Pour garder et interroger les données, un consommateur les enregistre dans une base (ici MongoDB).
3. **Lire un message ne le supprime pas.** Plusieurs programmes, dans des groupes différents, peuvent lire le même topic.
4. **Kafka retient où chaque groupe en est.** Un consommateur arrêté reprend là où il s'était arrêté.
5. **Les partitions permettent de partager le travail** entre plusieurs consommateurs d'un même groupe.

---

## Utiliser Kafka dans votre projet

Les programmes de ce kit sont un bon point de départ : gardez la partie Kafka, remplacez Wikipédia par votre propre source de données, et adaptez `archiveur.py` à ce que vous voulez enregistrer.

Pour lancer un programme **directement sur votre machine** plutôt que dans Docker :

```bash
pip install confluent-kafka requests pymongo
```

```bash
python programmes/archiveur.py
```

Le programme se connecte alors à `localhost:9092` pour Kafka et à `localhost:27017` pour MongoDB. Dans Docker, il utilisait `kafka:19092` et `mongo:27017` : un conteneur ne peut pas joindre les autres par `localhost`, qui désigne le conteneur lui-même.

---

## Arrêter

Arrêtez d'abord les programmes Python (`Ctrl+C` dans chaque terminal), puis :

```bash
docker compose down          # arrête, garde les messages
docker compose down -v       # arrête et efface les messages et la base MongoDB
```
