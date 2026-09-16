# Kit — Kafka (données en streaming)

Un serveur **Kafka** prêt à l'emploi, alimenté par un **vrai flux de données en direct** : toutes les modifications faites en ce moment sur Wikipédia et les autres wikis de Wikimedia, partout dans le monde — plusieurs dizaines par seconde.

Dans les autres kits, les données sont **au repos** : un fichier que l'on charge, puis que l'on interroge. Ici, elles sont **en mouvement** : elles arrivent sans arrêt, et des programmes les traitent au fur et à mesure.

**Kafka se place entre les deux** : d'un côté des programmes qui **envoient** des messages, de l'autre des programmes qui les **lisent**. Kafka garde les messages en attendant qu'on les lise.

```
Wikipédia  ──→  producteur.py  ──→  KAFKA  ──→  consommateur.py
 (internet)       (envoie)                        (lit et compte)
```

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))
- Une connexion internet (le flux Wikipédia arrive en direct)

### Note pour Windows

À l'installation, Docker Desktop demande d'activer **WSL 2** : acceptez.

Toutes les commandes de ce kit tiennent **sur une seule ligne** et fonctionnent telles quelles dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer Kafka

Ouvrez un terminal **dans le dossier de ce kit**, puis :

```bash
docker compose up -d
```

Deux conteneurs démarrent :

| Conteneur | Rôle |
|---|---|
| `kafka` | le serveur Kafka : il reçoit les messages et les garde sur disque |
| `kafka-ui` | une interface web pour voir ce qui se passe dans Kafka |

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

`--partitions 3` découpe le topic en **trois morceaux**, appelés **partitions**. On verra à la section 6 à quoi ça sert.

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

## 6. Plusieurs consommateurs

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

## 7. L'interface web

Ouvrez [http://localhost:8080](http://localhost:8080), puis cliquez sur le cluster **wikipedia** :

- **Topics → wikipedia → Messages** : les messages, en direct.
- **Topics → wikipedia → Overview** : le nombre de messages dans chaque partition. Vous y verrez le déséquilibre expliqué plus haut.
- **Consumers → compteur** : quel consommateur lit quelle partition, et son **retard** (colonne *Lag*) : le nombre de messages arrivés mais pas encore lus.

---

## Ce qu'il faut retenir

1. **Kafka transporte des données en mouvement**, entre des producteurs qui envoient et des consommateurs qui lisent. Les deux ne se connaissent pas.
2. **Lire un message ne le supprime pas.** Plusieurs programmes peuvent lire le même topic.
3. **Kafka retient où chaque groupe en est.** Un consommateur arrêté reprend là où il s'était arrêté.
4. **Les partitions permettent de partager le travail** entre plusieurs consommateurs d'un même groupe.

---

## Utiliser Kafka dans votre projet

Les deux programmes de ce kit sont un bon point de départ : gardez la partie Kafka, et remplacez Wikipédia par votre propre source de données.

Pour lancer un programme **directement sur votre machine** plutôt que dans Docker :

```bash
pip install confluent-kafka requests
```

```bash
python programmes/consommateur.py
```

Le programme se connecte alors à `localhost:9092`. Dans Docker, il utilisait `kafka:19092` : un conteneur ne peut pas joindre Kafka par `localhost`, qui désigne le conteneur lui-même.

---

## Arrêter

Arrêtez d'abord le producteur et les consommateurs (`Ctrl+C` dans chaque terminal), puis :

```bash
docker compose down          # arrête, garde les messages
docker compose down -v       # arrête et efface les messages
```
