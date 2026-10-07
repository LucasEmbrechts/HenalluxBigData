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

Créez le topic `wikipedia` :

```bash
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --topic wikipedia --partitions 3
```

Vous devez lire `Created topic wikipedia.`

> `--bootstrap-server localhost:9092` revient dans toutes les commandes : c'est l'adresse du serveur Kafka. Si vous lisez `Connection refused`, Kafka n'a pas fini de démarrer : attendez dix secondes et recommencez.

---

## 3. Envoyer le flux Wikipédia dans Kafka

Lancez le producteur :

```bash
docker compose run --rm producteur
```

La première fois, Docker prépare l'image Python : comptez une minute. Puis :

```
envoye : enwiki - Category:Wikipedia semi-protected edit requests
envoye : commonswiki - File:Apamea monoglypha-o.jpg
envoye : wikidatawiki - Q102423043
...
```

Chaque ligne est une modification de Wikipédia, envoyée dans Kafka au moment où elle a lieu. Ça défile vite : plusieurs dizaines par seconde.

**Laissez ce terminal ouvert** : le producteur tourne tant que vous ne l'arrêtez pas. Pour la suite, ouvrez un **nouveau terminal** dans le dossier du kit.

Ouvrez [programmes/producteur.py](programmes/producteur.py). La partie Kafka tient en trois lignes :

| Code | Ce qu'il fait |
|---|---|
| `Producer({"bootstrap.servers": KAFKA})` | se connecter à Kafka |
| `producteur.produce("wikipedia", key=..., value=...)` | envoyer un message dans le topic `wikipedia` |
| `producteur.flush()` | à l'arrêt, attendre que tous les messages soient partis |

Tout le reste sert à lire le flux de Wikipédia. Chaque message envoyé ressemble à ceci :

```json
{"wiki": "frwiki", "titre": "Namur", "utilisateur": "Dupont", "robot": false}
```

`frwiki` est le Wikipédia en français, `enwiki` en anglais, `wikidatawiki` est Wikidata, `commonswiki` la médiathèque Wikimedia Commons…

---

## 4. Lire les messages

Dans votre nouveau terminal, demandez à Kafka les 5 prochains messages qui arrivent :

```bash
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wikipedia --max-messages 5
```

```
{"wiki": "specieswiki", "titre": "Gracilaria", "utilisateur": "Thiotrix", "robot": false}
{"wiki": "wikidatawiki", "titre": "Q138006868", "utilisateur": "Roger.ec1", "robot": false}
...
Processed a total of 5 messages
```

> Kafka affiche peut-être d'abord une ligne `The consumer rebalance protocol (KIP-848) is production-ready!…` : c'est une simple information, ignorez-la.

Ajoutez maintenant `--from-beginning` pour lire **depuis le début** du topic :

```bash
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wikipedia --max-messages 5 --from-beginning
```

Relancez cette commande plusieurs fois : ce sont **toujours les mêmes 5 messages**.

C'est le point le plus important de Kafka : **lire un message ne le supprime pas**. Contrairement à une boîte mail, où un message lu puis archivé disparaît de la boîte de réception, Kafka garde les messages un certain temps : 7 jours par défaut, 24 heures dans ce kit. En pratique, il les garde même plus longtemps, car il supprime par blocs entiers, jamais message par message. Plusieurs programmes peuvent donc lire les mêmes données, chacun à son rythme.

---

## 5. Un programme qui consomme

Lancez le consommateur :

```bash
docker compose run --rm consommateur
```

Il affiche chaque message reçu :

```
partition 1 | offset 669 | enwiki | List of supermarket chains in Asia
partition 2 | offset 246 | zhwikisource | Category:中华人民共和国民事判决书
partition 0 | offset 679 | zhwiki | 比桑贝格
...
```

La première fois, il commence par **tous** les messages déjà présents dans le topic (d'où le défilement rapide au début), puis il affiche les nouveaux au fur et à mesure.

Chaque ligne indique où le message est rangé dans Kafka :

| | Ce que c'est |
|---|---|
| **partition** | le morceau du topic où se trouve le message (0, 1 ou 2) |
| **offset** | le numéro du message dans sa partition : 0, 1, 2… |

Ouvrez [programmes/consommateur.py](programmes/consommateur.py). La partie Kafka tient en quatre lignes :

| Code | Ce qu'il fait |
|---|---|
| `Consumer({...})` | se connecter à Kafka, dans le groupe `lecteurs` |
| `consommateur.subscribe(["wikipedia"])` | s'abonner au topic `wikipedia` |
| `consommateur.poll(1.0)` | attendre le message suivant (au plus 1 seconde) |
| `consommateur.close()` | à l'arrêt, quitter le groupe proprement |

> **Le consommateur n'affiche rien ?** Un consommateur précédent a sans doute été fermé sans `Ctrl+C` (terminal fermé, par exemple). Kafka le croit encore présent et lui réserve les partitions pendant environ 45 secondes. Attendez : les messages arriveront tout seuls.

### Arrêter et reprendre

Arrêtez le consommateur (`Ctrl+C`), attendez une trentaine de secondes, puis relancez-le.

Regardez les offsets : ils **ne repartent pas de 0**. Le programme reprend là où il s'était arrêté : il ne relit pas les anciens messages, et récupère ceux qui sont arrivés pendant son absence.

C'est Kafka qui s'en souvient. Le consommateur appartient à un **groupe**, nommé `lecteurs` dans le code (`group.id`), et Kafka retient **jusqu'où ce groupe a lu**.

### Tout relire depuis le début

Les messages sont toujours là : reprendre au bon endroit est un choix, pas une obligation. Pour tout relire, deux façons.

**La plus simple : changer de groupe.** Dans [programmes/consommateur.py](programmes/consommateur.py), remplacez le nom du groupe :

```python
"group.id": "essai",     # au lieu de "lecteurs"
```

Relancez le consommateur : les offsets repartent de **0**. Ce groupe n'a jamais rien lu, et `auto.offset.reset: earliest` lui dit de commencer au début du topic. Remettez ensuite `lecteurs` pour la suite.

**L'autre : ramener le groupe en arrière.** Arrêtez d'abord tous les consommateurs, puis :

```bash
docker exec kafka kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group lecteurs --topic wikipedia --reset-offsets --to-earliest --execute
```

Kafka affiche la nouvelle position de chaque partition, remise à 0. Au prochain lancement, le groupe `lecteurs` relit tout.

> Le groupe doit être **à l'arrêt** : tant qu'un consommateur y est connecté, Kafka refuse de déplacer sa position.

C'est une des grandes différences avec une file d'attente classique : on corrige un bug dans son programme, et on **rejoue l'historique** sans rien redemander à la source.

---

## 6. Plusieurs consommateurs

Quand un seul programme ne suffit plus à traiter tous les messages, on en lance plusieurs **dans le même groupe**, et Kafka répartit le travail entre eux.

Laissez tourner le producteur et le premier consommateur. Ouvrez un **troisième terminal** et lancez un deuxième consommateur :

```bash
docker compose run --rm consommateur
```

Après quelques secondes, regardez la colonne `partition` dans les deux terminaux. Par exemple :

```
Terminal 2 :  seulement des lignes « partition 2 »
Terminal 3 :  seulement des lignes « partition 0 » et « partition 1 »
```

C'est à ça que servent les partitions : Kafka a donné **une partie du topic à chaque consommateur**. Un message n'est lu que par l'un des deux, jamais par les deux.

Arrêtez l'un des deux consommateurs : quelques secondes plus tard, l'autre reçoit à nouveau **les trois** partitions.

### Pourquoi l'un des deux reçoit-il si peu ?

Vous l'avez sans doute remarqué : l'un des deux terminaux défile beaucoup plus vite que l'autre.

Le producteur envoie chaque message avec une **clé** : le nom du wiki (`key=modification["wiki"]`). Kafka range **tous les messages d'une même clé dans la même partition**. Or `commonswiki` et `wikidatawiki` représentent à eux seuls plus de la moitié du flux, et ils sont tombés dans les partitions 0 et 1. La partition 2 ne reçoit que les « petits » wikis.

C'est une situation très courante avec de vraies données : le choix de la clé décide de la répartition du travail.

---

## 7. L'interface web

Ouvrez [http://localhost:8080](http://localhost:8080), puis cliquez sur le cluster **wikipedia** :

- **Topics → wikipedia → Messages** : les messages, en direct.
- **Topics → wikipedia → Overview** : le nombre de messages dans chaque partition. Vous y verrez le déséquilibre expliqué plus haut.
- **Consumers → lecteurs** : quel consommateur lit quelle partition, et son **retard** (colonne *Lag*) : le nombre de messages arrivés mais pas encore lus.

---

## Arrêter

Arrêtez d'abord les programmes Python (`Ctrl+C` dans chaque terminal), puis :

```bash
docker compose down          # arrête, garde les messages
docker compose down -v       # arrête et efface les messages
```
