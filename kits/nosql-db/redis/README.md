# Kit — Redis (base NoSQL clé/valeur)

Un serveur **Redis** prêt à l'emploi, avec une interface web pour explorer les données.

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))

### Note pour Windows

À l'installation, Docker Desktop demande d'activer **WSL 2** : acceptez.

Toutes les commandes de ce kit tiennent **sur une seule ligne** et fonctionnent
telles quelles dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer Redis

Ouvrez un terminal **dans le dossier de ce kit**, puis :

```bash
docker compose up -d
```

Deux conteneurs :

| Conteneur | Rôle |
|---|---|
| `redis` | le serveur : il garde les données **en mémoire vive** |
| `redisinsight` | une interface web officielle pour explorer la base |

Vérifiez que le serveur répond :

```bash
docker exec redis redis-cli PING
```

Il doit répondre `PONG`.

---

## 2. Charger les données

```bash
docker exec redis sh /kit/charger.sh
```

Vous devez lire `errors: 0, replies: 5000`, puis `Termine.` et le nombre de
clés : **5000**.

Le fichier de départ est [data/trajets.csv](data/trajets.csv) : 5 000 relevés au
format `camion_id,vitesse,temp_moteur`, les mêmes que dans les kits Hadoop,
MongoDB et Cassandra.

Ouvrez maintenant [data/charger.sh](data/charger.sh) et lisez-le, il fait dix
lignes. Le point important est là :

> **On n'importe pas un fichier dans Redis.** Il n'y a ni table, ni colonne, ni
> schéma, donc rien qui puisse « recevoir » un CSV. Le script transforme chaque
> ligne en une **commande Redis**, et les envoie toutes d'un bloc. C'est à vous
> de décider comment vos données sont représentées.

---

## 3. Comment les données sont rangées

Chaque ligne du CSV est devenue un **hash** — un objet à champs nommés — sous
une clé numérotée :

```
releve:1     →   camion_id = CAM021 , vitesse = 95.6 , temp_moteur = 97.5
releve:2     →   camion_id = CAM008 , vitesse = 81.8 , temp_moteur = 102.5
...
releve:5000
```

Deux choses à remarquer :

- Le `:` dans `releve:1` n'a **aucune signification** pour Redis : c'est un
  caractère ordinaire dans le nom de la clé. Mais comme Redis n'a pas de
  tables, cette convention `type:identifiant` est la seule façon d'organiser
  les clés, et tout le monde l'utilise.
- Redis n'est pas un simple dictionnaire `clé → texte`. Ici la valeur est un
  hash ; il existe aussi des listes, des ensembles, des compteurs triés.

---

## 4. Lire le contenu


Ouvrez [http://localhost:5540](http://localhost:5540).

À la première ouverture, RedisInsight demande d'ajouter une base :

- **Host** : `redis`  *(le nom du conteneur, pas `localhost ou 127.0.0.1`)*
- **Port** : `6379`
- puis *Add Redis Database*

Vous pouvez alors parcourir les clés à la souris et voir le contenu de chaque
hash. C'est l'équivalent de l'interface HDFS du kit Hadoop : voir les données
rend le modèle beaucoup plus concret qu'une ligne de commande.

## Arrêter

```bash
docker compose down          # arrête, garde les données
docker compose down -v       # arrête et efface les données
```
