# Kit — MongoDB (base NoSQL orientée documents)

Un serveur **MongoDB** prêt à l'emploi, avec une interface web pour explorer les données.

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))

### Note pour Windows

À l'installation, Docker Desktop demande d'activer **WSL 2** : acceptez.

Toutes les commandes de ce kit tiennent **sur une seule ligne** et fonctionnent
telles quelles dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer MongoDB

Ouvrez un terminal **dans le dossier de ce kit**, puis :

```bash
docker compose up -d
```

Deux conteneurs :

| Conteneur | Rôle |
|---|---|
| `mongo` | le serveur : il stocke les documents sur disque |
| `mongo-express` | une interface web pour explorer la base |

Vérifiez que le serveur répond :

```bash
docker exec mongo mongosh --quiet --eval "db.runCommand({ping:1})"
```

Il doit répondre `{ ok: 1 }`.

---

## 2. Charger les données

```bash
docker exec mongo mongoimport --db bigdata --collection releves --type csv --headerline --file /kit/trajets.csv
```

Vous devez lire `5000 document(s) imported successfully`.

Le fichier de départ est [data/trajets.csv](data/trajets.csv) : 5 000 relevés au
format `camion_id,vitesse,temp_moteur`, les mêmes que dans les kits Hadoop et
Redis.

Décortiquons la commande :

| Option | Ce qu'elle dit |
|---|---|
| `--db bigdata` | le nom de la base |
| `--collection releves` | le nom de la **collection** (l'équivalent d'une table) |
| `--type csv` | le format du fichier d'entrée |
| `--headerline` | la première ligne contient les noms des champs |
| `--file /kit/trajets.csv` | le fichier, vu **depuis l'intérieur** du conteneur |

---

## 3. Comment les données sont rangées

Le vocabulaire de MongoDB, comparé à celui du SQL que vous connaissez :

| SQL | MongoDB |
|---|---|
| base de données | base de données |
| table | **collection** |
| ligne | **document** |
| colonne | **champ** |

Chaque ligne du CSV est devenue un document, écrit dans un format proche du
JSON :

```json
{
  "_id": ObjectId("6a717cd0e81d27e9f203cab6"),
  "camion_id": "CAM008",
  "vitesse": 81.8,
  "temp_moteur": 102.5
}
```

Trois choses à remarquer :

- **`_id` est apparu tout seul.** MongoDB ajoute à chaque document un
  identifiant unique s'il n'en trouve pas. C'est la clé primaire, et elle est
  obligatoire.
- **Les types ont été devinés.** `camion_id` est du texte, `vitesse` et
  `temp_moteur` sont des nombres — alors que le CSV ne contenait que du texte.
  C'est `--headerline` combiné à l'analyse des valeurs qui a fait ce travail.
- **Il n'y a pas de schéma.** Rien n'oblige deux documents d'une même
  collection à avoir les mêmes champs. On pourrait ajouter un relevé avec un
  champ `pression` sans toucher aux 5 000 autres. Aucune base SQL ne le permet.

---

## 4. Lire le contenu

Ouvrez [http://localhost:8081](http://localhost:8081).

Vous arrivez directement sur la liste des bases. Cliquez sur **`bigdata`**, puis
sur la collection **`releves`**.

Vous pouvez alors :

- parcourir les 5 000 documents page par page
- déplier chacun pour voir ses champs
- filtrer avec la barre de recherche, par exemple `{ "camion_id": "CAM021" }`
  ou `{ "vitesse": { "$gt": 90 } }`

C'est l'équivalent de l'interface HDFS du kit Hadoop : voir les données rend le
modèle beaucoup plus concret qu'une ligne de commande.

---

## Arrêter

```bash
docker compose down          # arrête, garde les données
docker compose down -v       # arrête et efface les données
```
