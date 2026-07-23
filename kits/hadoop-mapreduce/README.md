# Kit — MapReduce avec Hadoop (Java)

Un cluster Hadoop prêt à l'emploi pour écrire et exécuter un **job MapReduce**
sur HDFS et YARN.

## Prérequis

- Docker + Docker Compose (installation via Docker Desktop)

---

## 1. Démarrer le cluster

```bash
docker compose down && docker compose up -d
```

Quatre conteneurs démarrent : `namenode`, `datanode`, `resourcemanager`, `nodemanager`.
Comptez une à deux minutes au premier lancement (téléchargement de l'image).

Deux interfaces web à ouvrir tout de suite :

| Interface | URL | À quoi ça sert |
|---|---|---|
| HDFS (NameNode) | http://localhost:9870 | voir les fichiers, les blocs, les DataNodes |
| YARN (ResourceManager) | http://localhost:8088 | suivre les jobs en cours |

> Gardez l'onglet YARN ouvert : vous y verrez votre job apparaître, passer en
> RUNNING, puis en SUCCEEDED.

---

## 2. Charger les données dans HDFS

Les données sont pour l'instant sur votre disque. Il faut les **mettre dans HDFS**.

```bash
docker exec -it namenode bash

hdfs dfs -mkdir -p /projet/trajets
hdfs dfs -put /data/trajets.csv /projet/trajets/
hdfs dfs -ls /projet/trajets
hdfs dfs -head /projet/trajets/trajets.csv
exit
```

Allez vérifier dans l'interface HDFS (onglet *Utilities → Browse the file system*) :
votre fichier est là, découpé en blocs.

Le fichier contient 5 000 relevés au format `camion_id,vitesse,temp_moteur`.

---

## 3. Compiler le job

Le code Java est dans `job/`. On le compile avec Maven, **dans un conteneur** :

```bash
docker run --rm -v "$(pwd)/job":/app -w /app maven:3.9-eclipse-temurin-11 mvn -q clean package
```

> Windows PowerShell : remplacez `$(pwd)` par `${PWD}`.

Résultat : `job/target/exces-vitesse.jar`

---

## 4. Lancer le job MapReduce

```bash
docker exec -it resourcemanager bash

hadoop jar /job/exces-vitesse.jar \
       be.henallux.bigdata.ExcesDriver \
       /projet/trajets /projet/resultat
```

Regardez défiler les lignes `map 0% reduce 0%` → `map 100% reduce 100%`,
et suivez le job dans l'interface YARN.

---

## 5. Lire le résultat

```bash
hdfs dfs -ls /projet/resultat
hdfs dfs -cat /projet/resultat/part-r-00000
exit
```

Vous obtenez une ligne par camion : `CAM012   37`

> **Note** : `_SUCCESS` est un fichier témoin (le job s'est bien terminé) ;
> `part-r-00000` est la sortie du reducer n°0.
>
> **Attention** : Hadoop refuse d'écrire dans un répertoire de sortie existant.
> Pour relancer, changez de répertoire ou supprimez le précédent :
> `hdfs dfs -rm -r /projet/resultat`

---

## Arrêter le cluster

```bash
docker compose down          # arrête
docker compose down -v       # arrête et efface les données HDFS
```
