# Kit — Spark sur HDFS (Java)

Un cluster **HDFS + Spark** prêt à l'emploi.

**Le but** : compter les excès de vitesse par camion

## Prérequis

- Docker + Docker Compose
- ~4 Go de RAM disponibles

---

## 1. Démarrer le cluster

```bash
docker compose up -d
```

Quatre conteneurs : `namenode` et `datanode` (le stockage), `spark-master` et
`spark-worker` (le calcul).

| Interface | URL | À quoi ça sert |
|---|---|---|
| HDFS | http://localhost:9870 | voir les fichiers et les blocs |
| Spark Master | http://localhost:8080 | voir le worker et les applications |
| Application Spark | http://localhost:4040 | suivre les *jobs* et *stages* en cours |

> L'onglet 4040 n'existe que **pendant** l'exécution d'une application.

---

## 2. Charger les données dans HDFS

```bash
docker exec -it namenode bash

hdfs dfs -mkdir -p /projet/trajets
hdfs dfs -put /data/trajets.csv /projet/trajets/
hdfs dfs -ls /projet/trajets
exit
```

Mêmes 5 000 relevés que le kit MapReduce : `camion_id,vitesse,temp_moteur`.

---

## 3. Compiler le job

```bash
docker run --rm -v "$(pwd)/job":/app -w /app maven:3.9-eclipse-temurin-11 mvn -q clean package
```

Résultat : `job/target/exces-spark.jar`

> Windows PowerShell : remplacez `$(pwd)` par `${PWD}`.
> Lancez la commande **depuis le dossier du kit**, pas depuis `job/`.

---

## 4. Envoyer le `.jar` dans le conteneur

```bash
docker cp job/target/exces-spark.jar spark-master:/tmp/
```

> On copie plutôt qu'on ne monte un volume : c'est identique sur macOS,
> Windows et Linux, et ça évite des erreurs de chemin.

---

## 5. Lancer le job

**Version RDD** (l'équivalent direct de MapReduce) :

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class be.henallux.bigdata.ExcesSparkRDD \
  /tmp/exces-spark.jar \
  hdfs://namenode:8020/projet/trajets hdfs://namenode:8020/projet/resultat-rdd
```

**Version DataFrame / SQL** (l'API moderne) :

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class be.henallux.bigdata.ExcesSparkSQL \
  /tmp/exces-spark.jar \
  hdfs://namenode:8020/projet/trajets hdfs://namenode:8020/projet/resultat-sql
```

Celle-ci affiche directement les tableaux dans le terminal.

---

## 6. Lire le résultat

```bash
docker exec -it namenode bash
hdfs dfs -ls /projet/resultat-rdd
hdfs dfs -cat /projet/resultat-rdd/part-00000
exit
```

> Comme en MapReduce, Spark refuse d'écrire dans un répertoire existant
> (sauf en mode `overwrite`). Pour relancer :
> `hdfs dfs -rm -r /projet/resultat-rdd`

---

## Ce qu'il faut comparer

Ouvrez côte à côte le code de ce kit et celui de `hadoop-mapreduce` :

| | MapReduce | Spark RDD | Spark DataFrame |
|---|---|---|---|
| Classes à écrire | **3** | 1 | 1 |
| Lignes de logique | ~60 | ~15 | ~5 |
| Shuffle | implicite | `reduceByKey` | `groupBy` |
| Calculer une **moyenne** | difficile | moyen | trivial (`avg`) |

Trois observations à faire vous-même :

1. **Où est le `Reducer` ?** Dans Spark, `reduceByKey` fait le même travail
   qu'une classe entière en MapReduce.
2. **Quand le calcul démarre-t-il vraiment ?** Repérez la ligne qui déclenche
   tout (l'*action*) — avant elle, Spark n'a fait que noter les étapes.
3. **Regardez l'interface 4040** pendant l'exécution : Spark y affiche le DAG,
   c'est-à-dire le plan qu'il a construit à partir de vos transformations.

---

## Exercices

**Exercice 1 — la moyenne.** En MapReduce, calculer la vitesse moyenne par
camion demandait de repenser le mapper (et de renoncer au combiner).
Combien de lignes cela prend-il ici ? *(la réponse est déjà dans `ExcesSparkSQL`)*

**Exercice 2 — température.** Sortez, pour chaque camion, sa température moteur
**maximale**, et ne gardez que les camions dépassant 100 °C.

**Exercice 3 — les deux syntaxes.** Réécrivez l'exercice 2 dans les deux formes :
méthodes chaînées *puis* `spark.sql(...)`. Vérifiez que le résultat est identique.

**Exercice 4 — la paresse.** Commentez toutes les actions (`show`, `write`) et
relancez. Combien de temps met le job ? Pourquoi ?

**Exercice 5 — sans HDFS.** Remplacez le chemin `hdfs://...` par un fichier
local. Que démontre le fait que ça fonctionne encore ?

---

## Arrêter le cluster

```bash
docker compose down          # arrête
docker compose down -v       # arrête et efface les données HDFS
```
