# Kit — Spark sur YARN (Java)

Un cluster **HDFS + YARN** prêt à l'emploi, sur lequel on soumet des jobs Spark.

**Le but** : compter les excès de vitesse par camion

> Ce kit reprend exactement le cluster du kit `../mapreduce` — mêmes
> NameNode, DataNode, ResourceManager et NodeManager. Seul le *moteur de calcul*
> change : Spark au lieu de MapReduce. C'est tout l'intérêt de YARN.

## Prérequis

- Docker + Docker Compose
- ~4 Go de RAM disponibles

> **Un seul kit à la fois.** Les deux kits utilisent les mêmes noms de
> conteneurs (`namenode`, `datanode`…). Faites `docker compose down` dans
> l'autre kit avant de démarrer celui-ci.

---

## 1. Démarrer le cluster

```bash
docker compose up -d
```

Cinq conteneurs démarrent :

| Conteneur | Rôle |
|---|---|
| `namenode` | l'annuaire de HDFS : qui stocke quel bloc |
| `datanode` | le stockage réel des blocs |
| `resourcemanager` | l'arbitre de YARN : qui obtient de la RAM et des CPU |
| `nodemanager` | l'exécutant de YARN : **c'est ici que le calcul a lieu** |
| `spark-client` | la machine d'où l'on soumet — elle ne calcule pas |

Le premier lancement construit l'image `spark-client` (image Hadoop + binaires
Spark officiels) : comptez quelques minutes.

| Interface | URL | À quoi ça sert |
|---|---|---|
| HDFS (NameNode) | http://localhost:9870 | voir les fichiers et les blocs |
| YARN (ResourceManager) | http://localhost:8088 | suivre les applications |
| NodeManager | http://localhost:8042 | voir les conteneurs qui tournent |
| Application Spark | http://localhost:4040 | suivre les *jobs*, *stages* et le DAG |

> L'onglet 4040 n'existe que **pendant** l'exécution d'une application.

Avant d'aller plus loin, vérifiez que YARN a bien un exécutant :

```bash
docker exec spark-client yarn node -list
```

Vous devez lire `Total Nodes:1`. Si vous lisez `Total Nodes:0`, le NodeManager
n'a pas fini de s'enregistrer : attendez 30 secondes et refaites la commande.

---

## 2. Charger les données dans HDFS

```bash
docker exec -it spark-client bash

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

Résultat : `job/target/exces-spark.jar`, visible dans le conteneur sous
`/job/target/exces-spark.jar` (le dossier `job/` est monté).

> Windows PowerShell : remplacez `$(pwd)` par `${PWD}`.
> Lancez la commande **depuis le dossier du kit**, pas depuis `job/`.

---

## 4. Lancer le job sur YARN

**Version RDD** (l'équivalent direct de MapReduce) :

```bash
docker exec -it spark-client /opt/spark/bin/spark-submit \
  --master yarn --deploy-mode client \
  --class be.henallux.bigdata.ExcesSparkRDD \
  --num-executors 1 --executor-memory 512m --executor-cores 1 \
  --conf spark.yarn.am.memory=512m \
  /job/target/exces-spark.jar \
  /projet/trajets /projet/resultat-rdd
```

**Version DataFrame / SQL** (l'API moderne) :

```bash
docker exec -it spark-client /opt/spark/bin/spark-submit \
  --master yarn --deploy-mode client \
  --class be.henallux.bigdata.ExcesSparkSQL \
  --num-executors 1 --executor-memory 512m --executor-cores 1 \
  --conf spark.yarn.am.memory=512m \
  /job/target/exces-spark.jar \
  /projet/trajets /projet/resultat-sql
```

Celle-ci affiche directement les tableaux dans le terminal.

### Ce qui se passe pendant que ça défile

Vous verrez, dans l'ordre :

1. `Uploading resource .../__spark_libs__.zip` — Spark envoie ses ~200 Mo de
   bibliothèques dans HDFS. Les conteneurs YARN n'ont pas Spark installé : il
   faut le leur livrer. C'est pour ça que la soumission met un moment à démarrer.
2. `Submitting application application_..._0001 to ResourceManager` — le client
   a fini son travail. **À partir d'ici, il ne calcule plus rien.**
3. `state: ACCEPTED` — le ResourceManager a pris la demande, il cherche de la
   place pour l'ApplicationMaster.
4. `state: RUNNING` — l'ApplicationMaster tourne dans un conteneur du
   NodeManager et réclame des *executors*.

Suivez tout ça en parallèle sur http://localhost:8088.

> **Pourquoi `--deploy-mode client` ?** Il décide seulement d'où tourne le
> *driver* (le chef d'orchestre) : ici dans `spark-client`, pour que `show()`
> s'affiche dans votre terminal. Les *executors* — ceux qui font réellement le
> calcul — tournent dans tous les cas dans les conteneurs YARN. « client » ne
> veut pas dire « calcul local ».

> **Pourquoi limiter la mémoire ?** Docker Desktop n'alloue souvent que ~4 Go
> au total, et il faut y loger les quatre JVM du cluster *en plus* de votre
> job. Le fichier `config` n'annonce donc que 3 Go à YARN, et interdit à un
> conteneur de dépasser 1,5 Go. Ces options gardent le job largement sous ces
> plafonds. Si vous demandez trop, YARN ne plante pas : il garde votre
> application en `ACCEPTED` faute de place — ou la refuse d'emblée si un seul
> conteneur dépasse la limite. Avec plus de RAM (Settings → Resources), vous
> pouvez relever ces valeurs.

---

## 5. Lire le résultat

```bash
docker exec -it spark-client bash

hdfs dfs -ls /projet/resultat-rdd
hdfs dfs -cat /projet/resultat-rdd/part-*
exit
```

> Notez le `part-*` : Spark écrit **un fichier par partition**, ici deux.
> `_SUCCESS` est le fichier témoin, comme en MapReduce.
>
> Spark refuse d'écrire dans un répertoire existant (sauf en mode `overwrite`,
> que `ExcesSparkSQL` utilise). Pour relancer la version RDD :
> `hdfs dfs -rm -r /projet/resultat-rdd`

---

## Ce qu'il faut comparer

Ouvrez côte à côte le code de ce kit et celui de `../mapreduce` :

| | MapReduce | Spark RDD | Spark DataFrame |
|---|---|---|---|
| Classes à écrire | **3** | 1 | 1 |
| Lignes de logique | ~60 | ~15 | ~5 |
| Shuffle | implicite | `reduceByKey` | `groupBy` |
| Calculer une **moyenne** | difficile | moyen | trivial (`avg`) |
| Ordonnanceur | YARN | YARN | YARN |

La dernière ligne est la plus importante : **le cluster est identique**. YARN ne
sait pas ce qu'est un mapper ni ce qu'est un RDD — il distribue de la mémoire et
des CPU. C'est ce qui permet à MapReduce, Spark, Flink ou Hive de cohabiter sur
la même infrastructure.

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

**Exercice 5 — qui calcule vraiment ?** Pendant qu'un job tourne, ouvrez un
second terminal et lancez :

```bash
docker exec nodemanager ps -ef | grep -c CoarseGrainedExecutorBackend
```

Vous comptez les processus *executor* — et ils sont dans `nodemanager`, pas dans
`spark-client`. Relancez ensuite le job avec `--num-executors 2` et refaites le
compte. *(Si vous manquez de RAM, l'application restera en `ACCEPTED` : c'est
YARN qui vous dit non, et c'est instructif aussi.)*

**Exercice 6 — couper l'arbitre.** Arrêtez le ResourceManager
(`docker compose stop resourcemanager`) puis soumettez un job. Lisez le message
d'erreur, et déduisez-en à qui `spark-submit` parlait vraiment. Redémarrez avec
`docker compose start resourcemanager`.

---

## Arrêter le cluster

```bash
docker compose down          # arrête
docker compose down -v       # arrête et efface les données HDFS
```
