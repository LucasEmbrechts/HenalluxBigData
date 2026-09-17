# Kit — Spark (sans Hadoop)

Un petit **cluster Spark** prêt à l'emploi, sur lequel on lance des jobs écrits en **Python** (PySpark).

**Le but** : compter les excès de vitesse par camion.

> Le kit [kits/hadoop/spark](../hadoop/spark/) fait le même calcul, mais en Java et sur un cluster Hadoop. Celui-ci montre que **Spark n'a pas besoin de Hadoop** : il fournit son propre gestionnaire de cluster, appelé **Standalone**.

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))
- ~3 Go de RAM disponibles pour Docker

> **Un seul kit à la fois.** Faites `docker compose down` dans les autres kits avant de démarrer celui-ci.

### Note pour Windows

À l'installation, Docker Desktop demande d'activer **WSL 2** : acceptez.

Toutes les commandes de ce kit tiennent **sur une seule ligne** et fonctionnent telles quelles dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer le cluster

Ouvrez un terminal **dans le dossier de ce kit**, puis :

```bash
docker compose up -d
```

Quatre conteneurs démarrent :

| Conteneur | Rôle |
|---|---|
| `spark-master` | le chef : il connaît les workers et leur distribue le travail. **Il ne calcule pas.** |
| `spark-worker-1` | un exécutant : **c'est ici que le calcul a lieu** |
| `spark-worker-2` | un deuxième exécutant |
| `spark-client` | la machine d'où l'on lance les jobs. **Elle ne calcule pas.** |

Ouvrez l'interface du cluster : [http://localhost:8080](http://localhost:8080). Vous devez lire **Alive Workers: 2**. Si vous lisez 0 ou 1, attendez quelques secondes et rechargez la page.

---

## 2. Les données

Le fichier est [data/trajets.csv](data/trajets.csv) : 5 000 relevés au format `camion_id,vitesse,temp_moteur`, les mêmes que dans les autres kits.

Ici, il n'y a pas de HDFS. Pour que chaque worker puisse lire le fichier, le dossier `data/` est partagé avec les conteneurs : chaque machine le voit au même endroit, `/data/trajets.csv`.

> En entreprise, ce dossier partagé serait un stockage accessible par toutes les machines, le plus souvent dans le cloud (Amazon S3, Azure, Google Cloud).

---

## 3. Lancer le job

```bash
docker exec spark-client spark-submit --master spark://spark-master:7077 /job/exces.py
```

`--master spark://spark-master:7077` dit à Spark : « envoie le calcul au cluster dont le chef est `spark-master` ».

Après quelques secondes :

```
root
 |-- camion_id: string (nullable = true)
 |-- vitesse: double (nullable = true)
 |-- temp_moteur: double (nullable = true)

+---------+-----+
|camion_id|count|
+---------+-----+
|   CAM005|   52|
|   CAM014|   51|
|   CAM004|   50|
...
```

Ignorez la ligne `WARN NativeCodeLoader` : c'est un avertissement sans importance.

Pas besoin de compiler : le job est un simple fichier Python, lu directement par Spark.

---

## 4. Lire le code

Ouvrez [job/exces.py](job/exces.py). Il tient en trois étapes :

| Étape | Code | Ce qu'elle fait |
|---|---|---|
| 1. Lire | `spark.read.csv(...)` | le fichier devient un **DataFrame** (un tableau) |
| 2. Calculer | `filter`, `groupBy`, `count`, `orderBy` | garder les vitesses > 90, regrouper par camion, compter |
| 3. Afficher | `show()` | afficher le résultat |

Un point important : **Spark ne calcule rien avant l'étape 3.** Les lignes de l'étape 2 ne font que noter ce qu'il faudra faire. C'est `show()` qui déclenche vraiment le calcul.

---

## 5. Qui calcule vraiment ?

Retournez sur [http://localhost:8080](http://localhost:8080). Dans **Completed Applications**, cliquez sur votre application *Exces de vitesse* : la liste des **executors** montre que le calcul a tourné sur **les deux workers**.

### Arrêter un worker

```bash
docker compose stop spark-worker-2
```

Relancez le job (section 3) : **il fonctionne toujours**. Le cluster n'a plus qu'un worker, qui fait tout le travail.

### Arrêter tous les workers

```bash
docker compose stop spark-worker-1
```

Relancez le job : cette fois, il reste bloqué et affiche en boucle :

```
WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources
```

Spark vous dit qu'il n'a **personne pour calculer**. Arrêtez-le avec `Ctrl+C`, puis redémarrez les workers :

```bash
docker compose start spark-worker-1 spark-worker-2
```

---

## Comparer avec le kit Hadoop

| | [kits/hadoop/spark](../hadoop/spark/) | Ce kit |
|---|---|---|
| Langage | Java | Python |
| Avant de lancer | compiler avec Maven | rien |
| Gestionnaire du cluster | YARN (Hadoop) | Standalone (fourni par Spark) |
| Stockage des données | HDFS | un dossier partagé |
| Calcul | `filter`, `groupBy`, `count` | `filter`, `groupBy`, `count` — **identique** |

Le moteur Spark et ses opérations sont les mêmes dans les deux kits. Ce qui change, c'est **autour** de Spark : le langage, qui gère les machines, et où sont stockées les données.

Aujourd'hui, Spark est le plus souvent utilisé comme dans ce kit : **en Python, sans cluster Hadoop**.

---

## Ce qu'il faut retenir

1. **Spark est un moteur de calcul**, pas un système de stockage : il lit des données ailleurs (fichiers, bases, Kafka…).
2. **Spark n'a pas besoin de Hadoop.** Il peut gérer lui-même son cluster (Standalone), ou utiliser YARN ou Kubernetes.
3. **Le master distribue, les workers calculent.** Moins de workers, c'est moins de puissance ; plus aucun worker, plus de calcul.

---

## Arrêter le cluster

```bash
docker compose down
```
