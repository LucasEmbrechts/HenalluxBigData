# Kit — Spark (en Python)

**Le but** : écrire du code Spark. On lit un fichier, on filtre, on regroupe, on compte, on trie — en Python, puis en SQL.

**Le calcul** : compter les excès de vitesse par camion.

> Spark tourne ici sur **une seule machine**, dans un seul conteneur. Le kit [kits/hadoop/spark](../hadoop/spark/) fait le même calcul sur un cluster Hadoop, et montre comment Spark répartit le travail entre plusieurs machines. **Le code, lui, est le même dans les deux cas** : c'est tout l'intérêt de Spark.

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))
- ~1,5 Go de RAM disponibles pour Docker

### Note pour Windows

À l'installation, Docker Desktop demande d'activer **WSL 2** : acceptez.

Toutes les commandes de ce kit tiennent **sur une seule ligne** et fonctionnent telles quelles dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer Spark

Ouvrez un terminal **dans le dossier de ce kit**, puis :

```bash
docker compose up -d
```

Un seul conteneur démarre, `spark` : c'est votre machine de travail. Tout se passe dedans, et vous n'avez ni Java ni Spark à installer.

Les données sont dans [data/trajets.csv](data/trajets.csv) : 5 000 relevés au format `camion_id,vitesse,temp_moteur`, les mêmes que dans les autres kits. Le conteneur les voit sous `/data/trajets.csv`.

---

## 2. Lancer un programme

```bash
docker exec spark spark-submit /job/exces.py
```

`spark-submit` est la commande qui exécute un programme Spark. Il n'y a rien à compiler : le fichier Python est lu directement.

Ignorez la ligne `WARN NativeCodeLoader` : c'est un avertissement sans importance. Puis :

```
root
 |-- camion_id: string (nullable = true)
 |-- vitesse: double (nullable = true)
 |-- temp_moteur: double (nullable = true)

>>> Version DataFrame
+---------+-----+
|camion_id|count|
+---------+-----+
|   CAM005|   52|
|   CAM014|   51|
|   CAM004|   50|
...
```

---

## 3. Lire le code

Ouvrez [job/exces.py](job/exces.py).

### Lire les données

```python
releves = (
    spark.read
    .option("header", True)       # la première ligne contient les noms des colonnes
    .option("inferSchema", True)  # deviner les types
    .csv("/data/trajets.csv")
)
```

Le fichier devient un **DataFrame** : un tableau avec des colonnes nommées et typées, comme une table SQL. `printSchema()` affiche ces colonnes et leurs types, `show(5)` affiche les premières lignes.

### Calculer

```python
exces = (
    releves
    .filter(col("vitesse") > 90)
    .groupBy("camion_id")
    .count()
    .orderBy(col("count").desc())
)

exces.show(10)
```

Chaque méthode renvoie **un nouveau DataFrame**, qu'on enchaîne. Une habitude utile : écrire l'enchaînement entre parenthèses, une opération par ligne.

### Transformations et actions

C'est la particularité de Spark : les lignes `filter`, `groupBy`, `count`, `orderBy` **ne calculent rien**. Elles ne font que noter ce qu'il faudra faire. C'est `show()` qui déclenche le calcul.

| Type | Exemples | Effet |
|---|---|---|
| **Transformation** | `select`, `filter`, `groupBy`, `agg`, `orderBy`, `withColumn`, `join` | décrit le calcul, ne l'exécute pas |
| **Action** | `show`, `count`, `collect`, `write` | déclenche le calcul |

Si vous commentez tous les `show()` d'un programme, il s'exécute presque instantanément : Spark n'a rien eu à faire.

---

## 4. Les opérations courantes

| Opération | Exemple | Équivalent SQL |
|---|---|---|
| Choisir des colonnes | `releves.select("camion_id", "vitesse")` | `SELECT camion_id, vitesse` |
| Filtrer | `releves.filter(col("vitesse") > 90)` | `WHERE vitesse > 90` |
| Regrouper et compter | `.groupBy("camion_id").count()` | `GROUP BY camion_id` + `COUNT(*)` |
| Calculer | `.agg(avg("vitesse"), max("temp_moteur"))` | `AVG(vitesse)`, `MAX(temp_moteur)` |
| Renommer | `avg("vitesse").alias("vitesse_moyenne")` | `AS vitesse_moyenne` |
| Trier | `.orderBy(col("count").desc())` | `ORDER BY count DESC` |
| Limiter l'affichage | `.show(10)` | `LIMIT 10` |
| Ajouter une colonne | `.withColumn("exces", col("vitesse") > 90)` | une colonne calculée |
| Joindre deux tableaux | `ventes.join(magasins, "magasin_id")` | `JOIN … USING (magasin_id)` |

Les fonctions comme `col`, `avg` ou `round` s'importent depuis `pyspark.sql.functions` :

```python
from pyspark.sql.functions import avg, col, round
```

---

## 5. Le même calcul, en SQL

Spark comprend aussi le SQL. Il faut d'abord donner un nom de table au DataFrame :

```python
releves.createOrReplaceTempView("releves")

spark.sql("""
    SELECT camion_id, COUNT(*) AS nb_exces
    FROM releves
    WHERE vitesse > 90
    GROUP BY camion_id
    ORDER BY nb_exces DESC
""").show(10)
```

Le résultat est **identique** à la version DataFrame, et Spark exécute exactement le même calcul. On choisit la façon d'écrire la plus lisible : le SQL pour une requête classique, les méthodes chaînées quand le calcul se construit en plusieurs morceaux dans un programme.

> Les triples guillemets `"""…"""` permettent d'écrire un texte sur plusieurs lignes en Python : la requête SQL garde ainsi sa mise en forme habituelle.

---

## 6. Essayer des commandes une par une

Pour expérimenter sans écrire de fichier, ouvrez le **shell Spark** :

```bash
docker exec -it spark pyspark
```

La variable `spark` y est déjà prête. Tapez vos commandes, le résultat s'affiche aussitôt :

```python
releves = spark.read.option("header", True).option("inferSchema", True).csv("/data/trajets.csv")
releves.filter(releves.vitesse > 90).count()
```

```
966
```

Quittez avec `exit()`.

---

## 7. Écrire votre propre programme

Créez un fichier dans le dossier [job/](job/), par exemple `job/mon_calcul.py`, puis lancez-le :

```bash
docker exec spark spark-submit /job/mon_calcul.py
```

Le dossier `job/` est partagé avec le conteneur : vos modifications sont prises en compte immédiatement, sans rien reconstruire ni redémarrer.

Un programme Spark commence toujours par créer une session :

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Mon calcul").getOrCreate()
```

---

## Ce qu'il faut retenir

1. **Un DataFrame est un tableau** : des colonnes nommées et typées, comme une table SQL.
2. **On enchaîne des transformations** (`filter`, `groupBy`, `orderBy`…), et **une action** (`show`, `count`, `write`) déclenche le calcul.
3. **Deux écritures pour le même calcul** : les méthodes Python, ou le SQL. Spark fait exactement la même chose.
4. **Le code ne change pas avec la taille des données** : le même programme tourne sur votre machine ou sur un cluster de cent machines, comme dans le kit [kits/hadoop/spark](../hadoop/spark/).

---

## Arrêter

```bash
docker compose down
```
