# Projet d'exemple — SQL → Spark

**Le but** : lire des données dans une base SQL avec Spark, faire un calcul, puis réécrire le résultat dans la base.

Le projet contient **deux bases SQL**, PostgreSQL et MySQL, remplies avec les mêmes données. Le même programme Spark peut lire l'une ou l'autre.

```
PostgreSQL ─┐                         ┌─→ PostgreSQL
  ou        ├─→  SPARK (analyse.py)  ─┤      ou       (table ca_par_ville)
MySQL      ─┘      lit, joint,        └─→ MySQL
                   calcule
```

Les données sont **fictives** : 10 magasins et 20 000 ventes. Spark calcule le **chiffre d'affaires par ville**.

Faites d'abord le kit [kits/spark](../../kits/spark/) : ce projet ne réexplique pas les bases de Spark. Pour rester simple, Spark tourne ici sur une seule machine, sans master ni workers.

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))
- ~2 Go de RAM disponibles pour Docker

> **Un seul projet ou kit à la fois.** Faites `docker compose down` dans les autres dossiers avant de commencer.

Toutes les commandes tiennent **sur une seule ligne** et fonctionnent dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer les bases

Ouvrez un terminal **dans le dossier de ce projet**, puis :

```bash
docker compose up -d
```

| Conteneur | Rôle |
|---|---|
| `postgres` | une base PostgreSQL |
| `mysql` | une base MySQL |

Au premier démarrage, chaque base exécute automatiquement [donnees/init.sql](donnees/init.sql). Ce fichier crée deux tables et les remplit :

| Table | Contenu |
|---|---|
| `magasins` | `magasin_id`, `nom`, `ville` — 10 magasins |
| `ventes` | `vente_id`, `magasin_id`, `date_vente`, `montant` — 20 000 ventes |

---

## 2. Lancer Spark sur PostgreSQL

```bash
docker compose run --rm spark postgres
```

La première fois, Docker prépare l'image Spark : comptez quelques minutes. Spark attend aussi que les deux bases soient prêtes avant de démarrer.

Ignorez la ligne `WARN NativeCodeLoader` : c'est un avertissement sans importance. Puis :

```
>>> Base utilisee : postgres

20000 ventes lues dans 10 magasins
Version DataFrame :
+---------+---------+----------------+
|    ville|nb_ventes|chiffre_affaires|
+---------+---------+----------------+
|Bruxelles|     6048|       771314.46|
|    Namur|     4531|       575814.05|
|    Liège|     3907|       493484.50|
|Charleroi|     1995|       256401.76|
|    Wavre|     1646|       206937.69|
|     Mons|     1225|       149004.56|
|    Arlon|      648|        84481.86|
+---------+---------+----------------+

Version Spark SQL :
+---------+---------+----------------+
|    ville|nb_ventes|chiffre_affaires|
+---------+---------+----------------+
|Bruxelles|     6048|       771314.46|
...

>>> Resultat enregistre dans la table ca_par_ville (postgres)
```

Le même tableau s'affiche deux fois : il est calculé de deux façons différentes (voir section 5).

---

## 3. Lancer Spark sur MySQL

Même commande, en changeant seulement le nom de la base :

```bash
docker compose run --rm spark mysql
```

Le tableau est **identique** : les deux bases contiennent les mêmes données, et le calcul est le même.

---

## 4. Lire le résultat dans la base

Spark a créé une nouvelle table, `ca_par_ville`, dans chaque base.

**Dans PostgreSQL :**

```bash
docker exec -it postgres psql -U bigdata
```

```sql
SELECT * FROM ca_par_ville;
```

Quittez avec `\q`.

**Dans MySQL :**

```bash
docker exec -it mysql mysql -ubigdata -pbigdata bigdata
```

```sql
SELECT * FROM ca_par_ville;
```

Quittez avec `exit`. (MySQL affiche `Using a password on the command line interface can be insecure` : c'est normal ici, ignorez-le.)

---

## 5. Lire le job Spark

Ouvrez [spark/analyse.py](spark/analyse.py).

### Les deux versions

En haut du fichier, le dictionnaire `CONNEXIONS` contient les deux versions côte à côte :

| | PostgreSQL | MySQL |
|---|---|---|
| `url` | `jdbc:postgresql://postgres:5432/bigdata` | `jdbc:mysql://mysql:3306/bigdata` |
| `driver` | `org.postgresql.Driver` | `com.mysql.cj.jdbc.Driver` |
| `user` / `password` | `bigdata` / `bigdata` | `bigdata` / `bigdata` |

**C'est la seule différence entre les deux bases.** Tout le reste du programme est identique.

Spark se connecte aux bases SQL par **JDBC**, la méthode standard de Java pour parler à une base de données. Chaque base a besoin de son **pilote** (*driver*) : une bibliothèque fournie par l'éditeur. Les deux pilotes sont ajoutés à Spark dans [spark/Dockerfile](spark/Dockerfile).

### Les trois étapes

| Étape | Code | Ce qu'elle fait |
|---|---|---|
| 1. Lire | `spark.read.format("jdbc")` | chaque table SQL devient un DataFrame |
| 2. Calculer | `join`, `groupBy`, `agg` — puis `spark.sql(...)` | joindre les ventes à leur magasin, puis regrouper par ville |
| 3. Écrire | `resultat.write.format("jdbc")` | créer la table `ca_par_ville` dans la base |

### Deux façons d'écrire le même calcul

L'étape 2 est écrite **deux fois**, avec le même résultat :

**Version DataFrame** : des méthodes chaînées.

```python
resultat = (
    ventes
    .join(magasins, "magasin_id")
    .groupBy("ville")
    .agg(
        count("*").alias("nb_ventes"),
        round(sum("montant"), 2).alias("chiffre_affaires"),
    )
    .orderBy(col("chiffre_affaires").desc())
)
```

**Version Spark SQL** : du SQL. On donne d'abord un nom de table à chaque DataFrame avec `createOrReplaceTempView`.

```python
ventes.createOrReplaceTempView("ventes")
magasins.createOrReplaceTempView("magasins")

resultat_sql = spark.sql("""
    SELECT ville, COUNT(*) AS nb_ventes, ROUND(SUM(montant), 2) AS chiffre_affaires
    FROM ventes JOIN magasins USING (magasin_id)
    GROUP BY ville
    ORDER BY chiffre_affaires DESC
""")
```

Chaque ligne a son équivalent :

| DataFrame | Spark SQL |
|---|---|
| `.join(magasins, "magasin_id")` | `JOIN magasins USING (magasin_id)` |
| `.groupBy("ville")` | `GROUP BY ville` |
| `count("*").alias("nb_ventes")` | `COUNT(*) AS nb_ventes` |
| `round(sum("montant"), 2)` | `ROUND(SUM(montant), 2)` |
| `.orderBy(col("chiffre_affaires").desc())` | `ORDER BY chiffre_affaires DESC` |

Spark transforme les deux versions en **le même plan de calcul** : même résultat, même vitesse. On choisit selon ce qui est le plus lisible.

> **Attention à ne pas confondre.** Le « SQL » du nom de ce projet désigne les **bases** PostgreSQL et MySQL d'où viennent les données. **Spark SQL**, lui, est une façon d'écrire un calcul **à l'intérieur de Spark**, quelle que soit la source des données.

---

## Pourquoi Spark, si SQL sait déjà faire ce calcul ?

Bonne question : ici, une seule requête SQL donne exactement le même résultat.

```sql
SELECT ville, COUNT(*) AS nb_ventes, ROUND(SUM(montant), 2) AS chiffre_affaires
FROM ventes JOIN magasins USING (magasin_id)
GROUP BY ville
ORDER BY chiffre_affaires DESC;
```

Avec 20 000 ventes, la base s'en sort très bien toute seule. Spark devient utile quand :

- **les données sont trop volumineuses** pour qu'une seule base les calcule en un temps raisonnable : Spark répartit le calcul sur plusieurs machines ;
- **les données viennent de plusieurs sources** : joindre une table PostgreSQL avec des fichiers sur HDFS ou un flux Kafka, ce qu'aucune base ne sait faire seule ;
- **la base sert déjà une application** : lancer un calcul lourd dessus ralentirait le site ou l'application. Spark lit les données une fois, puis calcule de son côté.

Ce projet montre la **technique** (lire et écrire une base SQL avec Spark) sur des données petites, pour que tout tourne sur votre ordinateur.

---

## Changer de source de données

- **Utiliser vos propres tables** : remplacez [donnees/init.sql](donnees/init.sql), puis adaptez les noms de tables et le calcul (étapes 1 et 2, dans ses deux versions) dans `analyse.py`. `init.sql` n'est exécuté qu'au **premier** démarrage : faites `docker compose down -v` pour que les bases soient recréées.
- **Utiliser une autre base SQL** (SQL Server, Oracle…) : ajoutez une entrée dans `CONNEXIONS` et son pilote JDBC dans `spark/Dockerfile`.

---

## Arrêter

```bash
docker compose down          # arrête, garde les données
docker compose down -v       # arrête et efface les données
```
