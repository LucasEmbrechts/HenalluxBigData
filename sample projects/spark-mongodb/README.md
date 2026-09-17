# Projet d'exemple — Spark → MongoDB

**Le but** : lire des fichiers avec Spark, faire un calcul, puis enregistrer le résultat dans MongoDB, où une application pourrait le consulter rapidement.

```
fichiers CSV  ──→  SPARK (analyse.py)  ──→  MONGODB
 (données          lit, joint,              un document par ville
  brutes)          calcule                  (collection ventes_par_ville)
```

Les données sont **fictives**, les mêmes que dans le projet [sql-spark](../sql-spark/) : 10 magasins et 20 000 ventes. Spark calcule le **chiffre d'affaires par ville et par magasin**.

Faites d'abord les kits [kits/spark](../../kits/spark/) et [kits/nosql-db/mongodb](../../kits/nosql-db/mongodb/) : ce projet ne réexplique pas leurs bases. Pour rester simple, Spark tourne ici sur une seule machine, sans master ni workers.

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))
- ~2 Go de RAM disponibles pour Docker

> **Un seul projet ou kit à la fois.** Ce projet utilise les mêmes noms de conteneurs (`mongo`, `mongo-express`) que le kit MongoDB. Faites `docker compose down` dans les autres dossiers avant de commencer.

Toutes les commandes tiennent **sur une seule ligne** et fonctionnent dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer MongoDB

Ouvrez un terminal **dans le dossier de ce projet**, puis :

```bash
docker compose up -d
```

| Conteneur | Rôle |
|---|---|
| `mongo` | la base MongoDB |
| `mongo-express` | l'interface web de MongoDB |

Les données de départ sont deux fichiers CSV, dans le dossier [donnees/](donnees/) :

| Fichier | Colonnes |
|---|---|
| [magasins.csv](donnees/magasins.csv) | `magasin_id`, `nom`, `ville` — 10 magasins |
| [ventes.csv](donnees/ventes.csv) | `vente_id`, `magasin_id`, `date_vente`, `montant` — 20 000 ventes |

---

## 2. Lancer Spark

```bash
docker compose run --rm spark
```

La première fois, Docker prépare l'image Spark : comptez quelques minutes.

Ignorez la ligne `WARN NativeCodeLoader` : c'est un avertissement sans importance. Puis :

```
20000 ventes lues dans 10 magasins
+---------+---------+----------------+------------------------------------------------------------------------+
|ville    |nb_ventes|chiffre_affaires|magasins                                                                |
+---------+---------+----------------+------------------------------------------------------------------------+
|Bruxelles|6048     |771314.46       |[{Bruxelles Louise, 3809, 488058.58}, {Bruxelles Midi, 2239, 283255.88}]|
|Namur    |4531     |575814.05       |[{Namur Jambes, 1636, 206753.82}, {Namur Centre, 2895, 369060.23}]      |
|Liège    |3907     |493484.5        |[{Liège Guillemins, 2542, 322537.52}, {Liège Rocourt, 1365, 170946.98}] |
...
+---------+---------+----------------+------------------------------------------------------------------------+

>>> Resultat enregistre dans MongoDB : base bigdata, collection ventes_par_ville
```

Les totaux par ville sont les mêmes que dans `sql-spark`. La nouveauté, c'est la colonne `magasins` : **une liste** placée dans chaque ligne.

---

## 3. Lire le résultat dans MongoDB

Ouvrez le shell de MongoDB :

```bash
docker exec -it mongo mongosh bigdata
```

Le document de Namur :

```js
db.ventes_par_ville.find({ ville: "Namur" })
```

```
[
  {
    _id: ObjectId('6aabb2f8a7677c47c5c206ae'),
    ville: 'Namur',
    nb_ventes: Long('4531'),
    chiffre_affaires: 575814.05,
    magasins: [
      { nom: 'Namur Jambes', nb_ventes: Long('1636'), chiffre_affaires: 206753.82 },
      { nom: 'Namur Centre', nb_ventes: Long('2895'), chiffre_affaires: 369060.23 }
    ]
  }
]
```

Chaque ligne du DataFrame est devenue **un document**, et la liste de magasins est **rangée à l'intérieur** du document de sa ville. (`Long` signifie simplement « nombre entier ».)

On peut aussi chercher **à l'intérieur** de la liste — par exemple, dans quelle ville se trouve le magasin « Mons Grand-Place » :

```js
db.ventes_par_ville.find({ "magasins.nom": "Mons Grand-Place" }, { ville: 1, chiffre_affaires: 1, _id: 0 })
```

```
[ { ville: 'Mons', chiffre_affaires: 149004.56 } ]
```

Quittez avec `exit`. Vous pouvez aussi parcourir les documents à la souris sur [http://localhost:8081](http://localhost:8081) : base **bigdata**, collection **ventes_par_ville**.

---

## 4. Lire le job Spark

Ouvrez [spark/analyse.py](spark/analyse.py). Il tient en trois étapes :

| Étape | Code | Ce qu'elle fait |
|---|---|---|
| 1. Lire | `spark.read.csv(...)` | chaque fichier devient un DataFrame |
| 2. Calculer | `join`, `groupBy`, `agg`, `collect_list` | le chiffre d'affaires par magasin, puis par ville |
| 3. Écrire | `par_ville.write.format("mongodb")` | enregistrer un document par ville dans MongoDB |

Le calcul se fait en deux temps :

- **a)** `groupBy("ville", "nom")` : le chiffre d'affaires de **chaque magasin** ;
- **b)** `groupBy("ville")` : on additionne les magasins de chaque ville, et `collect_list(struct(...))` les **rassemble dans une liste**.

Pour écrire dans MongoDB, Spark a besoin du **connecteur MongoDB**, ajouté dans [spark/Dockerfile](spark/Dockerfile). Il suffit ensuite de préciser où écrire :

```python
par_ville.write
    .format("mongodb")
    .option("connection.uri", MONGO)             # l'adresse du serveur
    .option("database", "bigdata")               # la base
    .option("collection", "ventes_par_ville")    # la collection
    .mode("overwrite")                           # remplacer si elle existe déjà
    .save()
```

---

## Pourquoi écrire le résultat dans MongoDB ?

Imaginez un site web avec une page par ville, qui affiche son chiffre d'affaires et celui de chacun de ses magasins.

- **Spark** n'est pas fait pour répondre à un site web : chaque calcul prend plusieurs secondes. On le lance **une fois** (par exemple chaque nuit), sur toutes les données.
- **MongoDB** garde le résultat, déjà calculé. Pour afficher la page de Namur, le site lit **un seul document**, qui contient tout ce qu'il faut : instantané.

| | Table SQL (`sql-spark`) | Document MongoDB (ce projet) |
|---|---|---|
| Chiffre d'affaires de la ville | une ligne | un document |
| Liste de ses magasins | une **autre table**, à joindre | **dans** le même document |
| Pour afficher la page d'une ville | une jointure | une seule lecture |

C'est le rôle le plus courant de MongoDB à côté de Spark : **Spark calcule, MongoDB sert les résultats**.

---

## Changer de source de données

Remplacez les fichiers du dossier [donnees/](donnees/), puis adaptez dans `analyse.py` les noms de fichiers (étape 1) et le calcul (étape 2). L'étape 3 ne change pas, à part le nom de la collection.

En entreprise, ces fichiers seraient sur un stockage partagé (HDFS, Amazon S3…) : seul le chemin passé à `spark.read.csv(...)` changerait.

---

## Arrêter

```bash
docker compose down          # arrête, garde les données
docker compose down -v       # arrête et efface les données
```
