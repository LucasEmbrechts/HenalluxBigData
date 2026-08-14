# Kit — Cassandra (base NoSQL orientée colonnes)

Un **cluster de trois nœuds Cassandra**, prêt à l'emploi.

C'est aussi le seul kit où l'on travaille sur **plusieurs machines à la fois**, et où l'on peut en débrancher une pour voir ce qui se passe.

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))
- **~3 Go de RAM** disponibles pour Docker (trois nœuds à 512 Mo de tas chacun)

### Note pour Windows

À l'installation, Docker Desktop demande d'activer **WSL 2** : acceptez.

Toutes les commandes de ce kit tiennent **sur une seule ligne** et fonctionnent telles quelles dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer le cluster

Ouvrez un terminal **dans le dossier de ce kit**, puis :

```bash
docker compose up -d
```

Trois conteneurs, **tous identiques** :

| Conteneur | Rôle |
|---|---|
| `cassandra-1` | un nœud du cluster |
| `cassandra-2` | un nœud du cluster |
| `cassandra-3` | un nœud du cluster |

Dans HDFS, il fallait distinguer le NameNode du DataNode. Ici, **il n'y a pas de chef**. Les trois nœuds ont le même rôle, le même code et les mêmes droits.

> **Comptez 3 à 5 minutes.** Les nœuds démarrent volontairement l'un après l'autre : deux nœuds qui rejoignent l'anneau en même temps se disputent les mêmes jetons. Docker les enchaîne tout seul, laissez faire.

Vérifiez que l'anneau est formé :

```bash
docker exec cassandra-1 nodetool status
```

Vous devez lire **trois lignes commençant par `UN`** (*Up / Normal*) :

```
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load       Tokens  Owns (effective)  Host ID   Rack
UN  172.19.0.2  109.7 KiB  16      100.0%            2b3c...   rack1
UN  172.19.0.3  75.32 KiB  16      100.0%            8f1a...   rack1
UN  172.19.0.4  69.11 KiB  16      100.0%            c04d...   rack1
```

Si vous ne voyez qu'une ou deux lignes, un nœud n'a pas fini de rejoindre : attendez une minute et refaites la commande.

> La colonne **Tokens** vaut 16 pour chacun : chaque nœud est responsable de 16 plages de valeurs sur l'anneau. C'est ce découpage qui décide, tout à l'heure, quelle donnée ira sur quelle machine.

---

## 2. Charger les données

```bash
docker exec cassandra-1 sh /kit/charger.sh
```

Le script affiche ses trois étapes, puis :

```
 count
-------
  5000
```

Le fichier de départ est [data/trajets.csv](data/trajets.csv) : 5 000 relevés au format `camion_id,vitesse,temp_moteur`, les mêmes que dans les kits Hadoop, MongoDB et Redis.

Ouvrez maintenant [data/charger.sh](data/charger.sh) et lisez-le. Il fait trois choses, et la deuxième mérite qu'on s'arrête dessus.

---

## 3. Pourquoi il a fallu numéroter les relevés

Le vocabulaire de Cassandra, comparé au SQL que vous connaissez :

| SQL | Cassandra |
|---|---|
| base de données | **keyspace** |
| table | table |
| ligne | ligne |
| colonne | colonne |
| clé primaire | clé primaire — **mais en deux morceaux** |

Tout se ressemble, sauf le dernier point, et c'est là que tout se joue :

```sql
PRIMARY KEY ((camion_id), releve_id)
             └─────────┘  └───────┘
              partition    clustering
```

- **La clé de partition** (`camion_id`) décide **sur quelle machine** la donnée est stockée. Cassandra en calcule une empreinte, et cette empreinte tombe dans l'une des plages de l'anneau vues plus haut.
- **La clé de clustering** (`releve_id`) décide **dans quel ordre** les lignes sont rangées à l'intérieur d'une partition.

Ici, le fait de dire que `camion_id` est la clé de partition permet de faire en sorte que toutes les données d'un même camion soient stockées sur la même machine.

### Comment Cassandra sait laquelle est laquelle

Il n'existe pas de mot-clé `PARTITION` ni `CLUSTERING` : **c'est la ponctuation
seule qui le dit.** Les parenthèses *intérieures* délimitent la clé de
partition ; tout ce qui suit est clé de clustering.

| Déclaration | Clé de partition | Clé de clustering |
|---|---|---|
| `PRIMARY KEY (a, b, c)` | `a` | `b`, `c` |
| `PRIMARY KEY ((a), b, c)` | `a` | `b`, `c` |
| `PRIMARY KEY ((a, b), c)` | `a`, `b` | `c` |
| `PRIMARY KEY (a)` | `a` | *aucune* |

Les deux premières lignes sont **identiques** : quand la clé de partition n'a
qu'une seule colonne, les parenthèses intérieures sont facultatives. La règle
par défaut est que **la première colonne est la clé de partition**. Les
parenthèses ne deviennent obligatoires que pour en regrouper plusieurs.

Nous les écrivons quand même, pour que la frontière reste visible à l'œil.

> **Vérifiez-le vous-même** — Cassandra sait classer ses propres colonnes :
>
> ```bash
> docker exec cassandra-1 cqlsh -e "SELECT column_name, kind, position FROM system_schema.columns WHERE keyspace_name='bigdata' AND table_name='releves';"
> ```
>
> ```
>  column_name | kind          | position
> -------------+---------------+----------
>    camion_id | partition_key |        0
>    releve_id |    clustering |        0
>  temp_moteur |       regular |       -1
>      vitesse |       regular |       -1
> ```
>
> Refaites l'essai en changeant les parenthèses sur une table jetable, et
> regardez `kind` basculer.

Sans le `releve_id`, la clé aurait été `camion_id` seul. Or le fichier ne contient que **25 camions** : les quelque 200 relevés de CAM012 auraient tous eu **la même clé primaire** — et dans Cassandra, écrire sur une clé existante ne provoque pas d'erreur, ça écrase. Les 5 000 relevés se seraient silencieusement réduits à 25.

> **Essayez-le**, c'est instructif :
>
> ```bash
> docker exec cassandra-1 cqlsh -e "CREATE TABLE bigdata.mauvais (camion_id text PRIMARY KEY, vitesse double, temp_moteur double); COPY bigdata.mauvais (camion_id, vitesse, temp_moteur) FROM '/kit/trajets.csv' WITH HEADER=true; SELECT COUNT(*) FROM bigdata.mauvais;"
> ```
>
> Vous avez inséré 5 000 lignes. Il en reste **25**. Aucun message d'erreur.

---

## 4. Où vit chaque camion

Demandez à Cassandra quelles machines détiennent les relevés d'un camion :

```bash
docker exec cassandra-1 nodetool getendpoints bigdata releves CAM012
```

Trois adresses IP s'affichent — parce que le keyspace a été créé avec
`replication_factor: 3`. Chaque donnée existe en trois exemplaires, sur trois
nœuds différents.

Essayez avec un autre camion :

```bash
docker exec cassandra-1 nodetool getendpoints bigdata releves CAM003
```

Avec trois nœuds et trois copies, tout le monde a tout : les listes sont identiques. Ce qui change, c'est **l'ordre** — le premier de la liste est le nœud « naturel » pour cette clé, celui que Cassandra interrogera en priorité.
Sur un vrai cluster de vingt machines, les listes seraient différentes.

---

## 5. Les requêtes qui marchent, et celles qui ne marchent pas

Ouvrez un shell CQL :

```bash
docker exec -it cassandra-1 cqlsh
```

**Ceci fonctionne, et instantanément :**

```sql
SELECT * FROM bigdata.releves WHERE camion_id = 'CAM012' LIMIT 5;
```

Cassandra calcule l'empreinte de `CAM012`, sait exactement quel nœud interroger, et lit une seule partition.

**Ceci échoue :**

```sql
SELECT * FROM bigdata.releves WHERE vitesse > 90;
```

```
InvalidRequest: ... Cannot execute this query as it might involve data filtering and thus may have unpredictable performance.
```

Ce n'est pas une limitation qu'on aurait oublié de lever : `vitesse` ne fait pas partie de la clé, donc Cassandra n'a **aucun moyen de savoir où chercher**. Répondre l'obligerait à interroger les trois nœuds et à tout relire.

Vous *pouvez* le forcer :

```sql
SELECT * FROM bigdata.releves WHERE vitesse > 90 ALLOW FILTERING;
```

Sur 5 000 lignes, la réponse arrive. Sur 5 milliards, elle n'arriverait jamais.
`ALLOW FILTERING` est un aveu, pas une solution.

### La bonne réponse : une table par question

En SQL, on écrit la table d'abord et les requêtes ensuite. **En Cassandra, on part des questions.**

Prenons celle-ci : *quels sont les cinq relevés les plus rapides du camion CAM012 ?* Sur la table `releves`, il faudrait tout relire puis trier. On crée donc une seconde table, rangée pour cette question-là :

```sql
CREATE TABLE bigdata.releves_par_vitesse (
  camion_id    text,
  vitesse      double,
  releve_id    int,
  temp_moteur  double,
  PRIMARY KEY ((camion_id), vitesse, releve_id)
) WITH CLUSTERING ORDER BY (vitesse DESC, releve_id ASC);
```

> `CLUSTERING ORDER BY` doit énumérer **toutes** les colonnes de clustering, dans l'ordre où elles apparaissent dans la clé — sinon Cassandra refuse la table.

Quittez le shell (`exit`) et remplissez-la avec le fichier numéroté que
`charger.sh` a laissé dans le conteneur :

```bash
docker exec cassandra-1 cqlsh -e "COPY bigdata.releves_par_vitesse (camion_id, releve_id, vitesse, temp_moteur) FROM '/tmp/releves-numerotes.csv';"
```

La question trouve maintenant sa réponse immédiatement, sans `ALLOW FILTERING` :

```bash
docker exec cassandra-1 cqlsh -e "SELECT * FROM bigdata.releves_par_vitesse WHERE camion_id = 'CAM012' LIMIT 5;"
```

Les relevés sortent déjà triés du plus rapide au plus lent : c'est la clé de clustering `vitesse DESC` qui les a stockés dans cet ordre. Cassandra ne trie rien au moment de la lecture — **elle lit dans l'ordre où elle a écrit**.

Les mêmes 5 000 relevés sont maintenant écrits à deux endroits. C'est volontaire, ça porte un nom — la **dénormalisation** — et c'est exactement l'inverse de ce qu'on vous a appris en SQL. Le disque coûte moins cher que le temps de réponse.

---

## 6. Une question, une table

Pour s'entraîner, voici treize questions posées sur les mêmes données, et ce que chacune impose. Le principe ne change jamais : **on part de la question, jamais des données**.

Les sept premières demandent chacune leur propre table. Les suivantes montrent qu'une table bien conçue en sert souvent plusieurs.

### 1 — « Quels sont les relevés du camion CAM012 ? »

```sql
CREATE TABLE releves (
  camion_id    text,
  releve_id    int,
  vitesse      double,
  temp_moteur  double,
  PRIMARY KEY ((camion_id), releve_id)
);

SELECT * FROM releves WHERE camion_id = 'CAM012';
```

La table de base du kit : une partition par camion, les relevés rangés par
numéro croissant.

### 2 — « Quels sont les 5 relevés les plus rapides du camion CAM012 ? »

```sql
CREATE TABLE releves_par_vitesse (
  camion_id    text,
  vitesse      double,
  releve_id    int,
  temp_moteur  double,
  PRIMARY KEY ((camion_id), vitesse, releve_id)
) WITH CLUSTERING ORDER BY (vitesse DESC, releve_id ASC);

SELECT * FROM releves_par_vitesse WHERE camion_id = 'CAM012' LIMIT 5;
```

Mêmes données que la table 1, rangement différent. `LIMIT 5` suffit : le tri
est déjà fait sur le disque.

### 3 — « Quelle est la température moteur maximale du camion CAM012 ? »

```sql
CREATE TABLE releves_par_temperature (
  camion_id    text,
  temp_moteur  double,
  releve_id    int,
  vitesse      double,
  PRIMARY KEY ((camion_id), temp_moteur, releve_id)
) WITH CLUSTERING ORDER BY (temp_moteur DESC, releve_id ASC);

SELECT temp_moteur FROM releves_par_temperature WHERE camion_id = 'CAM012' LIMIT 1;
```

Il n'existe pas de `MAX()` utilisable ici. On le remplace par « trie à
l'écriture, prends la première ligne » — au prix d'une troisième copie des
mêmes 5 000 relevés.

### 4 — « Donne-moi le relevé n°4217 »

```sql
CREATE TABLE releve_par_id (
  releve_id    int,
  camion_id    text,
  vitesse      double,
  temp_moteur  double,
  PRIMARY KEY ((releve_id))
);

SELECT * FROM releve_par_id WHERE releve_id = 4217;
```

La clé inversée : parfaite pour cette question, inutilisable pour toutes les
autres. Aucune clé de clustering, puisqu'il n'y a qu'une ligne par partition.

### 5 — « Quelle est la liste de tous les camions ? »

```sql
CREATE TABLE camions (
  flotte     text,
  camion_id  text,
  PRIMARY KEY ((flotte), camion_id)
);

SELECT camion_id FROM camions WHERE flotte = 'nord';
```

Question banale en SQL (`SELECT DISTINCT camion_id`), impossible ici sans
balayage complet. Il faut une petite table d'index dont la clé de partition
est une **constante** qui regroupe tout.

> Une clé de partition constante crée une partition unique. C'est acceptable
> pour 25 camions, catastrophique pour 25 millions : toute la table tiendrait
> sur une seule machine.

### 6 — « Combien d'excès de vitesse pour le camion CAM012 ? »

```sql
CREATE TABLE exces_par_camion (
  camion_id  text PRIMARY KEY,
  nb         counter
);

UPDATE exces_par_camion SET nb = nb + 1 WHERE camion_id = 'CAM012';
SELECT nb FROM exces_par_camion WHERE camion_id = 'CAM012';
```

Le type `counter` sait s'incrémenter sans relire la valeur, ce qui reste
correct même quand plusieurs clients écrivent en même temps. Attention : c'est **votre programme** qui déclenche l'incrément à chaque excès détecté, Cassandra ne le fait pas toute seule — il n'existe ni trigger ni contrainte.

À noter : `SELECT COUNT(*) FROM releves WHERE camion_id = 'CAM012'` fonctionne aussi, parce que l'agrégat reste dans **une seule partition**. C'est le `COUNT(*)` sans `WHERE` qui déclenche l'avertissement `Aggregation query used without partition key` vu à la fin de `charger.sh`.

### 7 — « Quels camions ont dépassé 90 km/h aujourd'hui, du plus rapide au plus lent ? »

```sql
CREATE TABLE exces_par_jour (
  jour       text,
  vitesse    double,
  camion_id  text,
  releve_id  int,
  PRIMARY KEY ((jour), vitesse, camion_id, releve_id)
) WITH CLUSTERING ORDER BY (vitesse DESC, camion_id ASC, releve_id ASC);

SELECT * FROM exces_par_jour WHERE jour = '2026-08-14' LIMIT 10;
```

Cette table **ne peut pas être remplie** : `trajets.csv` ne contient aucune
date. C'est l'exercice le plus formateur du lot — la modélisation par les
requêtes fait apparaître qu'il manque une colonne à la source, avant même
d'écrire la moindre ligne de code.

Le `jour` sert ici de *bucket* : il regroupe les excès d'une journée dans une
partition, et empêche celle-ci de grossir indéfiniment.

---

Les trois questions suivantes n'exigent **aucune nouvelle table**. Elles se contentent de celles que nous avons déjà.

### 8 — « Quels relevés du camion CAM012 dépassent 90 km/h ? »

```sql
SELECT * FROM releves_par_vitesse WHERE camion_id = 'CAM012' AND vitesse > 90;
```

Regardez bien : c'est **exactement le filtre qui échouait en section 5**, et il passe ici sans `ALLOW FILTERING`.

Rien n'a changé dans les données. Ce qui a changé, c'est que `vitesse` est devenue clé de clustering dans la table 2 : les relevés y sont déjà rangés par vitesse décroissante, donc Cassandra n'a qu'à s'arrêter de lire dès qu'elle passe sous 90.

**Un filtre n'est donc pas légal ou illégal en soi — il l'est en fonction du rangement de la table.** C'est sans doute l'idée la plus importante de tout le kit.

### 9 — « Les relevés des camions CAM001, CAM012 et CAM025 »

```sql
SELECT * FROM releves WHERE camion_id IN ('CAM001', 'CAM012', 'CAM025');
```

`IN` sur la clé de partition demande au coordinateur d'interroger trois machines et de rassembler les réponses. Acceptable pour trois valeurs ; à éviter pour trois cents, où l'on préfère lancer trois cents requêtes en parallèle depuis l'application — le coordinateur n'est pas là pour faire le travail d'un client.

### 10 — « Les relevés n°100 à 200 du camion CAM012 »

```sql
SELECT * FROM releves
  WHERE camion_id = 'CAM012' AND releve_id >= 100 AND releve_id <= 200;
```

Un intervalle sur la clé de clustering. À l'intérieur d'une partition, on retrouve une liberté proche du SQL : filtres, intervalles, tri inversé. **C'est entre les partitions que tout se ferme**, jamais à l'intérieur de l'une d'elles.

> Ne soyez pas surpris du faible nombre de résultats : `releve_id` numérote les 5 000 relevés du fichier entier, pas ceux d'un camion. L'intervalle 100–200 ne contient donc que les quelques relevés de CAM012 tombés dans cette plage — six, en l'occurrence.

---

Les trois dernières demandent à nouveau leur propre table.

### 11 — « Quel est le dernier relevé connu de chaque camion ? »

```sql
CREATE TABLE dernier_releve (
  camion_id    text PRIMARY KEY,
  vitesse      double,
  temp_moteur  double
);

INSERT INTO dernier_releve (camion_id, vitesse, temp_moteur)
  VALUES ('CAM012', 97.3, 101.2);

SELECT * FROM dernier_releve WHERE camion_id = 'CAM012';
```

C'est le miroir exact de la section 3. L'écrasement silencieux qui était un piège devient ici **la fonctionnalité recherchée** : chaque nouvel envoi remplace le précédent, et la table ne dépasse jamais 25 lignes.

Le même comportement est un défaut ou une qualité selon la question posée.

### 12 — « Les relevés du camion CAM012 pour août 2026 » *(demande une date)*

```sql
CREATE TABLE releves_par_mois (
  camion_id  text,
  mois       text,
  releve_id  int,
  vitesse    double,
  PRIMARY KEY ((camion_id, mois), releve_id)
);

SELECT * FROM releves_par_mois WHERE camion_id = 'CAM012' AND mois = '2026-08';
```

Le seul exemple à **clé de partition composite** : `camion_id` *et* `mois` désignent ensemble la machine. C'est le *bucketing* annoncé en question 7 — il empêche la partition d'un camion de grossir sans fin, puisqu'une nouvelle partition démarre chaque mois.

> Conséquence à ne pas manquer : `WHERE camion_id = 'CAM012'` seul ne fonctionne **plus**. Pour localiser une partition, Cassandra exige **toutes** les colonnes de la clé de partition, jamais une partie.

### 13 — « Ne garder les relevés bruts que 30 jours »

```sql
INSERT INTO releves (camion_id, releve_id, vitesse, temp_moteur)
  VALUES ('CAM012', 9001, 88.0, 95.0) USING TTL 2592000;

SELECT TTL(vitesse) FROM releves WHERE camion_id = 'CAM012' AND releve_id = 9001;
```

Le **TTL** (*time to live*, en secondes) fait disparaître la donnée d'elle-même à l'expiration. Le `SELECT` renvoie le nombre de secondes restantes, qui décroît à chaque appel.

C'est la seule chose de tout le kit que la base fasse spontanément, sans qu'on le lui redemande — et elle prend tout son sens sur des données de télémétrie, où l'on garde le détail un mois et les agrégats pour toujours.

### Récapitulatif

| # | Question | Table | Clé de partition | Clustering |
|---|---|---|---|---|
| 1 | Relevés d'un camion | `releves` | `camion_id` | `releve_id` |
| 2 | Les plus rapides d'un camion | `releves_par_vitesse` | `camion_id` | `vitesse DESC`, `releve_id` |
| 3 | Température max d'un camion | `releves_par_temperature` | `camion_id` | `temp_moteur DESC`, `releve_id` |
| 4 | Un relevé par son numéro | `releve_par_id` | `releve_id` | — |
| 5 | Liste des camions | `camions` | `flotte` (constante) | `camion_id` |
| 6 | Nombre d'excès d'un camion | `exces_par_camion` | `camion_id` | — (`counter`) |
| 7 | Excès du jour, triés | `exces_par_jour` | `jour` | `vitesse DESC`, … |
| 8 | Excès d'un camion | *table 2* | — | — |
| 9 | Relevés de plusieurs camions | *table 1* | — | — |
| 10 | Relevés n°100 à 200 | *table 1* | — | — |
| 11 | Dernier relevé de chaque camion | `dernier_releve` | `camion_id` | — |
| 12 | Relevés d'un camion sur un mois | `releves_par_mois` | `camion_id` **+** `mois` | `releve_id` |
| 13 | Purge automatique à 30 jours | *table 1* | — | — (`TTL`) |

Treize questions, **neuf tables**. Quatre questions n'ont rien coûté : elles étaient déjà servies par une table conçue pour une autre.

C'est la nuance à retenir. « Une question, une table » est le point de départ du raisonnement, pas une fatalité : une clé de partition bien choisie sert souvent plusieurs questions à la fois. Ce qu'on paie en espace disque et en discipline d'écriture, on le gagne en temps de réponse et en tolérance aux pannes.

---

## 7. Débrancher une machine

C'est la démonstration qui justifie les trois nœuds.

```bash
docker compose stop cassandra-3
docker exec cassandra-1 nodetool status
```

Le nœud arrêté finit par apparaître en **`DN`** (*Down / Normal*). Comptez une dizaine de secondes : sans chef pour l'annoncer, les nœuds survivants doivent constater eux-mêmes que leur voisin ne répond plus — c'est le *gossip*, la rumeur qu'ils se transmettent en permanence.

Maintenant, relisez des données — en exigeant explicitement que **deux nœuds sur trois** soient d'accord :

```bash
docker exec cassandra-1 cqlsh -e "CONSISTENCY QUORUM; SELECT * FROM bigdata.releves WHERE camion_id = 'CAM012' LIMIT 3;"
```

**Ça répond.** Il reste deux nœuds sur trois, le quorum est atteint, le service continue.

Remettez le nœud en route :

```bash
docker compose start cassandra-3
```

### À comparer avec le kit Hadoop

| | HDFS | Cassandra |
|---|---|---|
| Machine indispensable | le **NameNode** | aucune |
| Si elle tombe | tout le système s'arrête | le cluster répond toujours |
| Ajouter une machine | déclarer le DataNode | elle rejoint l'anneau |

Dans le kit Hadoop, arrêter le `namenode` rend HDFS totalement muet : plus
aucune lecture n'est possible, même si les DataNodes détiennent encore tous les
blocs. Ici, aucune machine n'occupe cette position.

C'est le compromis de Cassandra : elle abandonne le chef — donc la vision
globale et instantanée qu'il permettait — et gagne en échange de continuer à
fonctionner quand une machine meurt.

---

## Ce qu'il faut retenir

1. **La clé de partition décide de la machine.** C'est la décision de
   conception la plus importante, et elle est irréversible.
2. **On modélise à partir des requêtes**, pas à partir des données. Une table
   par question, quitte à écrire la même donnée plusieurs fois.
3. **Pas de chef, pas de point de panne unique** — au prix des jointures, des
   `WHERE` libres et des agrégats, qui n'existent pas ici.

---

## Arrêter

```bash
docker compose down          # arrête, garde les données
docker compose down -v       # arrête et efface les données
```
