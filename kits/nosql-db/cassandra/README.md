# Kit — Cassandra (base NoSQL orientée colonnes)

Un **cluster de trois nœuds Cassandra**, prêt à l'emploi.

C'est aussi le seul kit où l'on travaille sur **plusieurs machines à la fois**, et où l'on peut en débrancher une pour voir ce qui se passe.

Rappel : Cassandra est distribué *by design*.

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

Le vocabulaire de Cassandra, comparé au SQL que vous connaissez :

| SQL | Cassandra |
|---|---|
| base de données | **keyspace** |
| table | table |
| ligne | ligne |
| colonne | colonne |
| clé primaire | clé primaire (**en deux parties**) |

Concernant la clé primaire :

```
PRIMARY KEY ((camion_id), releve_id)
             └─────────┘  └───────┘
              partition    clustering
```

- **La clé de partition** décide **sur quelle machine** la donnée est stockée. Cassandra en calcule une empreinte, et cette empreinte tombe dans l'une des plages de l'anneau vues plus haut.
- **La clé de clustering** décide **dans quel ordre** les lignes sont rangées à l'intérieur d'une partition.

Ici, le fait de choisir `camion_id` comme clé de partition garantit que toutes les données d'un même camion sont stockées sur la même machine.


| Déclaration | Clé de partition | Clé de clustering |
|---|---|---|
| `PRIMARY KEY (a, b, c)` | `a` | `b`, `c` |
| `PRIMARY KEY ((a), b, c)` | `a` | `b`, `c` |
| `PRIMARY KEY ((a, b), c)` | `a`, `b` | `c` |
| `PRIMARY KEY (a)` | `a` | *aucune* |

Les deux premières lignes sont **identiques** : quand la clé de partition n'a qu'une seule colonne, les parenthèses intérieures sont facultatives. La règle par défaut est que **la première colonne est la clé de partition**. Les parenthèses ne deviennent obligatoires que pour en regrouper plusieurs.

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
> Refaites l'essai en changeant les parenthèses sur une table jetable, et regardez `kind` basculer.

Sans le `releve_id`, la clé aurait été `camion_id` seul. Or le fichier ne contient que **25 camions** : les quelque 200 relevés de CAM012 auraient tous eu **la même clé primaire** — et dans Cassandra, écrire sur une clé existante ne provoque pas d'erreur, ça écrase. Les 5 000 relevés se seraient silencieusement réduits à 25.

---

## 3. Distribution des données sur le cluster

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

## 4. Extraire les données

Ouvrez un shell CQL :

```bash
docker exec -it cassandra-1 cqlsh
```

### Requête 1 : afficher tous les relevés d'un camion

```sql
SELECT * FROM bigdata.releves WHERE camion_id = 'CAM012' LIMIT 5;
```

Cassandra calcule l'empreinte de `CAM012`, sait exactement quel nœud interroger, et lit une seule partition.

### Requête 2 : afficher tous les relevés dont la vitesse dépasse 90km/h

#### Solution non fonctionnelle

```sql
SELECT * FROM bigdata.releves WHERE vitesse > 90;
```

```
InvalidRequest: ... Cannot execute this query as it might involve data filtering and thus may have unpredictable performance.
```

`vitesse` ne fait pas partie de la clé, donc Cassandra n'a **aucun moyen de savoir où chercher**. Répondre l'obligerait à interroger les trois nœuds et à tout relire.

#### Solution "bricole"

Il est possible néanmoins de forcer la requête :

```sql
SELECT * FROM bigdata.releves WHERE vitesse > 90 ALLOW FILTERING;
```

Sur 5 000 lignes, la réponse arrive. Sur 5 milliards, elle n'arriverait jamais.
`ALLOW FILTERING` est une "bricole", pas une solution.

#### Cassandra <> balayage complet

Il est important de comprendre que Cassandra n'est pas faite pour ce genre de requête.

En effet, « Tous les relevés au-dessus de 90 » est un balayage analytique complet, et un système distribué comme Cassandra n'est pas du tout efficace pour un tel balayage.

Une solution consisterait à créer une table à partition unique dans laquelle tous les excès seraient stockés dans cette seule partition. Ça marche, mais ce n'est pas du tout conseillé.


---

## 5. Une table par question

### Un premier exemple

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

### D'autres exemples en vrac

#### 1 — « Quels sont les relevés du camion CAM012 ? »

```sql
CREATE TABLE releves (
  camion_id    text,
  releve_id    int,
  vitesse      double,
  temp_moteur  double,
  PRIMARY KEY ((camion_id), releve_id)
);

SELECT *
FROM releves
WHERE camion_id = 'CAM012';
```

La table de base du kit : une partition par camion, les relevés rangés par numéro croissant.

#### 2 — « Quels sont les 5 relevés les plus rapides du camion CAM012 ? »

```sql
CREATE TABLE releves_par_vitesse (
  camion_id    text,
  vitesse      double,
  releve_id    int,
  temp_moteur  double,
  PRIMARY KEY ((camion_id), vitesse, releve_id)
) WITH CLUSTERING ORDER BY (vitesse DESC, releve_id ASC);

SELECT * FROM releves_par_vitesse
WHERE camion_id = 'CAM012'
LIMIT 5;
```

Mêmes données que la table 1, rangement différent. `LIMIT 5` suffit : le tri est déjà fait sur le disque.

#### 3 — « Quelle est la température moteur maximale du camion CAM012 ? »

```sql
CREATE TABLE releves_par_temperature (
  camion_id    text,
  temp_moteur  double,
  releve_id    int,
  vitesse      double,
  PRIMARY KEY ((camion_id), temp_moteur, releve_id)
) WITH CLUSTERING ORDER BY (temp_moteur DESC, releve_id ASC);

SELECT temp_moteur
FROM releves_par_temperature
WHERE camion_id = 'CAM012' LIMIT 1;
```

Il n'existe pas de `MAX()` utilisable ici. On le remplace par « trie à l'écriture, prends la première ligne » — au prix d'une troisième copie des mêmes 5 000 relevés.

#### 4 — « Donne-moi le relevé n°4217 »

```sql
CREATE TABLE releve_par_id (
  releve_id    int,
  camion_id    text,
  vitesse      double,
  temp_moteur  double,
  PRIMARY KEY ((releve_id))
);

SELECT *
FROM releve_par_id
WHERE releve_id = 4217;
```

La clé inversée : parfaite pour cette question, inutilisable pour toutes les autres. Aucune clé de clustering, puisqu'il n'y a qu'une ligne par partition.

#### 5 — « Quelle est la liste de tous les camions ? »

```sql
CREATE TABLE camions (
  flotte     text,
  camion_id  text,
  PRIMARY KEY ((flotte), camion_id)
);

SELECT camion_id
FROM camions
WHERE flotte = 'nord';
```

Question banale en SQL (`SELECT DISTINCT camion_id`), impossible ici sans balayage complet. Il faut une petite table d'index dont la clé de partition est une **constante** qui regroupe tout.

> Une clé de partition constante crée une partition unique. C'est acceptable
> pour 25 camions, catastrophique pour 25 millions : toute la table tiendrait
> sur une seule machine.

Pour rappel, Cassandra n'est pas fait pour ce genre de requête. Préférez un stockage orienté clé/valeur.

#### 6 — « Combien d'excès de vitesse pour le camion CAM012 ? »

```sql
CREATE TABLE exces_par_camion (
  camion_id  text,
  nb         counter,
  PRIMARY KEY ((camion_id))
);

UPDATE exces_par_camion
SET nb = nb + 1
WHERE camion_id = 'CAM012';

SELECT nb FROM exces_par_camion
WHERE camion_id = 'CAM012';
```

Le type `counter` sait s'incrémenter sans relire la valeur, ce qui reste correct même quand plusieurs clients écrivent en même temps. Attention : c'est **votre programme** qui déclenche l'incrément à chaque excès détecté, Cassandra ne le fait pas toute seule — il n'existe ni trigger ni contrainte.

À noter : `SELECT COUNT(*) FROM releves WHERE camion_id = 'CAM012'` fonctionne aussi, parce que l'agrégat reste dans **une seule partition**. C'est le `COUNT(*)` sans `WHERE` qui déclenche l'avertissement `Aggregation query used without partition key`.

#### 7 — « Quels camions ont dépassé 90 km/h aujourd'hui, du plus rapide au plus lent ? »

```sql
CREATE TABLE exces_par_jour (
  jour       text,
  vitesse    double,
  camion_id  text,
  releve_id  int,
  PRIMARY KEY ((jour), vitesse, camion_id, releve_id)
) WITH CLUSTERING ORDER BY (vitesse DESC, camion_id ASC, releve_id ASC);

SELECT *
FROM exces_par_jour
WHERE jour = '2026-08-14'
LIMIT 10;
```

Cette table ne peut pas être remplie : `trajets.csv` ne contient aucune date (manque une colonne à la source).

Le `jour` sert ici de clé de partition : il regroupe les excès d'une journée dans une partition, et empêche celle-ci de grossir indéfiniment.

#### 8 — « Quels relevés du camion CAM012 dépassent 90 km/h ? »

```sql
SELECT * FROM releves_par_vitesse WHERE camion_id = 'CAM012' AND vitesse > 90;
```

#### 9 — « Les relevés des camions CAM001, CAM012 et CAM025 »

```sql
SELECT *
FROM releves
WHERE camion_id
IN ('CAM001', 'CAM012', 'CAM025');
```

`IN` sur la clé de partition demande à Cassandra d'interroger trois machines et de rassembler les réponses. Acceptable pour 3 valeurs ; à éviter pour 300.

#### 10 — « Les relevés n°100 à 200 du camion CAM012 »

```sql
SELECT * FROM releves
WHERE camion_id = 'CAM012'
AND releve_id >= 100
AND releve_id <= 200;
```

Un intervalle sur la clé de clustering. À l'intérieur d'une partition, on retrouve une liberté proche du SQL : filtres, intervalles, tri inversé.

#### 11 — « Quel est le dernier relevé connu de chaque camion ? »

```sql
CREATE TABLE dernier_releve (
  camion_id    text,
  vitesse      double,
  temp_moteur  double,
  PRIMARY KEY ((camion_id))
);

INSERT INTO dernier_releve
(camion_id, vitesse, temp_moteur)
VALUES ('CAM012', 97.3, 101.2);

SELECT *
FROM dernier_releve
WHERE camion_id = 'CAM012';
```

Avec `camion_id` comme clé de partition et aucune clé de clustering, la clé primaire se réduit à `camion_id` seul : chaque nouvel envoi pour un camion écrase le précédent. La table contient donc exactement une ligne par camion.

#### 12 — « Les relevés du camion CAM012 pour août 2026 »

```sql
CREATE TABLE releves_par_mois (
  camion_id  text,
  mois       text,
  releve_id  int,
  vitesse    double,
  PRIMARY KEY ((camion_id, mois), releve_id)
);

SELECT *
FROM releves_par_mois
WHERE camion_id = 'CAM012'
AND mois = '2026-08';
```

Un exemple de clé de partition composite : `camion_id` *et* `mois` désignent ensemble la machine. Il empêche la partition d'un camion de grossir sans fin, puisqu'une nouvelle partition démarre chaque mois.

> Conséquence  : `WHERE camion_id = 'CAM012'` seul ne fonctionne plus. Pour localiser une partition, Cassandra exige toutes les colonnes de la clé de partition, jamais une partie.

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

Douze questions, neuf tables. Trois questions n'ont rien coûté : elles étaient déjà servies par une table conçue pour une autre.

C'est la nuance à retenir. « Une question, une table » est le point de départ du raisonnement, pas une fatalité : une clé de partition bien choisie sert souvent plusieurs questions à la fois. Ce qu'on paie en espace disque et en discipline d'écriture, on le gagne en temps de réponse et en tolérance aux pannes.

---

## 6. Débrancher une machine

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
