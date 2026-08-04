# Kit — Neo4j (base NoSQL orientée graphe)

Un serveur **Neo4j** prêt à l'emploi, chargé avec le **réseau ferroviaire belge**.

## Prérequis

- **Docker Desktop** ([docker.com](https://www.docker.com/products/docker-desktop/))

### Note pour Windows

À l'installation, Docker Desktop demande d'activer **WSL 2** : acceptez.

Toutes les commandes de ce kit tiennent **sur une seule ligne** et fonctionnent telles quelles dans PowerShell comme dans un terminal macOS ou Linux.

---

## 1. Démarrer Neo4j

Ouvrez un terminal **dans le dossier de ce kit**, puis :

```bash
docker compose up -d
```

Un seul conteneur, `neo4j` : il contient à la fois le serveur et l'interface web. Comptez une trentaine de secondes avant qu'il ne réponde.

Vérifiez :

```bash
docker exec neo4j cypher-shell "RETURN 'ok' AS test"
```

---

## 2. Charger les données

```bash
docker exec neo4j cypher-shell -f /var/lib/neo4j/import/charger.cypher
```

Vous devez lire `gares 29`, puis `liaisons 33`.

Cette fois il y a **deux** fichiers, et ce n'est pas un hasard :

| Fichier | Contenu |
|---|---|
| [data/gares.csv](data/gares.csv) | 29 gares belges, avec leur région |
| [data/liaisons.csv](data/liaisons.csv) | 33 tronçons, avec le n° de ligne SNCB et la distance |

> **Un graphe se charge toujours en deux temps** : d'abord les nœuds, ensuite les relations. On ne peut pas relier deux gares qui n'existent pas encore.
> Ouvrez [data/charger.cypher](data/charger.cypher) : c'est exactement ce qu'il fait.

Les numéros de ligne sont réels (ligne 36 Bruxelles–Liège, ligne 162
Namur–Arlon…). Les distances sont approximatives, arrondies au kilomètre.

---

## 3. Comment les données sont rangées

Le vocabulaire de Neo4j, comparé au SQL que vous connaissez :

| SQL | Neo4j |
|---|---|
| table | **label** (`:Gare`) |
| ligne | **nœud** |
| colonne | **propriété** |
| jointure | **relation** (`:RELIE`) |

Un nœud et une relation ressemblent à ceci :

```
(:Gare {nom: "Namur", region: "Wallonie"})
   -[:RELIE {ligne: "125", distance_km: 61}]->
(:Gare {nom: "Liege-Guillemins", region: "Wallonie"})
```

Trois choses à remarquer :

- **Les relations portent des propriétés.** Le numéro de ligne et la distance sont stockés sur le lien lui-même, pas sur les gares. C'est le propre du modèle graphe : la relation est un objet de première classe, pas une simple clé étrangère.
- **Les relations ont un sens.** Neo4j impose une direction à la création. Mais un rail se parcourt dans les deux sens : dans les requêtes, on écrit donc `-[:RELIE]-` **sans flèche**, ce qui ignore la direction.
- **Il n'y a pas de table de jointure.** En SQL, relier 29 gares par 33 tronçons demanderait une table intermédiaire et un `JOIN` à chaque saut Ici, la relation *est* le chemin.

---

## 4. Lire le contenu

Ouvrez [http://localhost:7474](http://localhost:7474).

Aucun mot de passe n'est demandé. Tapez vos requêtes dans la barre du haut,
puis `Ctrl + Entrée`.

### Voir le réseau en entier

```cypher
MATCH (g:Gare)-[r:RELIE]-(voisine) RETURN g, r, voisine
```

Neo4j dessine le graphe. Faites glisser les nœuds à la souris : la carte du rail belge apparaît, avec Bruxelles au centre.

### Les voisins directs d'une gare

```cypher
MATCH (:Gare {nom: 'Namur'})-[r:RELIE]-(voisine)
RETURN voisine.nom, r.ligne, r.distance_km ORDER BY r.distance_km
```

Namur a cinq voisins : Dinant, Ciney, Ottignies, Charleroi et Liège.

### Le plus court chemin — ce que le graphe fait le mieux

```cypher
MATCH p = shortestPath((:Gare {nom: 'Oostende'})-[:RELIE*]-(:Gare {nom: 'Arlon'}))
RETURN [n IN nodes(p) | n.nom] AS itineraire,
       length(p) AS nb_liaisons,
       reduce(t = 0, r IN relationships(p) | t + r.distance_km) AS km
```

Résultat : Ostende → Bruges → Gand → Denderleeuw → Bruxelles-Midi → Charleroi →
Namur → Ciney → Libramont → Arlon. **9 liaisons, 357 km.**

Le `*` dans `-[:RELIE*]-` veut dire « autant de sauts que nécessaire ». C'est
la requête qui justifie à elle seule l'existence des bases graphe : en SQL,
elle demanderait une jointure récursive, et il faudrait connaître le nombre de
correspondances **à l'avance**.

### Les gares les mieux desservies

```cypher
MATCH (g:Gare)-[r:RELIE]-() RETURN g.nom, count(r) AS liaisons
ORDER BY liaisons DESC LIMIT 5
```

---

## Arrêter

```bash
docker compose down          # arrête, garde les données
docker compose down -v       # arrête et efface le graphe
```
