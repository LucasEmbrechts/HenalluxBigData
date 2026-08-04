// Charge le reseau ferroviaire belge dans Neo4j.
//
// Ce fichier est execute PAR Neo4j, a l'interieur du conteneur. Les chemins
// "file:///" designent le repertoire d'import de Neo4j, ou le dossier data/
// du kit est monte.
//
// Un graphe se charge toujours en deux temps : d'abord les noeuds, ensuite
// les relations. On ne peut pas relier deux gares qui n'existent pas encore.

// --- Remise a zero, pour pouvoir relancer le script sans doublons ---
MATCH (n) DETACH DELETE n;

// --- Une contrainte d'unicite sur le nom de gare ---
// Elle garantit qu'on ne cree pas deux fois la meme gare, et cree au passage
// un index qui accelere les recherches par nom.
CREATE CONSTRAINT gare_nom_unique IF NOT EXISTS
FOR (g:Gare) REQUIRE g.nom IS UNIQUE;

// --- 1. Les noeuds : une gare par ligne du CSV ---
LOAD CSV WITH HEADERS FROM 'file:///gares.csv' AS ligne
CREATE (:Gare {nom: ligne.nom, region: ligne.region});

// --- 2. Les relations : une liaison ferroviaire par ligne du CSV ---
// MATCH retrouve les deux gares deja creees, CREATE les relie.
LOAD CSV WITH HEADERS FROM 'file:///liaisons.csv' AS ligne
MATCH (depart:Gare {nom: ligne.depart})
MATCH (arrivee:Gare {nom: ligne.arrivee})
CREATE (depart)-[:RELIE {ligne: ligne.ligne,
                         distance_km: toInteger(ligne.distance_km)}]->(arrivee);

// --- Verification ---
MATCH (g:Gare) RETURN count(g) AS gares;
MATCH ()-[r:RELIE]->() RETURN count(r) AS liaisons;
