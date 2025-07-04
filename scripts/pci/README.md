# CloudCadastre PCI Vecteur

## L'objectif

L'objectif est de fournir un accès simplifié aux données des parcelles, sections et bâtis du PCI vecteur en se connectant qu'à une seule source pour la totalité d'un millésime, tout en permettant la sélection d'un sous-ensemble sans aucun téléchargement préalable.

## Pourquoi ?

L'ouverture des données depuis 2022 sur cadastre.data.gouv.fr permet d'accéder à la donnée sans avoir à conventionner avec la DGFIP, cependant quelques irritants demeurent :
* les formats proposés sont limitants
	* le GeoJSON est un format texte prévu pour échanger un montant restreint de géométrie, pas des milliers, encore moins des millions; son système géographique est le WGS84, ce qui implique une perte de précision métrique pour les coordonnées.
	* le DXF est un format texte prévu pour le dessin, pas pour manipuler de la donnée
	* le MBTiles est un format prévu pour un affichage rapide, pas pour manipuler de la donnée
	* le Shapefile est un format dont [la liste des inconvénients](http://switchfromshapefile.org/) dépasse le raisonnable
* pour récupérer quelques communes, cela implique
	* de parcourir une arborescence touffue
	* de télécharger l'intégralité de fichiers lourds ou de fichiers archivés et compressés (eux-mêmes placés dans des fichiers archivés et compressés)

## Une solution

En partant des fichiers au format SHP, on bénéficie de la projection légale en RGF93/Lambert93 (EPSG:2154).

En publiant chaque millésime au format Parquet, tout [outil compatible](https://geoparquet.org/#implementations) pourra s'y connecter, filtrer et sélectionner des éléments sans avoir télécharger l'ensemble d'un département ou une palanquée de feuilles communales.

En reprennant les fichiers issus des extractions d'Etalab, les utilisateurs auront les mêmes structures et les mêmes problèmes, pas de suprise.

## Ce qui serait hors-sujet

Ce jeu de données n'est pas fait pour de l'affichage cartographique mais pour une extraction par code administratif (code commune ou de département) ou une extraction selon une emprise personnalisée.

Il ne contient pas toutes les informations contenues dans les fichiers [EDIGEO](https://fr.wikipedia.org/wiki/EDIGEO), il faudrait modifier le parseur d'etalab et retraiter l'ensemble pour exposer ça en Parquet (!= mon temps disponible). Si ça n'est pas dans les fichiers PCI retravaillés par Etalab, ça ne sera pas présent non plus (exit Strasbourg).

