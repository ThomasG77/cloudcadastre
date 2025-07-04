Liste des fichiers :

1. telechargement.py, script de téléchargement depuis les dépôts etalab
	* se base sur url_sources_departements.tsv pour les URL sources
2. unzip_agglist.py, extrait le contenu de chaque fichier zip et crée des fichiers avec tous les chemins
3. create_cpg_file.py, script créant un fichier auxiliaire *.cpg pour forcer la reconnaissance de l'encodage utf8 des *.shp
	* les étapes 1, 2 et 3 pourraient sauter en corrigeant la source, l'étape 4 pourrait directement consommer les *.shp.zip avec le pilote gdal vsizip
4. convert_shp_to_parquet.py, conversion de chaque shp en fichiers Parquet pour accélérer le parcours lors de l'importation étape 5
	* permet de supprimer les fichiers des étapes 1 et 2 pour limiter l'espace disque utilisé
5. duckdb_convert_pci.sql, importation dans une base DuckDB de tous les fichiers parquet
	* ajout de colonnes
	* première phase de tri
6. duckdb_export_pci.sql, exportation par lots de départements puis fusion en seul fichier parquet
	* les exports individuels permettent de faire des ORDER BY sans erreurs OOM dans duckdb