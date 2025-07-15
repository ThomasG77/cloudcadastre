## Liste des fichiers :

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
6. duckdb_export_pci.py, exportation par lots de départements puis fusion en un seul fichier parquet
	* les exports individuels permettent de faire des ORDER BY sans erreurs OOM dans duckdb

 ## Environnement préalable sur Linux

```bash
curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"
bash Miniforge3-$(uname)-$(uname -m).sh
~/miniforge3/bin/conda config --set auto_activate_base false
~/miniforge3/bin/conda create -n geospatial python=3.12 -y
~/miniforge3/bin/conda init
conda activate geospatial
conda install py7zr gdal ipython ipdb -y
conda install -c conda-forge libgdal-arrow-parquet duckdb -y
curl https://install.duckdb.org | sh
export PATH='/root/.duckdb/cli/latest':$PATH
```

## Opération sur le serveur de production

```
# ssh my_alias
cd /srv/
git clone https://github.com/Jean-Roc/cloudcadastre.git
cd cloudcadastre/scripts/pci
python convert_shp_to_parquet.py --root /srv/cadastre/etalab-cadastre/etalab-cadastre/shp/departements/ --optionsgdal '-oo ENCODING=ISO8859-1 -makevalid'
duckdb < duckdb_convert_pci.sql cadastre.duckdb
python duckdb_export_pci.py
```
