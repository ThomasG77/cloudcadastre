@echo off


SET WORK_PATH=D:\Users\jrmorreale\Documents\SIG\DGFIP\cloudcadastre\
SET SCRIPT_PATH=%WORK_PATH%\scripts
SET DATADUMP_PATH=%WORK_PATH%\shp
SET DATASAVE_PATH=%WORK_PATH%\donnees

SET DUCKDB_PATH=D:\Users\jrmorreale\Documents\Applications\

:: téléchargement
python %SCRIPT_PATH%\telechargement.py --workers 4 --format shp --resume --tsv %WORK_PATH%\url_sources_departements.tsv --output %DATADUMP_PATH%
wget -r -np -c -N --no-check-certificate -e robots=off -P %WORK_PATH% https://cadastre.data.gouv.fr/data/etalab-cadastre/2025-04-01/shp/departements/

:: unzip_agglist
python %SCRIPT_PATH%\unzip_agglist.py --quiet --processes 8 --input %DATADUMP_PATH% --output %DATADUMP_PATH%

:: create cpg
python %SCRIPT_PATH%\create_cpg_file.py --input %DATADUMP_PATH%\departements

:: convert to parquet
python %SCRIPT_PATH%\convert_shp_to_parquet.py --workers 8 --overwrite --root %WORK_PATH%

:: Import into duckdb
%DUCKDB_PATH%\duckdb.exe -f %SCRIPT_PATH%\duckdb_convert_pci.sql %DATASAVE_PATH%\cloudcadastre.duckdb 

:: export to Parquet (monolithic)
%DUCKDB_PATH%\duckdb.exe -f %SCRIPT_PATH%\duckdb_export_pci.sql %DATASAVE_PATH%\cloudcadastre.duckdb