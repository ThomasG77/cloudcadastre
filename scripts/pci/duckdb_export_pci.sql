SET memory_limit = '16GB';
SET max_temp_directory_size = '125GB';

SET VARIABLE my_workspace = 'D:\Users\jrmorreale\Documents\SIG\DGFIP\cloudcadastre';
SET VARIABLE millesime = '2025-04-01'
SET file_search_path = getvariable('my_workspace');
SET file_search_path = 'D:\Users\jrmorreale\Documents\SIG\DGFIP\cloudcadastre';

LOAD spatial;

.timer on

-- export découpé en plusieurs fichier parquet
-- diviser par lots de départements
-- permet d'éviter des erreurs OOM

COPY (
	SELECT * FROM source_unique WHERE departement IN ('2A', '2B') 
	ORDER BY "departement", "commune", "type_objet"[:8], "section") 
	TO getvariable('my_workspace') || '\donnees\cloudcadastre_2A_2B.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int <= 10 
	ORDER BY "departement", "commune", "type_objet"[:8], "section") 
	TO getvariable('my_workspace') || '\donnees\cloudcadastre_01_10.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 10 AND "departement"::int <= 25 
	ORDER BY  "departement", "commune", "type_objet"[:8], "section") 
	TO getvariable('my_workspace') || '\donnees\cloudcadastre_11_25.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 25 AND "departement"::int <= 40 
	ORDER BY  "departement", "commune", "type_objet"[:8], "section") 
	TO getvariable('my_workspace') || '\donnees\cloudcadastre_26_40.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 40 AND "departement"::int <= 55 
	ORDER BY  "departement", "commune", "type_objet"[:8], "section") 
	TO getvariable('my_workspace') || '\donnees\cloudcadastre_41_55.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 55 AND "departement"::int <= 70 
	ORDER BY  "departement", "commune", "type_objet"[:8], "section") 
	TO getvariable('my_workspace') || '\donnees\cloudcadastre_56_70.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 70 AND "departement"::int <= 85 
	ORDER BY  "departement", "commune", "type_objet"[:8], "section") 
	TO getvariable('my_workspace') || '\donnees\cloudcadastre_71_85.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 85 AND "departement"::int <= 95 
	ORDER BY  "departement", "commune", "type_objet"[:8], "section") 
	TO getvariable('my_workspace') || '\donnees\cloudcadastre_86_95.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 95 
	ORDER BY  "departement", "commune", "type_objet"[:8], "section") 
	TO getvariable('my_workspace') || '\donnees\cloudcadastre_971_976.parquet' (FORMAT parquet, COMPRESSION zstd);

-- regroupement de tous les fichiers parquet
COPY (
	SELECT * FROM read_parquet(getvariable('my_workspace') || '\donnees\cloudcadastre_*.parquet')) 
	TO getvariable('my_workspace') || '\donnees\cloudcadastrefusion.parquet' (FORMAT parquet, COMPRESSION zstd);

-- export pour la commune de Lille Lomme Hellemmes
COPY (
	SELECT * FROM read_parquet(getvariable('my_workspace') || '\donnees\cloudcadastrefusion.parquet') 
	WHERE commune = '59350') 
	TO getvariable('my_workspace') || '\donnees\cloudcadastrefusion_lille.parquet' (FORMAT parquet, COMPRESSION zstd);

.exit

/*
a faire après évaluation :
	- ajouter le tri par section
	- remplir pour les type_objet concernés les colonnes section et parcelle pour permettre la récupération sans requêtes spatiales
*/