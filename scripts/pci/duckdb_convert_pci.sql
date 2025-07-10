-- importation des fichiers parquet par type et département dans une seule base duckdb
-- ajout de colonnes avec le millésime, un code de département et le type d'objet

SET memory_limit = '16GB';
SET max_temp_directory_size = '125GB';
SET VARIABLE my_workspace = 'D:\Users\jrmorreale\Documents\SIG\DGFIP\cloudcadastre';
SET VARIABLE millesime = '2025-04-01'
SET file_search_path = getvariable('my_workspace');

INSTALL spatial;
LOAD spatial;

.timer on

/*
import des fichiers Parquet
ajout des colonnes millesimes, departement, commune et type_objet
tri par département et commune

la valeur 97 permet de traiter spécifiquement les DROM-COM
*/

CREATE TABLE communes AS 
SELECT getvariable('millesime') AS "millesime", CASE WHEN left(id, 2)::VARCHAR != '97' THEN left(id, 2) WHEN left(id, 2)::VARCHAR = '97' THEN left(id, 3) END AS "departement", "id" AS "commune", 'communes' AS "type_objet", * 
FROM read_parquet(getvariable('my_workspace') || '\donnees\**\communes.parquet') 
ORDER BY "departement", "commune";

CREATE TABLE feuilles AS 
SELECT getvariable('millesime') AS "millesime", CASE WHEN left(commune, 2)::VARCHAR != '97' THEN left(commune, 2) WHEN left(commune, 2)::VARCHAR = '97' THEN left(commune, 3) END AS "departement", 'feuilles' AS "type_objet", * 
FROM read_parquet(getvariable('my_workspace') || '\donnees\**\feuilles.parquet') 
ORDER BY "departement", "commune";

CREATE TABLE lieux_dits AS 
SELECT getvariable('millesime') AS "millesime", CASE WHEN left(commune, 2)::VARCHAR != '97' THEN left(commune, 2) WHEN left(commune, 2)::VARCHAR = '97' THEN left(commune, 3) END AS "departement", 'lieux_dits' AS "type_objet", * 
FROM read_parquet(getvariable('my_workspace') || '\donnees\**\lieux_dits.parquet') 
ORDER BY "departement", "commune";

CREATE TABLE prefixes_sections AS 
SELECT getvariable('millesime') AS "millesime", CASE WHEN left(commune, 2)::VARCHAR != '97' THEN left(commune, 2) WHEN left(commune, 2)::VARCHAR = '97' THEN left(commune, 3) END AS "departement", 'prefixes_sections' AS "type_objet", * 
FROM read_parquet(getvariable('my_workspace') || '\donnees\**\prefixes_sections.parquet') 
ORDER BY "departement", "commune";

CREATE TABLE sections AS 
SELECT getvariable('millesime') AS "millesime", CASE WHEN left(commune, 2)::VARCHAR != '97' THEN left(commune, 2) WHEN left(commune, 2)::VARCHAR = '97' THEN left(commune, 3) END AS "departement", 'sections' AS "type_objet", * 
FROM read_parquet(getvariable('my_workspace') || '\donnees\**\sections.parquet') 
ORDER BY "departement", "commune";

CREATE TABLE subdivisions_fiscales AS
SELECT 
	getvariable('millesime') AS "millesime", 
	CASE WHEN left(parcelle, 2)::VARCHAR != '97' THEN left(parcelle, 2) WHEN left(parcelle, 2)::VARCHAR = '97' THEN left(parcelle, 3) END AS "departement", 
	left(parcelle, 5) AS "commune", 'subdivisions_fiscales' AS "type_objet",
	* 
FROM read_parquet(getvariable('my_workspace') || '\donnees\**\subdivisions_fiscales.parquet')
WHERE parcelle IS NOT NULL
ORDER BY "departement", "commune";

-- distingue les subdivisions non explicitement attachées à une parcelle
CREATE TABLE subdivisions_fiscales_sanscommune AS
SELECT 
	getvariable('millesime') AS "millesime", 
	'000' AS "departement", '00000' AS "commune", 
	'subdivisions_fiscales' AS "type_objet", * 
FROM read_parquet(getvariable('my_workspace') || '\donnees\**\subdivisions_fiscales.parquet')
WHERE parcelle IS NULL;

/*
-- attribution d'une commune par intersection pour les enregistrements sans attributs de localisation
-- désactivé pour être iso avec etatlab
UPDATE subdivisions_fiscales_sanscommune
SET commune = communes.commune, departement = communes.departement
FROM communes
WHERE 
	ST_Contains(communes.geometry,
	ST_PointOnSurface(subdivisions_fiscales_sanscommune.geometry));
	
VACUUM subdivisions_fiscales_sanscommune;
*/

CREATE TABLE batiments AS
SELECT 
	getvariable('millesime') AS "millesime",
	CASE 
		WHEN left(commune, 2)::VARCHAR != '97' THEN left(commune, 2) 
		WHEN left(commune, 2)::VARCHAR = '97' THEN left(commune, 3) 
		END AS "departement",
	'batiments' AS "type_objet",
	* 
FROM read_parquet(getvariable('my_workspace') || '\donnees\**\batiments.parquet');

CREATE TABLE parcelles AS
SELECT 
	getvariable('millesime') AS "millesime", 
	CASE 
		WHEN left(commune, 2)::VARCHAR != '97' THEN left(commune, 2) 
		WHEN left(commune, 2)::VARCHAR = '97' THEN left(commune, 3) 
		END AS "departement", 
	'parcelles' AS "type_objet", * 
FROM read_parquet(getvariable('my_workspace') || '\donnees\**\parcelles.parquet');

-- vue offrant un accès unifié à toutes les tables d'importation
CREATE OR REPLACE VIEW source_union AS
SELECT * FROM sections
	UNION ALL BY NAME
	SELECT * FROM subdivisions_fiscales
	UNION ALL BY NAME
	SELECT * FROM subdivisions_fiscales_sanscommune
	UNION ALL BY NAME
	SELECT * FROM communes
	UNION ALL BY NAME
	SELECT * FROM feuilles
	UNION ALL BY NAME
	SELECT * FROM lieux_dits
	UNION ALL BY NAME
	SELECT * FROM prefixes_sections
	UNION ALL BY NAME
	SELECT * FROM batiments
	UNION ALL BY NAME
	SELECT * FROM parcelles;

-- vue assignant un SRID EPSG en fonction du département
-- le SRID 0 est mis lorsque l'enregistrement n'a aucun attribut de localisation
CREATE OR REPLACE VIEW source_unique AS
SELECT 
	"millesime"::DATE AS "millesime",
	"departement",
	"commune",
	"type_objet",
	"id",
	"section",
	"parcelle",
	"numero",
	"prefixe",
	"code",
	"lettre",
	"nom",
	"created",
	"updated",
	"qualite",
	"modeConfec",
	"echelle",
	"ancienne",
	"type",
	"contenance",
	"geometry",
	"geometry_bbox",
	CASE 
		WHEN "departement" NOT IN ('971', '972', '973', '974', '976', '00') THEN 2154
		WHEN "departement" IN ('971', '972') THEN 5490
		WHEN "departement" = '973' THEN 2972
		WHEN "departement" = '974' THEN 2975
		WHEN "departement" = '976' THEN 4471
		WHEN "departement" = '000' THEN 0
	END AS geom_srid
FROM source_union;

/*
gestion des SRIDs
metro + corse = EPSG:2154
971, 972	RGAF09 / UTM zone 20N	5490
973	RGFG95 / UTM zone 22N	2972
974	RGR92 / UTM zone 40S	2975
976	RGM04 / UTM zone 38S	4471
*/

.exit