from datetime import datetime
import duckdb

my_workspace = '/srv/cloudcadastre'

output_parquet_chunks = [f'''
COPY (
	SELECT * FROM source_unique WHERE departement IN ('2A', '2B')
	ORDER BY "departement", "commune", "type_objet"[:8], "section")
	TO '{my_workspace}/cloudcadastre_2A_2B.parquet' (FORMAT parquet, COMPRESSION zstd);
''',
f'''
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int <= 10
	ORDER BY "departement", "commune", "type_objet"[:8], "section")
	TO '{my_workspace}/cloudcadastre_01_10.parquet' (FORMAT parquet, COMPRESSION zstd);
''',
f'''
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 10 AND "departement"::int <= 25
	ORDER BY  "departement", "commune", "type_objet"[:8], "section")
	TO '{my_workspace}/cloudcadastre_11_25.parquet' (FORMAT parquet, COMPRESSION zstd);
''',
f'''
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 25 AND "departement"::int <= 40
	ORDER BY  "departement", "commune", "type_objet"[:8], "section")
	TO '{my_workspace}/cloudcadastre_26_40.parquet' (FORMAT parquet, COMPRESSION zstd);
''',
f'''
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 40 AND "departement"::int <= 55
	ORDER BY  "departement", "commune", "type_objet"[:8], "section")
	TO '{my_workspace}/cloudcadastre_41_55.parquet' (FORMAT parquet, COMPRESSION zstd);
''',
f'''
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 55 AND "departement"::int <= 70
	ORDER BY  "departement", "commune", "type_objet"[:8], "section")
	TO '{my_workspace}/cloudcadastre_56_70.parquet' (FORMAT parquet, COMPRESSION zstd);
''',
f'''
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 70 AND "departement"::int <= 85
	ORDER BY  "departement", "commune", "type_objet"[:8], "section")
	TO '{my_workspace}/cloudcadastre_71_85.parquet' (FORMAT parquet, COMPRESSION zstd);
''',
f'''
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 85 AND "departement"::int <= 95
	ORDER BY  "departement", "commune", "type_objet"[:8], "section")
	TO '{my_workspace}/cloudcadastre_86_95.parquet' (FORMAT parquet, COMPRESSION zstd);
''',
f'''
COPY (
	SELECT * FROM source_unique WHERE departement NOT IN ('2A', '2B') AND "departement"::int > 95
	ORDER BY  "departement", "commune", "type_objet"[:8], "section")
	TO '{my_workspace}/cloudcadastre_971_976.parquet' (FORMAT parquet, COMPRESSION zstd);
''']

group_parquet_query = [f'''
-- regroupement de tous les fichiers parquet
COPY (
	SELECT * FROM read_parquet('{my_workspace}/cloudcadastre_*.parquet'))
	TO '{my_workspace}/cadastre.parquet' (FORMAT parquet, COMPRESSION zstd);
''']

sql_statements = output_parquet_chunks + group_parquet_query
#sql_statements = group_parquet_query

start_time = datetime.now()
with duckdb.connect("cadastre.duckdb") as con:
    con.sql("INSTALL SPATIAL;LOAD spatial;") # required extension

    for index, statement in enumerate(sql_statements):
        print(f'{index + 1}/{len(sql_statements)}', statement)
        intermediate_start_time = datetime.now()
        con.sql(statement)
        intermediate_end_time = datetime.now()
        print('Intermediate Duration: {}'.format(intermediate_end_time - intermediate_start_time))
    
end_time = datetime.now()
print('Full Duration: {}'.format(end_time - start_time))
