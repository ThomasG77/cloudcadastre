import os
import glob
import argparse
import concurrent.futures
import subprocess
from pathlib import Path
import zipfile


def process_shapefile(shp_file, overwrite=False):
    """
    Traite un fichier shapefile en le convertissant au format PARQUET.
    
    Args:
        shp_file (str): Chemin complet vers le fichier .shp
        overwrite (bool): Si True, écrase les fichiers existants
    
    Returns:
        tuple: (succès (bool), nom du fichier (str), message (str))
    """
    # Extraction du chemin et du nom de fichier sans extension
    if 'shp.zip' in shp_file:
        zip = zipfile.ZipFile(shp_file)
        shps_within_zip = [i for i in zip.namelist() if i.endswith('.shp')]
        filename = os.path.splitext(os.path.basename(shps_within_zip[0]))[0]
        if len(shps_within_zip) == 1:
            path = os.path.dirname(shp_file)
            shp_file = '/vsizip/' + shp_file + '/' + shps_within_zip[0]
        else:
            message = f"Le fichier en entrée ne contient pas de shp ou plusieurs shp"
            print(message)
            return False, filename, message

    else:
        filename = os.path.splitext(os.path.basename(shp_file))[0]
        path = os.path.dirname(shp_file)
    output_file = os.path.join(path, f"{filename}.parquet")
    
    # Vérifier si le fichier de sortie existe déjà
    if os.path.exists(output_file) and not overwrite:
        message = f"Le fichier {output_file} existe déjà. Utilisez --overwrite pour l'écraser."
        print(message)
        return False, filename, message
    
    # Construction de la commande avec un ordre correct des arguments
    # Le fichier d'entrée doit être placé AVANT les options de sortie pour éviter les problèmes d'interprétation
    cmd = 'ogr2ogr '
    
    # Ajouter l'option pour écraser si nécessaire
    if overwrite:
        cmd += '-overwrite '
    
    # Ajouter l'ordre des arguments en plaçant le fichier de sortie en premier, puis les options, puis le fichier d'entrée
    cmd += f'-f PARQUET "{output_file}" "{shp_file}" -nln {filename} -dsco COMPRESSION=ZSTD'
    
    print(f"Traitement de {shp_file}")
    print(f"Exécution de la commande: {cmd}")
    
    # Exécution de la commande
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        message = f"Conversion réussie pour {filename}"
        print(message)
        return True, filename, message
    else:
        message = f"Erreur lors de la conversion de {filename}: {result.stderr}"
        print(f"Erreur lors de la conversion de {filename}")
        print(f"Erreur: {result.stderr}")
        return False, filename, message

def find_shapefiles(root_dir):
    """
    Recherche récursivement tous les fichiers .shp dans l'arborescence.
    
    Args:
        root_dir (str): Dossier racine à parcourir
    
    Returns:
        list: Liste des chemins complets vers les fichiers .shp trouvés
    """
    # Utilisation de glob avec le pattern ** pour la recherche récursive
    shp_files = glob.glob(os.path.join(root_dir, "**", "*.shp"), recursive=True) +  glob.glob(os.path.join(root_dir, "**", "*shp.zip"), recursive=True)
    
    # Alternative avec os.walk() si glob pose problème
    if not shp_files:
        shp_files = []
        for root, dirs, files in os.walk(root_dir):
            for file in files:
                if file.lower().endswith('.shp') or file.lower().endswith('.shp.zip'):
                    shp_files.append(os.path.join(root, file))
    
    return shp_files


def main():
    parser = argparse.ArgumentParser(description='Convertit des fichiers SHP en PARQUET en parallèle')
    parser.add_argument('--root', required=True, help='Dossier racine à parcourir')
    parser.add_argument('--workers', type=int, default=4, help='Nombre de processus parallèles')
    parser.add_argument('--overwrite', action='store_true', help='Écrase les fichiers .parquet existants')
    args = parser.parse_args()
    
    # S'assurer que le chemin existe
    if not os.path.isdir(args.root):
        print(f"Erreur: Le dossier '{args.root}' n'existe pas ou n'est pas accessible.")
        return
    
    # Recherche des fichiers .shp dans l'arborescence
    print(f"Recherche des fichiers .shp dans {args.root} et ses sous-dossiers...")
    shp_files = find_shapefiles(args.root)
    
    if not shp_files:
        print(f"Aucun fichier .shp trouvé dans {args.root} et ses sous-dossiers.")
        return
    
    print(f"Nombre de fichiers .shp trouvés: {len(shp_files)}")
    
    # Traitement parallèle des fichiers
    with concurrent.futures.ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_shapefile, shp_file, args.overwrite): shp_file for shp_file in shp_files}
        
        success_count = 0
        fail_count = 0
        skipped_count = 0
        results = []
        
        for future in concurrent.futures.as_completed(futures):
            success, filename, message = future.result()
            results.append((success, filename, message))
            
            if success:
                success_count += 1
            elif "existe déjà" in message:
                skipped_count += 1
            else:
                fail_count += 1
    
    print(f"\nConversion terminée:")
    print(f"- Succès: {success_count}")
    print(f"- Échecs: {fail_count}")
    print(f"- Ignorés (fichiers existants): {skipped_count}")
    
    # Afficher les fichiers qui ont échoué si nécessaire
    if fail_count > 0:
        print("\nDétail des fichiers en échec:")
        for success, filename, message in results:
            if not success and "existe déjà" not in message:
                print(f"- {filename}: {message}")


if __name__ == "__main__":
    main()
