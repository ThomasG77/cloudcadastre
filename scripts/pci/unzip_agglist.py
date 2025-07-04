import os
import sys
import zipfile
import argparse
from datetime import datetime
import re
from collections import defaultdict
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

def extract_single_zip(zip_path, verbose=True):
    """Décompresse un fichier ZIP."""
    try:
        if verbose:
            print(f"Décompression de {zip_path}...")
        root = os.path.dirname(zip_path)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(root)
        return zip_path, True
    except Exception as e:
        if verbose:
            print(f"Erreur lors de la décompression de {zip_path}: {e}")
        return zip_path, False

def extract_zip_files(directory, num_processes=None, verbose=True):
    """Parcourt le répertoire et décompresse tous les fichiers ZIP trouvés en parallèle."""
    if verbose:
        print(f"Recherche de fichiers ZIP dans {directory}...")
    
    zip_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.zip'):
                zip_files.append(os.path.join(root, file))
    
    if not zip_files:
        if verbose:
            print("Aucun fichier ZIP trouvé.")
        return 0
    
    if verbose:
        print(f"Trouvé {len(zip_files)} fichiers ZIP à décompresser.")
    
    # Utiliser le nombre de processus spécifié ou la moitié des processeurs disponibles par défaut
    if num_processes is None:
        num_processes = max(1, multiprocessing.cpu_count() // 2)
    
    if verbose:
        print(f"Décompression en parallèle avec {num_processes} processus...")
    
    extract_function = partial(extract_single_zip, verbose=verbose)
    
    success_count = 0
    failure_count = 0
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        results = list(executor.map(extract_function, zip_files))
        
        for _, success in results:
            if success:
                success_count += 1
            else:
                failure_count += 1
    
    if verbose:
        print(f"Décompression terminée: {success_count} réussis, {failure_count} échecs sur {len(zip_files)} fichiers ZIP.")
    
    return success_count

def find_shp_files(directory):
    """Trouve tous les fichiers .shp dans le répertoire."""
    shp_files = []
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith('.shp'):
                shp_files.append(os.path.join(root, file))
    
    print(f"Trouvé {len(shp_files)} fichiers SHP.")
    return shp_files

def categorize_shp_files(shp_files):
    """Catégorise les fichiers SHP selon les critères spécifiés."""
    categories = [
        "batiments", "communes", "feuilles", "lieux_dits", 
        "parcelles", "prefixes_sections", "sections", "subdivisions_fiscales"
    ]
    
    # Structure pour stocker les fichiers par date et par catégorie
    by_date_category = defaultdict(lambda: defaultdict(list))
    
    for file_path in shp_files:
        filename = os.path.basename(file_path).lower()
        file_stats = os.stat(file_path)
        
        # Obtenir la date de création ou de modification du fichier
        try:
            creation_time = datetime.fromtimestamp(file_stats.st_ctime)
        except:
            # En cas d'erreur, utiliser la date de modification comme alternative
            creation_time = datetime.fromtimestamp(file_stats.st_mtime)
        
        date_key = f"{creation_time.year}_{creation_time.month:02d}"
        
        # Vérifier si le nom du fichier contient l'une des catégories
        file_categorized = False
        for category in categories:
            if category in filename:
                by_date_category[date_key][category].append(file_path)
                file_categorized = True
                break
    
    return by_date_category

def write_lists_to_files(by_date_category, output_dir):
    """Écrit les listes de fichiers dans des fichiers texte, classés par date et catégorie."""
    os.makedirs(output_dir, exist_ok=True)
    files_created = 0
    
    # Écrire les fichiers par date et catégorie
    for date_key, categories in by_date_category.items():
        year, month = date_key.split('_')
        
        # Écrire un fichier pour chaque catégorie de cette date
        for category, files in categories.items():
            if files:
                output_path = os.path.join(output_dir, f"{year}_{month}_{category}.txt")
                with open(output_path, 'w', encoding='utf-8') as f:
                    for file_path in files:
                        f.write(f"{file_path}\n")
                files_created += 1
                print(f"Créé {output_path} avec {len(files)} fichiers")
    
    return files_created

def main():
    parser = argparse.ArgumentParser(description='Traitement de fichiers ZIP et SHP')
    parser.add_argument('--input', required=True, help='Répertoire à parcourir pour trouver les fichiers ZIP et SHP')
    parser.add_argument('--output', required=True, help='Répertoire où écrire les listes de fichiers')
    parser.add_argument('--processes', type=int, default=None, 
                        help='Nombre de processus pour la décompression parallèle (par défaut: moitié des CPU disponibles)')
    parser.add_argument('--quiet', action='store_true', help='Réduire les messages de progression')
    
    args = parser.parse_args()
    verbose = not args.quiet
    
    if not os.path.isdir(args.input):
        print(f"Erreur: {args.input} n'est pas un répertoire valide.")
        return 1
    
    if verbose:
        print("=== Début du traitement ===")
    
    # Étape 1: Décompresser les fichiers ZIP avec parallélisation
    zip_count = extract_zip_files(args.input, num_processes=args.processes, verbose=verbose)
    
    # Étape 2: Trouver tous les fichiers SHP
    shp_files = find_shp_files(args.input)
    
    if not shp_files:
        print("Aucun fichier SHP trouvé. Fin du programme.")
        return 0
    
    # Étape 3: Catégoriser les fichiers SHP
    by_date_category = categorize_shp_files(shp_files)
    
    # Étape 4: Écrire les listes dans des fichiers (par date et catégorie uniquement)
    files_created = write_lists_to_files(by_date_category, args.output)
    
    if verbose:
        print(f"\n=== Traitement terminé ===")
        print(f"Fichiers ZIP traités: {zip_count}")
        print(f"Fichiers SHP trouvés: {len(shp_files)}")
        print(f"Listes créées: {files_created}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())