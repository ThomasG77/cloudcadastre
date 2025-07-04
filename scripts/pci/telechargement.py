import os
import requests
import csv
import argparse
import json
from urllib.parse import urlparse, urljoin
from pathlib import Path
import sys
from tqdm import tqdm
import concurrent.futures
import threading
from bs4 import BeautifulSoup
import re

# Variable globale pour la barre de progression partagée entre les threads
progress_lock = threading.Lock()

# Fichier pour enregistrer les téléchargements réussis
DOWNLOAD_LOG_FILE = "downloads_completed.json"

def discover_downloaded_files(output_dir):
    """Reconstruit le fichier de log en scannant les fichiers existants dans le dossier de sortie"""
    print(f"Reconstruction du fichier de suivi à partir des fichiers existants dans {output_dir}...")
    
    downloaded_files = []
    
    # Parcourir tous les sous-répertoires du dossier de sortie
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            # Ignorer le fichier de log lui-même
            if file == DOWNLOAD_LOG_FILE:
                continue
                
            # Chemin complet du fichier
            file_path = os.path.join(root, file)
            
            # Taille du fichier (pour vérifier qu'il n'est pas vide)
            file_size = os.path.getsize(file_path)
            
            if file_size > 0:
                # Essayer de reconstruire l'URL à partir du chemin relatif
                # Note: Cette reconstruction est approximative et pourrait ne pas correspondre exactement à l'URL d'origine
                rel_path = os.path.relpath(file_path, output_dir)
                path_parts = rel_path.split(os.sep)
                
                # Le premier élément devrait être le nom de domaine
                if len(path_parts) > 1:
                    domain = path_parts[0]
                    # Reconstruire le chemin sans le nom de domaine
                    path = '/'.join(path_parts[1:])
                    
                    # Construire une URL approximative - utiliser HTTPS par défaut
                    # car on ne peut pas déterminer de façon fiable le protocole original
                    url = f"https://{domain}/{path}"
                    downloaded_files.append(url)
    
    print(f"Detected {len(downloaded_files)} previously downloaded files")
    return {"downloaded_urls": downloaded_files}

def load_download_log(output_dir, resume=False):
    """Charge la liste des fichiers déjà téléchargés"""
    log_path = os.path.join(output_dir, DOWNLOAD_LOG_FILE)
    
    # Si le fichier existe, essayer de le charger
    if os.path.exists(log_path):
        try:
            with open(log_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"Le fichier de log {log_path} est corrompu. Création d'un nouveau log.")
            if resume:
                # En mode reprise, reconstruire le log à partir des fichiers existants
                return discover_downloaded_files(output_dir)
            return {"downloaded_urls": []}
    else:
        # Si le fichier n'existe pas et qu'on est en mode reprise,
        # tenter de reconstruire le log à partir des fichiers existants
        if resume and os.path.exists(output_dir):
            return discover_downloaded_files(output_dir)
        return {"downloaded_urls": []}

def save_download_log(output_dir, url):
    """Ajoute une URL à la liste des téléchargements réussis"""
    log_path = os.path.join(output_dir, DOWNLOAD_LOG_FILE)
    
    with progress_lock:
        download_log = load_download_log(output_dir)
        if url not in download_log["downloaded_urls"]:
            download_log["downloaded_urls"].append(url)
            
        with open(log_path, 'w') as f:
            json.dump(download_log, f, indent=2)

def create_directory_structure(url, base_dir):
    """Crée la structure de répertoires basée sur l'URL"""
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.strip('/').split('/')
    
    # Ignorer le nom de fichier à la fin de l'URL s'il s'agit d'un fichier
    if '.' in path_parts[-1] and not path_parts[-1].endswith('/'):
        path_parts = path_parts[:-1]
    
    # Construire le chemin complet
    full_path = os.path.join(base_dir, parsed_url.netloc, *path_parts)
    
    # Créer les répertoires si nécessaire
    os.makedirs(full_path, exist_ok=True)
    
    return full_path

def is_directory_listing(url, content):
    """Détermine si la page est une liste de fichiers/dossiers"""
    # Vérifier si l'URL se termine par un slash
    if not url.endswith('/'):
        return False
    
    # Rechercher des éléments typiques d'une page d'index de dossier
    soup = BeautifulSoup(content, 'html.parser')
    
    # Vérifier s'il y a des liens qui semblent être des fichiers/dossiers
    links = soup.find_all('a')
    for link in links:
        href = link.get('href')
        # Ignorer les liens absolus ou les liens qui ne pointent pas vers des fichiers/dossiers
        if href and not href.startswith('http') and not href.startswith('#') and not href.startswith('javascript:'):
            if href.endswith('/') or '.' in href:
                return True
    
    return False

def extract_links(url, content):
    """Extrait les liens vers les fichiers et dossiers d'une page d'index"""
    soup = BeautifulSoup(content, 'html.parser')
    links = []
    
    for link in soup.find_all('a'):
        href = link.get('href')
        
        if href and not href.startswith('http') and not href.startswith('#') and not href.startswith('javascript:'):
            # Ignorer les liens vers les répertoires parents
            if href == '../' or href == './' or href == '/':
                continue
                
            # Construire l'URL complète en préservant le protocole d'origine
            full_url = urljoin(url, href)
            links.append(full_url)
    
    return links

def get_file_name_from_url(url):
    """Extrait le nom de fichier à partir de l'URL"""
    parsed_url = urlparse(url)
    path = parsed_url.path
    file_name = os.path.basename(path)
    return file_name

def is_file_already_downloaded(url, destination_folder):
    """Vérifie si un fichier correspondant à l'URL existe déjà dans le dossier de destination"""
    file_name = get_file_name_from_url(url)
    if not file_name:
        return False
        
    file_path = os.path.join(destination_folder, file_name)
    
    # Vérifier si le fichier existe et n'est pas vide
    return os.path.exists(file_path) and os.path.getsize(file_path) > 0

def download_file(url, destination_folder, pbar=None, resume=False, base_output_dir=None):
    """Télécharge un fichier depuis l'URL vers le dossier de destination"""
    try:
        # Obtenir le nom du fichier depuis l'URL
        file_name = os.path.basename(urlparse(url).path)
        if not file_name:  # Si l'URL se termine par un slash, c'est probablement un dossier
            return None
            
        file_path = os.path.join(destination_folder, file_name)
        
        # Vérifier si l'URL est déjà dans le log des téléchargements
        if resume and base_output_dir:
            download_log = load_download_log(base_output_dir, resume)
            if url in download_log["downloaded_urls"]:
                with progress_lock:
                    print(f"URL déjà téléchargée (d'après le log): {url}")
                if pbar:
                    pbar.update(1)
                return True
        
        # Vérifier si le fichier existe déjà
        if os.path.exists(file_path):
            # Vérifier que le fichier n'est pas vide
            if os.path.getsize(file_path) > 0:
                # En mode reprise, on considère que les fichiers existants sont déjà téléchargés
                if resume:
                    # Ajouter l'URL au log
                    if base_output_dir:
                        save_download_log(base_output_dir, url)
                    with progress_lock:
                        print(f"Le fichier {file_name} existe déjà dans {destination_folder}")
                    if pbar:
                        pbar.update(1)
                    return True
                else:
                    # En mode normal, on signale simplement que le fichier existe déjà
                    with progress_lock:
                        print(f"Le fichier {file_name} existe déjà dans {destination_folder}")
                    if pbar:
                        pbar.update(1)
                    return True
            else:
                # Le fichier existe mais est vide, on le supprime pour le retélécharger
                os.remove(file_path)
                with progress_lock:
                    print(f"Le fichier {file_name} existe mais est vide, retéléchargement...")
        
        # Effectuer la requête en respectant le protocole de l'URL
        response = requests.get(url, allow_redirects=True)
        response.raise_for_status()
        
        # Vérifier si la réponse est une page d'index de dossier
        if is_directory_listing(url, response.text):
            return None  # Ce n'est pas un fichier mais un dossier
        
        # Télécharger le fichier
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        # Vérifier que le fichier a bien été écrit et n'est pas vide
        if os.path.getsize(file_path) > 0:
            # Ajouter l'URL au log des téléchargements réussis
            if base_output_dir:
                save_download_log(base_output_dir, url)
            
            with progress_lock:
                print(f"Téléchargement réussi: {file_path}")
        else:
            with progress_lock:
                print(f"Erreur: Le fichier téléchargé {file_path} est vide")
            return False
        
        if pbar:
            pbar.update(1)
        
        return True
        
    except requests.exceptions.RequestException as e:
        with progress_lock:
            print(f"Erreur lors du téléchargement de {url}: {e}")
        if pbar:
            pbar.update(1)
        return False

def explore_directory(url, base_output_dir, files_to_download, visited_urls=None, resume=False):
    """Explore récursivement un répertoire et ajoute tous les fichiers à télécharger"""
    if visited_urls is None:
        visited_urls = set()
    
    if url in visited_urls:
        return
    
    visited_urls.add(url)
    
    try:
        with progress_lock:
            print(f"Exploration du répertoire: {url}")
        
        # Utiliser le protocole spécifié dans l'URL
        response = requests.get(url, allow_redirects=True)
        response.raise_for_status()
        
        # Vérifier si la réponse est une page d'index de dossier
        if is_directory_listing(url, response.text):
            # Créer le dossier correspondant
            destination_folder = create_directory_structure(url, base_output_dir)
            
            # Extraire les liens vers les fichiers et dossiers
            links = extract_links(url, response.text)
            
            # Explorer chaque lien
            for link in links:
                if link.endswith('/'):
                    # C'est un dossier, l'explorer récursivement
                    explore_directory(link, base_output_dir, files_to_download, visited_urls, resume)
                else:
                    # C'est un fichier, vérifier s'il est déjà téléchargé en mode reprise
                    dest_folder = create_directory_structure(url, base_output_dir)
                    
                    if resume:
                        # En mode reprise, vérifier si le fichier existe déjà
                        if is_file_already_downloaded(link, dest_folder):
                            # Ajouter au log sans ajouter à la liste des téléchargements
                            save_download_log(base_output_dir, link)
                            continue
                    
                    # Ajouter à la liste de téléchargement
                    files_to_download.append((link, dest_folder))
        else:
            # Ce n'est pas une page d'index, c'est peut-être un fichier
            destination_folder = create_directory_structure(os.path.dirname(url) + '/', base_output_dir)
            files_to_download.append((url, destination_folder))
    
    except requests.exceptions.RequestException as e:
        with progress_lock:
            print(f"Erreur lors de l'exploration de {url}: {e}")

def download_directory_recursive(url, output_dir, num_workers=1, resume=False):
    """Télécharge récursivement tous les fichiers d'un répertoire"""
    files_to_download = []
    
    # Explorer le répertoire pour trouver tous les fichiers
    explore_directory(url, output_dir, files_to_download, resume=resume)
    
    if not files_to_download:
        print(f"Aucun fichier trouvé à télécharger pour l'URL: {url}")
        return 0, 0
    
    success_count = 0
    failure_count = 0
    
    total_files = len(files_to_download)
    print(f"\nTéléchargement de {total_files} fichiers avec {num_workers} workers...")
    
    # Charger le log des téléchargements si on est en mode reprise
    if resume:
        download_log = load_download_log(output_dir, resume)
        print(f"Mode reprise activé: {len(download_log['downloaded_urls'])} fichiers déjà téléchargés selon le log")
    
    # Créer une barre de progression partagée
    with tqdm(total=total_files, desc="Téléchargements", unit="fichier") as pbar:
        # Télécharger les fichiers en parallèle si demandé
        if num_workers > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                # Soumettre toutes les tâches de téléchargement
                futures = [executor.submit(download_file, url, folder, pbar, resume, output_dir) 
                          for url, folder in files_to_download]
                
                # Attendre que tous les téléchargements soient terminés
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result is True:
                        success_count += 1
                    elif result is False:
                        failure_count += 1
        else:
            # Mode séquentiel
            for url, folder in files_to_download:
                result = download_file(url, folder, pbar, resume, output_dir)
                if result is True:
                    success_count += 1
                elif result is False:
                    failure_count += 1
    
    return success_count, failure_count

def download_from_tsv(tsv_file, output_dir, num_workers=1, format_filter=None, resume=False):
    """Traite le fichier TSV et télécharge les fichiers"""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    total_success_count = 0
    total_failure_count = 0
    filtered_count = 0
    skipped_count = 0
    
    # Déterminer le séparateur (TSV = tabulation)
    separator = '\t'
    
    try:
        # Collecter les tâches de téléchargement
        download_urls = []
        
        with open(tsv_file, 'r', encoding='utf-8') as f:
            # Vérifier si le fichier est vide
            first_line = f.readline().strip()
            if not first_line:
                print("Le fichier TSV est vide.")
                return
            
            # Revenir au début du fichier
            f.seek(0)
            
            # Lire le fichier TSV
            reader = csv.DictReader(f, delimiter=separator)
            
            if 'source' not in reader.fieldnames:
                print("La colonne 'source' est manquante dans le fichier TSV.")
                return
            
            # Vérifier si la colonne format existe si un filtre est demandé
            if format_filter and 'format' not in reader.fieldnames:
                print("Attention: La colonne 'format' est manquante dans le fichier TSV, le filtrage par format ne fonctionnera pas.")
                format_filter = None
            
            # Charger le log des téléchargements si on est en mode reprise
            download_log = load_download_log(output_dir, resume) if resume else {"downloaded_urls": []}
            
            # Pour chaque ligne du fichier
            for i, row in enumerate(reader, 1):
                url = row.get('source', '').strip()
                millesime = row.get('millesime', 'inconnu').strip()
                format_value = row.get('format', '').strip() if 'format' in row else ''
                
                if not url:
                    print(f"URL manquante à la ligne {i}")
                    total_failure_count += 1
                    continue
                
                # S'assurer que l'URL se termine par un slash pour les répertoires
                # Préserver le protocole original de l'URL
                if not url.endswith('/') and '.' not in os.path.basename(urlparse(url).path):
                    url = url + '/'
                
                # Filtrer par format si demandé
                if format_filter and format_value != format_filter:
                    filtered_count += 1
                    continue
                
                # En mode reprise, vérifier si l'URL a déjà été entièrement téléchargée
                if resume and url in download_log["downloaded_urls"]:
                    print(f"URL déjà traitée (selon le log): {url}")
                    skipped_count += 1
                    continue
                
                print(f"Préparation de l'URL {i}: {url} (millésime: {millesime}, format: {format_value})")
                download_urls.append(url)
        
        # Si aucune URL n'a été trouvée
        if not download_urls:
            if skipped_count > 0:
                print(f"Toutes les URL ont déjà été téléchargées. {skipped_count} entrées ont été ignorées.")
            elif filtered_count > 0:
                print(f"Aucune URL ne correspond au format '{format_filter}'. {filtered_count} entrées ont été filtrées.")
            else:
                print("Aucune URL valide n'a été trouvée dans le fichier TSV.")
            return
        
        total_urls = len(download_urls)
        print(f"\nTraitement de {total_urls} URL(s)...")
        if skipped_count > 0:
            print(f"{skipped_count} URL(s) déjà téléchargées ont été ignorées (mode reprise)")
        if filtered_count > 0:
            print(f"{filtered_count} entrées ont été ignorées car elles ne correspondent pas au format '{format_filter}'")
        
        # Traiter chaque URL
        for i, url in enumerate(download_urls, 1):
            print(f"\nTraitement de l'URL {i}/{total_urls}: {url}")
            success_count, failure_count = download_directory_recursive(url, output_dir, num_workers, resume)
            total_success_count += success_count
            total_failure_count += failure_count
            
            # En mode reprise, marquer l'URL comme traitée après téléchargement complet
            if resume:
                save_download_log(output_dir, url)
    
    except FileNotFoundError:
        print(f"Le fichier {tsv_file} n'a pas été trouvé.")
        return
    except Exception as e:
        print(f"Une erreur s'est produite lors du traitement du fichier TSV: {e}")
        return
    
    print(f"\nRésumé global des téléchargements:")
    print(f"- Réussis: {total_success_count}")
    print(f"- Échoués: {total_failure_count}")
    if resume and skipped_count > 0:
        print(f"- Ignorés (déjà téléchargés): {skipped_count}")
    if format_filter:
        print(f"- Filtrés (format différent de '{format_filter}'): {filtered_count}")

def list_available_formats(tsv_file):
    """Liste tous les formats disponibles dans le fichier TSV"""
    formats = set()
    separator = '\t'
    
    try:
        with open(tsv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=separator)
            
            if 'format' not in reader.fieldnames:
                print("La colonne 'format' est manquante dans le fichier TSV.")
                return formats
            
            for row in reader:
                format_value = row.get('format', '').strip()
                if format_value:
                    formats.add(format_value)
        
        return formats
    
    except FileNotFoundError:
        print(f"Le fichier {tsv_file} n'a pas été trouvé.")
        return formats
    except Exception as e:
        print(f"Une erreur s'est produite lors de la lecture des formats: {e}")
        return formats

def main():
    parser = argparse.ArgumentParser(description="Télécharge des données depuis un fichier TSV en conservant la structure hiérarchique.")
    parser.add_argument('--tsv', default='edigeo_departements.tsv', help='Chemin vers le fichier TSV (par défaut: edigeo_departements.tsv)')
    parser.add_argument('--output', default='./downloads', help='Répertoire de destination (par défaut: ./downloads)')
    parser.add_argument('--workers', type=int, default=1, help='Nombre de téléchargements parallèles (par défaut: 1)')
    parser.add_argument('--format', help='Filtre par format de données (utilise la colonne "format" du TSV)')
    parser.add_argument('--list-formats', action='store_true', help='Liste tous les formats disponibles dans le fichier TSV et quitte')
    parser.add_argument('--resume', action='store_true', help='Reprend les téléchargements précédemment interrompus')
    
    args = parser.parse_args()
    
    # Si l'option --list-formats est activée, afficher les formats disponibles et quitter
    if args.list_formats:
        print(f"Lecture des formats disponibles dans {args.tsv}...")
        formats = list_available_formats(args.tsv)
        if formats:
            print("\nFormats disponibles:")
            for fmt in sorted(formats):
                print(f"- {fmt}")
            print(f"\nUtilisez l'option --format pour sélectionner un format spécifique.")
        else:
            print("Aucun format n'a été trouvé ou le fichier ne contient pas de colonne 'format'.")
        return
    
    print(f"Fichier TSV: {args.tsv}")
    print(f"Répertoire de destination: {args.output}")
    print(f"Nombre de workers: {args.workers}")
    if args.format:
        print(f"Filtrage par format: {args.format}")
    if args.resume:
        print(f"Mode reprise activé: les téléchargements interrompus seront poursuivis")
        
        # Si le fichier de log n'existe pas, mais que le dossier de sortie existe
        log_path = os.path.join(args.output, DOWNLOAD_LOG_FILE)
        if not os.path.exists(log_path) and os.path.exists(args.output):
            # Reconstruire le log à partir des fichiers existants
            download_log = discover_downloaded_files(args.output)
            # Sauvegarder le log reconstruit
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            with open(log_path, 'w') as f:
                json.dump(download_log, f, indent=2)
            print(f"Fichier de suivi créé avec {len(download_log['downloaded_urls'])} entrées")
    
    download_from_tsv(args.tsv, args.output, args.workers, args.format, args.resume)

if __name__ == "__main__":
    main()