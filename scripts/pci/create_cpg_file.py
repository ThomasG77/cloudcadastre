import os
import argparse

def create_cpg_files(root_directory):
    """
    Parcourt récursivement une arborescence de dossiers à partir de root_directory
    et crée un fichier .cpg pour chaque fichier .shp trouvé.
    Le fichier .cpg contiendra le texte '88591'.
    """
    print(f"Recherche dans {root_directory}...")
    
    # Compteurs pour les statistiques
    found_files = 0
    created_files = 0
    
    # Parcourir récursivement l'arborescence
    for dirpath, dirnames, filenames in os.walk(root_directory):
        # Filtrer les fichiers avec l'extension .shp
        shp_files = [f for f in filenames if f.lower().endswith('.shp')]
        
        # Pour chaque fichier .shp trouvé
        for shp_file in shp_files:
            found_files += 1
            base_name = os.path.splitext(shp_file)[0]
            cpg_file = base_name + '.cpg'
            cpg_path = os.path.join(dirpath, cpg_file)
            
            # Vérifier si le fichier .cpg existe déjà
            if os.path.exists(cpg_path):
                print(f"Le fichier {cpg_path} existe déjà, il ne sera pas modifié.")
                continue
            
            # Créer le fichier .cpg
            try:
                with open(cpg_path, 'w') as f:
                    f.write('88591')
                created_files += 1
                print(f"Fichier créé: {cpg_path}")
            except Exception as e:
                print(f"Erreur lors de la création de {cpg_path}: {e}")
    
    # Afficher les statistiques
    print(f"\nRécapitulatif:")
    print(f"- Fichiers .shp trouvés: {found_files}")
    print(f"- Fichiers .cpg créés: {created_files}")

if __name__ == "__main__":
    # Configuration du parser d'arguments
    parser = argparse.ArgumentParser(description='Crée des fichiers .cpg pour chaque fichier .shp trouvé')
    parser.add_argument('--input', required=True, help='Chemin du dossier racine à analyser')
    
    # Récupération des arguments
    args = parser.parse_args()
    
    # Vérifier que le chemin existe
    if os.path.isdir(args.input):
        create_cpg_files(args.input)
    else:
        print(f"Le chemin '{args.input}' n'est pas un dossier valide.")