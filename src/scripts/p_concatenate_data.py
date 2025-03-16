#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import glob
import os
from pathlib import Path

# Répertoire où se trouvent les fichiers bruts
input_dir = "data/raw"

# Récupérer tous les fichiers correspondant au motif DF_*_Raw_*.csv dans data/raw/
file_list = glob.glob(f"{input_dir}/DF_*_Raw_*.csv")

# Vérifier s'il y a des fichiers, sinon lever une erreur
if not file_list:
    raise FileNotFoundError("Aucun fichier brut trouvé dans data/raw/")

# Trier les fichiers par date de modification (du plus ancien au plus récent)
# Utiliser Path pour obtenir les métadonnées des fichiers
sorted_files = sorted(file_list, key=lambda x: Path(x).stat().st_mtime, reverse=True)

# Sélectionner uniquement les 3 derniers fichiers (les plus récents)
latest_files = sorted_files[:3]
print("Fichiers sélectionnés pour la concaténation :")
for file in latest_files:
    print(f" - {file}")

# Charger les 3 fichiers dans une liste de DataFrames
dfs = [pd.read_csv(file) for file in latest_files]

# Concaténer les DataFrames en un seul, ignorer les index existants et remplir les NaN avec 0
df_concat = pd.concat(dfs, axis=0, ignore_index=True).fillna(0)

# Définir le répertoire de sortie et s'assurer qu'il existe
output_dir = "data/raw"
os.makedirs(output_dir, exist_ok=True)

# Nommer le fichier de sortie avec toutes les années couvertes
output_filename = f"{output_dir}/DF_2021-23_Concat_Raw.csv"

# Sauvegarder le DataFrame concaténé dans un fichier CSV
df_concat.to_csv(output_filename, index=False)
print(f"Dataset concaténé enregistré : {output_filename}")