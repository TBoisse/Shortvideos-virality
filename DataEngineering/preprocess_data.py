import pandas as pd
import numpy as np
import re
import os

# Define robust paths dynamically
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw_data.csv")
CLEAN_DATA_PATH = os.path.join(BASE_DIR, "data", "clean_data.csv")

def parse_iso8601_duration(duration_str):
    """
    Parses ISO 8601 durations like 'PT1M4S' into total seconds.
    If 'duration_str' is invalid or nan, returns NaN.
    """
    if pd.isna(duration_str) or not isinstance(duration_str, str):
        return np.nan
    
    # Matches patterns like 'PT' followed by digits and H, M, or S
    # E.g., PT1H2M30S
    pattern = re.compile(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$')
    match = pattern.match(duration_str)
    
    if match:
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0
        return hours * 3600 + minutes * 60 + seconds
    return np.nan

def clean_data():
    if not os.path.exists(RAW_DATA_PATH):
        print(f"Erreur: Le fichier {RAW_DATA_PATH} n'existe pas.")
        return

    print(f"Chargement des données depuis {RAW_DATA_PATH}...")
    # Read raw data but force difficult columns to string first to avoid mixed-types warnings
    df = pd.read_csv(RAW_DATA_PATH, dtype={'comments': str, 'likes': str, 'views': str})
    
    initial_shape = df.shape
    print(f"Taille initiale: {initial_shape[0]} lignes, {initial_shape[1]} colonnes")

    # 1. Parsing publishedAt to pandas DateTime
    print("Conversion de 'publishedAt' en datetime...")
    if 'publishedAt' in df.columns:
        df['publishedAt'] = pd.to_datetime(df['publishedAt'], errors='coerce')

    # 2. Parsing duration (PT1M4S) to total seconds
    print("Conversion de 'duration' (ex: PT1M4S) en 'duration_sec' (numérique)...")
    if 'duration' in df.columns:
        df['duration_sec'] = df['duration'].apply(parse_iso8601_duration)
        # On peut supprimer l'ancienne colonne duration textuelle si on veut, ou la garder.
        # df = df.drop(columns=['duration'])

    # 3. Forcing engagement columns to numeric (handling any commas or corrupted text)
    print("Nettoyage des colonnes d'engagement (views, likes, comments)...")
    for col in ['views', 'likes', 'comments']:
        if col in df.columns:
            # Enlever les éventuelles virgules des milliers si elles se sont glissées
            df[col] = df[col].astype(str).str.replace(',', '', regex=False)
            # Forcer la conversion en numérique
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    # Remplacer uniquement les descriptions vides par des chaînes de caractères vides
    if 'description' in df.columns:
        df['description'] = df['description'].fillna("NO_DESCRIPTION")
        
    # Imputation: remplacer likes/comments NaN par LE TAUX de la chaîne
    print("Imputation des likes/comments manquants par le taux de la chaîne...")
    
    channel_col = 'channel_id' if 'channel_id' in df.columns else ('channelId' if 'channelId' in df.columns else None)
    
    if channel_col and 'views' in df.columns:
        # Éviter les divisions par zéro
        safe_views = df['views'].clip(lower=1)
        
        for metric in ['likes', 'comments']:
            if metric in df.columns:
                # 1. Calcul du Taux d'engagement de la vidéo (likes/vues)
                rate_col = f'{metric}_rate'
                df[rate_col] = df[metric] / safe_views
                
                # 2. Moyenne du Taux pour la chaîne entière
                channel_rate = df.groupby(channel_col)[rate_col].transform('mean')
                
                # 3. L'imputation exacte sera : Vues de la vidéo actuelle * Taux de la chaîne
                imputed_values = df['views'] * channel_rate
                
                # 4. Remplacer les valeurs NaN originelles
                df[metric] = df[metric].fillna(imputed_values)
                
                # Supprimer la colonne taux temporaire
                df = df.drop(columns=[rate_col])
    else:
        print("Impossible d'imputer: colonne channel_id ou views absente.")

    # 4. Suppression des lignes sans identifiant de chaîne
    print("Suppression des vidéos sans channel_id...")
    if 'channel_id' in df.columns:
        df = df.dropna(subset=['channel_id'])
    elif 'channelId' in df.columns:
        df = df.dropna(subset=['channelId'])
        
    # 5. Suppression des lignes où likes ou comments sont toujours NaN après l'imputation
    print("Suppression des vidéos dont les likes ou comments sont encore inconnus...")
    cols_to_drop_nan = [c for c in ['likes', 'comments'] if c in df.columns]
    if cols_to_drop_nan:
        df = df.dropna(subset=cols_to_drop_nan)
        
    # 6. Suppression des doublons (basé sur l'identifiant de la vidéo 'id')
    print("Suppression des vidéos en doublon...")
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'], keep='first')
        
    final_shape = df.shape
    print(f"Taille finale: {final_shape[0]} lignes, {final_shape[1]} colonnes ({- (initial_shape[0] - final_shape[0])} lignes supprimées au total)")

    # 7. Création de la variable cible (Target pour le Machine Learning)
    print("Calcul du score de viralité (Échelle Min-Max 0-100)...")
    if set(['views', 'likes', 'comments']).issubset(df.columns):
        # 1. Calcul du score brut (Taux d'engagement pur)
        raw_score = (df['comments'] + df['likes']) / (df['views'].clip(lower=1))
        
        # 2. Filtrage de robustesse : Un score sur 10 vues n'a pas de valeur statistique.
        # On ne calcule la Target que pour les vidéos de plus de 500 vues
        mask_valid = df['views'] >= 500
        
        # 3. Échelle Min-Max (0 à 100) pour préserver la distribution naturelle
        # La pire vidéo vaudra 0, la meilleure absolue vaudra 100, les autres gardent l'échelle
        min_score = raw_score[mask_valid].min()
        max_score = raw_score[mask_valid].max()
        df.loc[mask_valid, 'virality_score'] = (raw_score[mask_valid] - min_score) / (max_score - min_score) * 100
        
        # 4. Suppression des vidéos non-pertinentes
        df = df.dropna(subset=['virality_score'])
        
    final_shape = df.shape
    print(f"Taille finale post-filtrage Target: {final_shape[0]} lignes, {final_shape[1]} colonnes")

    # Sauvegarder dans le nouveau fichier clean
    print(f"Sauvegarde des données nettoyées dans {CLEAN_DATA_PATH}...")
    df.to_csv(CLEAN_DATA_PATH, index=False)
    print("Nettoyage terminé avec succès !")

if __name__ == "__main__":
    clean_data()
