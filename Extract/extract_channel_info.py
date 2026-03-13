"""
extract_channel_info.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Extrait les features d'une chaîne YouTube à partir de son channel_id.
Utilisé pour enrichir le dataset de viralité des Shorts.
 
Dépendances : yt-dlp, pandas
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
 
import yt_dlp
import re
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
 
 
# ── Fetch brut des infos chaîne ───────────────────────────────────────────
 
def fetch_channel_info(channel_id: str) -> dict | None:
    """
    Utilise la playlist UUSH (déjà validée) au lieu de l'onglet /shorts
    qui retourne 404 avec yt-dlp.
    """
    if not channel_id or not str(channel_id).strip():
        print(f"   ⚠️  ID de chaîne vide ou invalide ignoré")
        return None

    if not channel_id.startswith("UC"):
        print(f"   ⚠️  channel_id invalide (doit commencer par UC) : {channel_id}")
        return None
 
    playlist_url = f"https://www.youtube.com/channel/{channel_id}/shorts"
 
    ydl_opts = {
        "quiet":        True,
        "no_warnings":  True,
        "extract_flat": True,
        "playlistend":  30,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            return ydl.extract_info(playlist_url, download=False)
    except Exception as e:
        print(f"   ⚠️  channel {channel_id} : {e}")
        return None
 
 
# ── Parse des features chaîne ─────────────────────────────────────────────
 
def parse_channel_info(channel_id: str, info: dict) -> dict:
    """
    Transforme le dict brut yt-dlp en features chaîne pour le feature engineering.
 
    Features extraites :
    ─────────────────────────────────────────────────────────────
    IDENTITÉ
        channel_id, channel_name
 
    TAILLE & MATURITÉ
        subscriber_count        → taille de l'audience
        creator_tier            → nano / micro / mid / macro
        total_videos            → volume de contenu produit
 
    ACTIVITÉ RÉCENTE (basée sur les 30 derniers Shorts)
        avg_views               → moyenne des vues sur les 30 derniers
        median_views            → médiane (résistante aux outliers)
        std_views               → écart-type (régularité des perf.)
        max_views               → pic de viralité atteint
        avg_like_rate           → taux de like moyen
        avg_duration            → durée moyenne des Shorts
 
    CONTENU
        avg_title_length        → longueur moyenne des titres
        avg_hook_count          → nb moyen de hook words dans les titres
        avg_emoji_count         → nb moyen d'emojis dans les titres
        pct_has_hashtag         → % de titres avec #hashtag
 
    SIGNAL VIRAL CHAÎNE
        viral_video_rate        → % de vidéos dépassant 1M vues
        views_per_subscriber    → ratio vues/abonnés moyen (outlier = viral)
        consistency_score       → 1 - (std/mean) normalisé → régularité
    ─────────────────────────────────────────────────────────────
    """
    entries = info.get("entries") or []
 
    # ── Identité ──────────────────────────────────────────────
    channel_name   = info.get("channel") or info.get("uploader") or ""
 
    # ── Taille & Maturité ─────────────────────────────────────
    subs = int(info.get("channel_follower_count") or 0)
 
    if subs == 0:             creator_tier = "unknown"
    elif subs < 10_000:       creator_tier = "nano"
    elif subs < 100_000:      creator_tier = "micro"
    elif subs < 1_000_000:    creator_tier = "mid"
    else:                     creator_tier = "macro"
 
    total_videos = int(info.get("playlist_count") or len(entries))
 
    # ── Stats sur les Shorts de l'échantillon ─────────────────
    views_list      = []
    like_rates      = []
    durations       = []
    title_lengths   = []
    hook_counts     = []
    emoji_counts    = []
    has_hashtags    = []
 
    for e in entries:
        if not e:
            continue
 
        v = int(e.get("view_count") or 0)
        l = int(e.get("like_count") or 0)
        d = int(e.get("duration")   or 0)
 
        views_list.append(v)
        like_rates.append(l / max(v, 1))
        durations.append(d)
 
        # Titre
        title = e.get("title") or ""
        title_lengths.append(len(title))
        hook_counts.append(_count_hooks(title))
        emoji_counts.append(_count_emojis(title))
        has_hashtags.append(1 if "#" in title else 0)
 
    n = len(views_list) or 1  # évite division par zéro
 
    # Métriques vues
    avg_views    = round(np.mean(views_list), 2)    if views_list else 0
    median_views = round(np.median(views_list), 2)  if views_list else 0
    std_views    = round(np.std(views_list), 2)      if views_list else 0
    max_views    = max(views_list)                   if views_list else 0
 
    # Viral rate : % de vidéos dépassant 1M de vues
    viral_video_rate = round(sum(1 for v in views_list if v >= 1_000_000) / n, 4)
 
    # Views per subscriber (moyen sur l'échantillon)
    views_per_sub = round(avg_views / max(subs, 1), 4)
 
    # Consistency score : plus proche de 1 = performances régulières
    # 0 = très irrégulier (une vidéo explose, les autres non)
    consistency_score = None
    if avg_views > 0:
        cv = std_views / avg_views                    # coefficient de variation
        consistency_score = round(1 / (1 + cv), 4)   # normalisé entre 0 et 1
 
    return {
        # ── Identité ─────────────────────────────────────────
        "channel_id":               channel_id,
        "channel_name":             channel_name,
 
        # ── Taille & Maturité ────────────────────────────────
        "subscriber_count":         subs,
        "creator_tier":             creator_tier,
        "total_videos":             total_videos,
 
        # ── Activité récente ─────────────────────────────────
        "shorts_count_sample":      n,
        "avg_views":                avg_views,
        "median_views":             median_views,
        "std_views":                std_views,
        "max_views":                max_views,
        "avg_like_rate":            round(np.mean(like_rates), 6) if like_rates else 0,
        "avg_duration_sec":         round(np.mean(durations), 2)  if durations else 0,
 
        # ── Contenu ──────────────────────────────────────────
        "avg_title_length":         round(np.mean(title_lengths), 2) if title_lengths else 0,
        "avg_hook_count":           round(np.mean(hook_counts), 4)   if hook_counts   else 0,
        "avg_emoji_count":          round(np.mean(emoji_counts), 4)  if emoji_counts  else 0,
        "pct_has_hashtag":          round(np.mean(has_hashtags), 4)  if has_hashtags  else 0,
 
        # ── Signal viral chaîne ──────────────────────────────
        "viral_video_rate":         viral_video_rate,
        "views_per_subscriber":     views_per_sub,
        "consistency_score":        consistency_score,
    }
 
 
# ── Helpers internes ──────────────────────────────────────────────────────
 
HOOK_WORDS = [
    "secret", "never", "shocking", "unbelievable", "insane", "mind blowing",
    "you won't believe", "wait for it", "wait", "pov", "how to", "hack",
    "trick", "tips", "tutorial", "life hack", "you need", "must see", "stop",
    "always", "never do", "viral", "trend", "trending", "watch this", "trying",
    "astuce", "incroyable", "jamais vu", "comment", "pourquoi", "arrête",
    "fou", "choc", "fail", "vrai", "facile", "rapide",
]
 
def _count_hooks(text: str) -> int:
    t = text.lower()
    return sum(1 for hw in HOOK_WORDS if hw in t)
 
def _count_emojis(text: str) -> int:
    # Compte les caractères emoji sans dépendance externe
    return sum(1 for c in text if ord(c) > 127462)
 
 
# ── Pipeline complet ──────────────────────────────────────────────────────
 
def extract_channel_info(channel_id: str) -> dict | None:
    """Point d'entrée principal : fetch + parse pour un channel_id."""
    info = fetch_channel_info(channel_id)
    if info is None:
        return None
    return parse_channel_info(channel_id, info)
 
 
def extract_all_channels(channel_ids: list[str], max_workers: int = 5) -> list[dict]:
    """
    Extrait les features de plusieurs chaînes en parallèle.
 
    Args:
        channel_ids : liste de channel_id (UCxxxxxxx)
        max_workers : nb de threads parallèles (≤5 recommandé)
 
    Returns:
        Liste de dicts de features, prête pour pd.DataFrame()
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(extract_channel_info, cid): cid
            for cid in channel_ids
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Chaînes"):
            result = future.result()
            if result is not None:
                results.append(result)
 
    print(f"✅ {len(results)} chaînes extraites ({len(channel_ids) - len(results)} échecs)")
    return results
 
 
# ── Enrichissement du DataFrame vidéos ───────────────────────────────────
 
def enrich_with_channel_features(df_videos, df_channels):
    """
    Joint les features chaîne au DataFrame vidéos sur channel_id.
 
    Les colonnes chaîne sont préfixées par 'ch_' pour éviter
    les collisions avec les features vidéo (ex: ch_avg_views vs avg_views).
 
    Usage :
        channel_ids = df_videos["channel_id"].unique().tolist()
        channel_records = extract_all_channels(channel_ids)
        df_channels = pd.DataFrame(channel_records).add_prefix("ch_").rename(
            columns={"ch_channel_id": "channel_id"}
        )
        df_enriched = enrich_with_channel_features(df_videos, df_channels)
    """
    import pandas as pd
    return df_videos.merge(df_channels, on="channel_id", how="left")
 
 
# ── Exemple d'utilisation ─────────────────────────────────────────────────
if __name__ == "__main__":
    import pandas as pd
    import os

    file_path = "./data/data.csv"
    if os.path.exists(file_path):
        df = pd.read_csv(file_path, sep="\t")
        if "channelId" in df.columns and "channel_id" not in df.columns:
            df = df.rename(columns={"channelId": "channel_id"})
            
        if "channel_id" in df.columns:
            channel_ids_raw = df["channel_id"].dropna().unique().tolist()
            channel_ids = [str(cid).strip() for cid in channel_ids_raw if str(cid).strip()]
            print(f"Trouvé {len(channel_ids)} chaînes uniques valides dans data.csv.")
        else:
            print("Erreur : la colonne 'channel_id' n'est pas dans data.csv.")
            channel_ids = []
    else:
        print(f"Fichier {file_path} introuvable, utilisation de chaînes d'exemple.")
        channel_ids = [
            "UCX6OQ3DkcsbYNE6H8uQQuVA",   # MrBeast
            "UCddiUEpeqJcYeBxX1IVBKvQ",   # Khaby Lame
        ]

    if channel_ids:
        # ── Extraction ────────────────────────────────────────────
        channel_records = extract_all_channels(channel_ids, max_workers=1)
        df_channels = pd.DataFrame(channel_records)
        df_channels.to_csv("./data/channel_data.csv", index=False)
        print("Fichier sauvegardé sous 'channel_data.csv'.")
    else:
        print("Aucune chaîne à extraire.")