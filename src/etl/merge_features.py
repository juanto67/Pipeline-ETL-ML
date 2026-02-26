#!/usr/bin/env python3
import logging
import pandas as pd
from pathlib import Path
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("merge_features")

#Saving into CSV and drop duplicates and sorting
def merge_and_save(df, filename, dedupe_keys, sort_keys):
    if filename.exists():
        existing = pd.read_csv(filename)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df
    combined = combined.drop_duplicates(subset=dedupe_keys, keep="last").reset_index(drop=True)
    combined = combined.sort_values(sort_keys).reset_index(drop=True)
    colums= ["result_ht","result"]

    for col in colums:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce").astype("Int64")
    combined.to_csv(filename, index=False)
    logger.info("Saved %s rows to %s", len(combined), filename) 

def __main__():
    alter_entry_folder = Path(__file__).resolve().parents[1] / "data" / "proc"
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "merge"
    output_folder = Path(__file__).resolve().parents[1] / "data" / "final"
    os.makedirs(output_folder, exist_ok=True)
    
    input_files = {
        "matches_proc.csv": alter_entry_folder / "matches_proc.csv",
        "matches_elo.csv": entry_folder / "matches_elo.csv",
        "matches_features.csv": entry_folder / "matches_features.csv",
        "matches_clustering.csv": entry_folder / "matches_clustering.csv",
    }
    
    dfs = {}
    for name, path in input_files.items():
        if path.exists():
            dfs[name] = pd.read_csv(path)
            logger.info("Loaded %s with %d rows", name, len(dfs[name]))
        else:
            logger.warning("File %s not found, skipping", name)
    
    if not dfs:
        logger.error("No input files found, exiting")
        return
    df = dfs.get("matches_proc.csv")
    df = df.drop(columns=["home_score", "home_score_ht", "home_shots_on_target", "home_shots", "home_corners", "home_fouls", "home_yellow", "home_red","away_score", "away_score_ht", "away_shots_on_target", "away_shots", "away_corners", "away_fouls", "away_yellow", "away_red"], errors="ignore")    

    # Añadir league_id igual que en clustering
    if "league_name" in df.columns:
        df["league_id"] = df["league_name"].astype("category").cat.codes

    df_clustering = dfs.get("matches_clustering.csv")

    # Merge para equipo local
    df = df.merge(df_clustering, 
                left_on=['league_id', 'HomeTeam', 'season_code'],
                right_on=['league_id', 'team', 'season_code'],
                how='left').rename(columns={'cluster': 'home_cluster'}).drop(columns='team')

    # Merge para equipo visitante
    df = df.merge(df_clustering,
                left_on=['league_id', 'AwayTeam', 'season_code'],
                right_on=['league_id', 'team', 'season_code'],
                how='left').rename(columns={'cluster': 'away_cluster'}).drop(columns='team')
    df["home_cluster"] = df["home_cluster"].fillna(1).astype(int)
    df["away_cluster"] = df["away_cluster"].fillna(1).astype(int)

    df_elo = dfs.get("matches_elo.csv")
    df = df.merge(df_elo,
                on=['season_code', 'division', 'league_name', 'Date', 'HomeTeam', 'AwayTeam','result','result_ht'],how='left')
    df = df.merge(dfs.get("matches_features.csv"), 
                on=['season_code', 'division', 'league_name', 'Date', 'HomeTeam', 'AwayTeam','result','result_ht'], how='left')
    
    df=df.drop(columns=["result_x","result_ht_x","result_y","result_ht_y"], errors="ignore")

    merge_and_save(df, output_folder / "matches_final.csv", dedupe_keys=["season_code", "division", "league_name", "Date", "HomeTeam", "AwayTeam"], sort_keys=["season_code", "division", "league_name", "Date", "HomeTeam", "AwayTeam"])
    logger.info("Loaded matches_final.csv with %d rows", len(df))

if __name__ == "__main__":
    __main__()