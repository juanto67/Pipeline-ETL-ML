#!/usr/bin/env python3
import logging
import pandas as pd
from pathlib import Path
import os
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("clustering")
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
def league_id(df):
    df["league_id"] = df["league_name"].astype("category").cat.codes
    return df 
#Get avg for all of the statas for the las 5 matches for home and away team    
def clustering(df):
    col_home = ["home_score", "home_score_ht", "home_shots_on_target", "home_shots", "home_corners", "home_fouls", "home_yellow", "home_red"]
    df = df.sort_values(["season_code","Date","HomeTeam","division","league_name"])
    df["season_code"] = df["season_code"].astype(int)
    season = sorted(df["season_code"].unique())
    
    team_clusters_all = []
    for i,s in enumerate(season[1:],1):
        df_pasado = df[df["season_code"]<s]
        
        # Medias como local
        home_stats = df_pasado.groupby(["league_id", "HomeTeam"]).agg(
            avg_goals_scored=("home_score", "mean"),
            avg_goals_conceded=("away_score", "mean"),
            avg_shots=("home_shots", "mean"),
            avg_shots_on_target=("home_shots_on_target", "mean"),
            avg_corners=("home_corners", "mean"),
            avg_fouls=("home_fouls", "mean"),
            avg_yellow=("home_yellow", "mean"),
            avg_red=("home_red", "mean"),
        ).reset_index().rename(columns={"HomeTeam": "team"})

        # Medias como visitante
        away_stats = df_pasado.groupby(["league_id", "AwayTeam"]).agg(
            avg_goals_scored=("away_score", "mean"),
            avg_goals_conceded=("home_score", "mean"),
            avg_shots=("away_shots", "mean"),
            avg_shots_on_target=("away_shots_on_target", "mean"),
            avg_corners=("away_corners", "mean"),
            avg_fouls=("away_fouls", "mean"),
            avg_yellow=("away_yellow", "mean"),
            avg_red=("away_red", "mean"),
        ).reset_index().rename(columns={"AwayTeam": "team"})
        # KMeans por liga separado
        # Promedias ambos correctamente
        all_stats = pd.concat([home_stats, away_stats])
        team_stats = all_stats.groupby(["league_id", "team"]).mean().reset_index()
        # KMeans por liga separado
        for league in team_stats["league_id"].unique():
            league_stats = team_stats[team_stats["league_id"] == league].copy()
            
            features = ["avg_goals_scored", "avg_goals_conceded", 
                        "avg_shots", "avg_corners", "avg_fouls", "avg_yellow", "avg_red"]
            
            X = league_stats[features]
            X_scaled = StandardScaler().fit_transform(X)
            
            kmeans = KMeans(n_clusters=3, random_state=42)
            league_stats["cluster"] = kmeans.fit_predict(X_scaled)
            league_stats["season_code"] = s
            
            team_clusters_all.append(league_stats[["league_id", "team", "cluster", "season_code"]])

    clusters_df = pd.concat(team_clusters_all)

    return clusters_df

#Main function to read data, transform it and save it
def __main__():
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "proc"
    output_folder = Path(__file__).resolve().parents[1] / "data" / "merge"
    os.makedirs(output_folder, exist_ok=True)
    #Read matches data from CSV
    df = pd.read_csv(entry_folder / "matches_proc.csv")
    if df.empty:
        logger.warning("No input files found in %s", entry_folder)
        return
    df = league_id(df)
    df = clustering(df)
    merge_and_save(df, output_folder / "matches_clustering.csv", dedupe_keys=["league_id", "team", "season_code"], sort_keys=["league_id", "team", "season_code"])
    logger.info("Loaded matches_proc.csv with %d rows", len(df))

if __name__ == "__main__":
    __main__()