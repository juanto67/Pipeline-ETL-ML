#!/usr/bin/env python3
import logging
import pandas as pd
from pathlib import Path
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("feature_engineering")
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
#Get avg for all of the statas for the las 5 matches for home and away team    
def stats_team(df):
    col_home = ["home_score", "home_score_ht", "home_shots_on_target", "home_shots", "home_corners", "home_fouls", "home_yellow", "home_red"]
    df = df.sort_values(["season_code","Date","HomeTeam","division","league_name"])
    #Calculate avg of the 5 last matches for home and away teams, moving the current match 
    funcion = lambda s: s.shift(1).rolling(window=5, min_periods=1).mean()
    for c in col_home:
        df["avg_"+c+"_5"] = (
            df.groupby(["HomeTeam","division","league_name"])[c]
            .transform(funcion)
            .fillna(0)
            .round(3)
        )
        logger.info("Calculated avg of last 5 matches for home team column %s", c)
        
    col_away = ["away_score", "away_score_ht", "away_shots_on_target", "away_shots", "away_corners", "away_fouls", "away_yellow", "away_red"]    
    df = df.sort_values(["season_code","Date","AwayTeam","division","league_name"])
    for c in col_away: 
        df["avg_"+c+"_5"] = (
            df.groupby(["AwayTeam","division","league_name"])[c]
            .transform(funcion)
            .fillna(0)
            .round(3)
        )
        logger.info("Calculated avg of last 5 matches for away team column %s", c)
    df = df.drop(columns=["home_score", "home_score_ht", "home_shots_on_target", "home_shots", "home_corners", "home_fouls", "home_yellow", "home_red","away_score", "away_score_ht", "away_shots_on_target", "away_shots", "away_corners", "away_fouls", "away_yellow", "away_red"], errors="ignore")    
    return df

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
    df = stats_team(df)
    merge_and_save(df, output_folder / "matches_features.csv", dedupe_keys=["season_code", "division", "league_name", "Date", "HomeTeam", "AwayTeam"], sort_keys=["season_code", "division", "league_name", "Date", "HomeTeam", "AwayTeam"])
    logger.info("Loaded matches_proc.csv with %d rows", len(df))

if __name__ == "__main__":
    __main__()