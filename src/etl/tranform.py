#!/usr/bin/env python3
import logging
import pandas as pd
from pathlib import Path
import os
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("transform")
     
def stats_team(df):
    col_home = ["home_score", "home_score_ht", "home_shots_on_target", "home_shots", "home_corners", "home_fouls", "home_yellow", "home_red"]
    df = df.sort_values(["HomeTeam", "Date"])
    for c in col_home:
        df["avg_"+c+"_5"] = df.groupby("HomeTeam")[c].shift(1).rolling(window=5, min_periods=1).mean()
    
    col_away = ["away_score", "away_score_ht", "away_shots_on_target", "away_shots", "away_corners", "away_fouls", "away_yellow", "away_red"]    
    df = df.sort_values(["AwayTeam", "Date"])
    for c in col_away: 
        df["avg_"+c+"_5"] = df.groupby("AwayTeam")[c].shift(1).rolling(window=5, min_periods=1).mean()
    
    
   # home_stats = home_stats.rename(columns={"AwayTeam": "team"})
   # away_stats = away_stats.rename(columns={"AwayTeam": "team"})
    #team_stats = pd.merge(home_stats, away_stats, on=["team", "season_code", "league_name"], how="outer").fillna(0)
    return df
def __main__():
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    output_folder = Path(__file__).resolve().parents[1] / "data" / "proc"
    os.makedirs(output_folder, exist_ok=True)
    #Read matches data from CSV
    df_liga = pd.read_csv(entry_folder / "matches_liga.csv")
    df_premier = pd.read_csv(entry_folder / "matches_premier.csv")
    df_france = pd.read_csv(entry_folder / "matches_france.csv")    
        
    if df_liga.empty or df_premier.empty or df_france.empty:
        logger.warning("No matches data found in some CSV")
        return
    logger.info("Loaded matches data with %d rows, %d rows, %d rows", len(df_liga), len(df_premier), len(df_france))

    colums= ["result_ht","result"]

    for col in colums:
        if col in df_premier.columns:
            df_premier[col] = (
                df_premier[col]
                .replace({"H": 0, "D": 1, "A": 2})
                .infer_objects(copy=False)
            )
        if col in df_liga.columns:
            df_liga[col] = (
                df_liga[col]
                .replace({"H": 0, "D": 1, "A": 2})
                .infer_objects(copy=False)
            )
        if col in df_france.columns:
            df_france[col] = (
                df_france[col]
                .replace({"H": 0, "D": 1, "A": 2})
                .infer_objects(copy=False)
            )
    df_liga = stats_team(df_liga)
    df_liga.to_csv(output_folder / "matches_liga.csv", index=False)      
if __name__ == "__main__":
    __main__()