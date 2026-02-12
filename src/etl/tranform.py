#!/usr/bin/env python3
import logging
import pandas as pd
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("transform")
     
def stats_team(df):
    col_home = ["home_score", "home_score_ht", "home_shots_on_target", "home_shots", "home_shots_ht", "home_corners", "home_fouls", "home_yellow", "home_red"]
    for c in col_home:
        df["avg_"+c+"_5"] = df.groupby("Home_team")[c].shift(1).rolling(window=5, min_periods=1).mean()
    
    col_away = ["away_score", "away_score_ht", "away_shots_on_target", "away_shots", "away_shots_ht", "away_corners", "away_fouls", "away_yellow", "away_red"]    
    for c in col_away: 
        df["avg_"+c+"_5"] = df.groupby("Away_team")[c].shift(1).rolling(window=5, min_periods=1).mean()
    
    home_stats = df.groupby(["Home_team","season_code","league_name"], as_index=False).agg(
            mean_home_score=("home_score", "mean"),
            mean_home_score_ht=("home_score_ht", "mean"),
            mean_home_shots=("home_shots", "mean"),
            mean_home_shots_on_target=("home_shots_on_target", "mean"),
            mean_home_corners=("home_corners", "mean"),
            mean_home_fouls=("home_fouls", "mean"),
            mean_home_yellow=("home_yellow", "mean"),
            mean_home_red=("home_red", "mean")
    )
    away_stats = df.groupby(["Away_team","season_code","league_name"], as_index=False).agg(
            mean_away_score=("away_score", "mean"),
            mean_away_score_ht=("away_score_ht", "mean"),
            mean_away_shots=("away_shots", "mean"),
            mean_away_shots_on_target=("away_shots_on_target", "mean"),
            mean_away_corners=("away_corners", "mean"),
            mean_away_fouls=("away_fouls", "mean"),
            mean_away_yellow=("away_yellow", "mean"),
            mean_away_red=("away_red", "mean")
    )
    home_stats = home_stats.rename(columns={"Home_team": "team"})
    away_stats = away_stats.rename(columns={"Away_team": "team"})
    team_stats = pd.merge(home_stats, away_stats, on=["team", "season_code", "league_name"], how="outer").fillna(0)
    return team_stats
def __main__():
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    output_folder = Path(__file__).resolve().parents[1] / "data" / "proc"
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
            df_premier[col] = df_premier[col].replace(
                {"H": 0, "D": 1, "A": 2})
        if col in df_liga.columns:
            df_liga[col] = df_liga[col].replace(
                {"H": 0, "D": 1, "A": 2})
        if col in df_france.columns:
            df_france[col] = df_france[col].replace(
                {"H": 0, "D": 1, "A": 2})       
if __name__ == "__main__":
    __main__()