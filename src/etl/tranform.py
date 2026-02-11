#!/usr/bin/env python3
import logging
import pandas as pd
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

    
def __main__():
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    output_folder = Path(__file__).resolve().parents[1] / "data" / "proc"
    #Read matches data from CSV
    df = pd.read_csv(entry_folder / "matches.csv")

    if df.empty:
        logger.warning("No matches data found in CSV")
        return
    logger.info("Loaded matches data with %d rows", len(df))

    colums= ["result_ht","result"]

    for col in colums:
        if col in df.columns:
            df[col] = df[col].replace(
                {"H": 0, "D": 1, "A": 2})
    df_liga= df[df["league_name"] == "La Liga"]
    df_premier= df[df["league_name"] == "Premier League"]
    df_france= df[df["league_name"] == "Ligue 1"]
        
def stats_team(df):
    df.groupby(["Home_team"], key=None)
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
        
if __name__ == "__main__":
    __main__()