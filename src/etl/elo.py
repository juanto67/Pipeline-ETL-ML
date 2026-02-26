#!/usr/bin/env python3
import logging
import pandas as pd
from pathlib import Path
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("elo_calculation")
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
#Calculating the ELO rating for each team before the match, and the difference between them as a feature for the model
def add_elo(df, k=10, base_elo=100):
    df = df.sort_values(["season_code", "division", "league_name", "Date", "HomeTeam", "AwayTeam"])
    df = df.copy()
    df["elo"] = float(base_elo)
    df["elo_away"] = float(base_elo)
    ratings = {}
    logger.info("Starting ELO calculation for %d matches", len(df))
    season = None
    division = None
    league_name = None
    for i, row in df.iterrows():
        if (
            season is not None
            and division is not None
            and league_name is not None
            and (season != row["season_code"] or division != row["division"] or league_name != row["league_name"])
        ):
            ratings = {}
        home_team = row["HomeTeam"]
        away_team = row["AwayTeam"]

        home_elo = ratings.get(home_team, base_elo)
        away_elo = ratings.get(away_team, base_elo)

        # ELO features
        df.at[i, "elo"] = home_elo
        df.at[i, "elo_away"] = away_elo
        

        result = row["result"]  # 0 home, 1 draw, 2 away
        expected_home = 1 / (1 + 10 ** ((away_elo - home_elo) / 400))
        expected_away = 1 / (1 + 10 ** ((home_elo - away_elo) / 400))

        if result == 0:
            home_score, away_score = 1.0, 0.0
        elif result == 1:
            home_score, away_score = 0.5, 0.5
        else:
            home_score, away_score = 0.0, 1.0
        division = row["division"]
        season = row["season_code"]
        league_name = row["league_name"]
        ratings[home_team] = round(home_elo + k * (home_score - expected_home), 3)
        ratings[away_team] = round(away_elo + k * (away_score - expected_away), 3)
    df["elo_diff"] = (df["elo"] - df["elo_away"]).round(3)
    logger.info("Completed ELO calculation")
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
    df = add_elo(df)
    df = df.drop(columns=["home_score", "home_score_ht", "home_shots_on_target", "home_shots", "home_corners", "home_fouls", "home_yellow", "home_red","away_score", "away_score_ht", "away_shots_on_target", "away_shots", "away_corners", "away_fouls", "away_yellow", "away_red"], errors="ignore")    

    merge_and_save(df, output_folder / "matches_elo.csv", dedupe_keys=["season_code", "division", "league_name", "Date", "HomeTeam", "AwayTeam"], sort_keys=["season_code", "division", "league_name", "Date", "HomeTeam", "AwayTeam"])
    logger.info("Loaded matches_proc.csv with %d rows", len(df))
if __name__ == "__main__":
    __main__()