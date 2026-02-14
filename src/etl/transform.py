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

def add_elo(df, k=10, base_elo=100):
    df = df.sort_values(["season_code", "division", "Date", "HomeTeam", "AwayTeam"])
    df = df.copy()
    df["elo"] = float(base_elo)
    df["elo_away"] = float(base_elo)
    ratings = {}
    logger.info("Starting ELO calculation for %d matches", len(df))
    season = None
    division = None
    for i, row in df.iterrows():
        if (
            season is not None
            and division is not None
            and (season != row["season_code"] or division != row["division"])
        ):
            ratings = {}
        home_team = row["HomeTeam"]
        away_team = row["AwayTeam"]

        home_elo = ratings.get(home_team, base_elo)
        away_elo = ratings.get(away_team, base_elo)

        # ELO antes del partido (features)
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
        ratings[home_team] = round(home_elo + k * (home_score - expected_home), 3)
        ratings[away_team] = round(away_elo + k * (away_score - expected_away), 3)
    df["elo_diff"] = (df["elo"] - df["elo_away"]).round(3)
    logger.info("Completed ELO calculation")
    return df

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
    
    
def stats_team(df):
    col_home = ["home_score", "home_score_ht", "home_shots_on_target", "home_shots", "home_corners", "home_fouls", "home_yellow", "home_red"]
    df = df.sort_values(["season_code","Date","HomeTeam"])
    #Calculate avg of the 5 last matches for home and away teams, moving the current match 
    funcion = lambda s: s.shift(1).rolling(window=5, min_periods=1).mean()
    for c in col_home:
        df["avg_"+c+"_5"] = (
            df.groupby("HomeTeam")[c]
            .transform(funcion)
            .fillna(0)
            .round(3)
        )
        logger.info("Calculated avg of last 5 matches for home team column %s", c)
        
    col_away = ["away_score", "away_score_ht", "away_shots_on_target", "away_shots", "away_corners", "away_fouls", "away_yellow", "away_red"]    
    df = df.sort_values(["season_code","Date","AwayTeam"])
    for c in col_away: 
        df["avg_"+c+"_5"] = (
            df.groupby("AwayTeam")[c]
            .transform(funcion)
            .fillna(0)
            .round(3)
        )
        logger.info("Calculated avg of last 5 matches for away team column %s", c)
    df = df.drop(columns=["home_score", "home_score_ht", "home_shots_on_target", "home_shots", "home_corners", "home_fouls", "home_yellow", "home_red","away_score", "away_score_ht", "away_shots_on_target", "away_shots", "away_corners", "away_fouls", "away_yellow", "away_red"], errors="ignore")    
    return df
def deduplicate(df, dedupe_keys):
    before = len(df)
    df = df.drop_duplicates(subset=dedupe_keys, keep="last").reset_index(drop=True)
    after = len(df)
    logger.info("Deduplicated dataframe from %d to %d rows using keys %s", before, after, dedupe_keys)
    return df

def modify_save(df_liga, df_premier, df_france, output_folder):
    dedupe_keys = ["season_code", "division", "Date", "HomeTeam", "AwayTeam"]
    for name, df in [("liga", df_liga), ("premier", df_premier), ("france", df_france)]:
        if df.empty:
            logger.warning("One of the dataframes is empty, skipping ELO and stats calculation")
            return
        df = deduplicate(df, dedupe_keys)
        df = stats_team(df)
        df = add_elo(df)
        merge_and_save(df, output_folder / f"matches_{name}.csv", dedupe_keys=dedupe_keys, sort_keys=["season_code", "division", "Date", "HomeTeam", "AwayTeam"])

def __main__():
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    output_folder = Path(__file__).resolve().parents[1] / "data" / "proc"
    os.makedirs(output_folder, exist_ok=True)
    #Read matches data from CSV
    df_liga = pd.read_csv(entry_folder / "matches_liga.csv")
    df_premier = pd.read_csv(entry_folder / "matches_premier.csv")
    df_france = pd.read_csv(entry_folder / "matches_france.csv")        
    logger.info("Loaded matches data with %d rows, %d rows, %d rows", len(df_liga), len(df_premier), len(df_france))

    colums= ["result_ht","result"]

    for col in colums:
        if col in df_premier.columns:
            df_premier[col] = df_premier[col].map({"H": 0, "D": 1, "A": 2})
            df_premier[col] = pd.to_numeric(df_premier[col], errors="coerce").astype("Int64")
        if col in df_liga.columns:
            df_liga[col] = df_liga[col].map({"H": 0, "D": 1, "A": 2})
            df_liga[col] = pd.to_numeric(df_liga[col], errors="coerce").astype("Int64")
        if col in df_france.columns:
            df_france[col] = df_france[col].map({"H": 0, "D": 1, "A": 2})
            df_france[col] = pd.to_numeric(df_france[col], errors="coerce").astype("Int64")
    
    df_cpy_liga = df_liga.copy()
    df_cpy_premier = df_premier.copy()
    df_cpy_france = df_france.copy()
    
    modify_save(df_liga, df_premier, df_france, output_folder)
    
    
    
if __name__ == "__main__":
    __main__()