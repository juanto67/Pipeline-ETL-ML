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


def encode_result_columns(df, columns=("result_ht", "result")):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].map({"H": 0, "D": 1, "A": 2})
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df



#Saving into CSV and drop duplicates and sorting
def merge_and_save(df, filename, dedupe_keys, sort_keys):
    if filename.exists():
        existing = pd.read_csv(filename)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df
    combined = combined.drop_duplicates(subset=dedupe_keys, keep="last").reset_index(drop=True)
    combined = combined.sort_values(sort_keys).reset_index(drop=True)
    colums= ["home_score","away_score","result","home_score_ht","away_score_ht","result_ht","home_shots","away_shots","home_shots_on_target","away_shots_on_target","home_corners","away_corners","home_fouls","away_fouls","home_yellow","away_yellow","home_red","away_red"]

    for col in colums:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce").astype("Int64")
    combined.to_csv(filename, index=False)
    logger.info("Saved %s rows to %s", len(combined), filename)  
    

#Drop duplicates before using df
def deduplicate(df, dedupe_keys):
    before = len(df)
    df = df.drop_duplicates(subset=dedupe_keys, keep="last").reset_index(drop=True)
    after = len(df)
    logger.info("Deduplicated dataframe from %d to %d rows using keys %s", before, after, dedupe_keys)
    return df
#Funcion to modifa df and save it
def modify_save(df, output_folder):
    dedupe_keys = ["season_code", "division", "league_name", "Date", "HomeTeam", "AwayTeam"]
    
    if df.empty:
        logger.warning("Dataframe is empty, skipping ELO and stats calculation")
        return
    df = deduplicate(df, dedupe_keys)
    
    merge_and_save(df, output_folder / f"matches_proc.csv", dedupe_keys=dedupe_keys, sort_keys=["season_code", "division", "league_name", "Date", "HomeTeam", "AwayTeam"])
#Main function to read data, transform it and save it
def __main__():
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    output_folder = Path(__file__).resolve().parents[1] / "data" / "proc"
    os.makedirs(output_folder, exist_ok=True)
    #Read matches data from CSV
    csv_files = sorted(entry_folder.glob("*.csv"))
    if not csv_files:
        logger.warning("No input files found in %s", entry_folder)
        return


    for file in csv_files:
        df = pd.read_csv(file)
        df = encode_result_columns(df)
        modify_save(df, output_folder)
        logger.info("Loaded %s with %d rows", file.name, len(df))


    
    
if __name__ == "__main__":
    __main__()