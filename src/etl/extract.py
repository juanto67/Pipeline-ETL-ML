#!/usr/bin/env python3
import logging
import time
from datetime import date
from io import StringIO
from pathlib import Path
import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("extract")

DATA_BASE = "https://www.football-data.co.uk/mmz4281"
SEASONS_BACK = 10
LEAGUES = {
    "La Liga": "SP1",
    "Premier League": "E0",
    "Ligue 1": "F1",
}

#Column to see the number of years to the match
def build_season_codes(seasons_back=SEASONS_BACK, today=None):
    if today is None:
        today = date.today()
    current_start_year = today.year
    codes = []
    for offset in range(1, seasons_back + 1):
        start_year = current_start_year - offset
        end_year = start_year + 1
        codes.append(f"{start_year % 100:02d}{end_year % 100:02d}")
    return codes

#Feching the csv file for the season and division
def fetch_season_csv(season_code, division):
    url = f"{DATA_BASE}/{season_code}/{division}.csv"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 404:
            logger.warning("Season not found: %s", url)
            return None, url
        response.raise_for_status()
        return response.text, url
    except requests.RequestException:
        logger.exception("Failed to fetch %s", url)
        return None, url

#Change a little bit the data to be understable
#Transform the raw to the type needed
def normalize_matches(raw_df, season_code, league_name, division):
    df = raw_df.rename(
        columns={
            "FTHG": "home_score",
            "FTAG": "away_score",
            "FTR": "result",
            "HTHG": "home_score_ht",
            "HTAG": "away_score_ht",
            "HTR": "result_ht",
            "HS": "home_shots",
            "AS": "away_shots",
            "HST": "home_shots_on_target",
            "AST": "away_shots_on_target",
            "HC": "home_corners",
            "AC": "away_corners",
            "HF": "home_fouls",
            "AF": "away_fouls",
            "HY": "home_yellow",
            "AY": "away_yellow",
            "HR": "home_red",
            "AR": "away_red",
            
        }
    )

    # Parse common football-data date formats without inference warnings.
    parsed_dates = pd.to_datetime(
        df["Date"],
        errors="coerce",
        dayfirst=True,
        format="%d/%m/%y",
    )
    if parsed_dates.isna().any():
        missing_mask = parsed_dates.isna()
        parsed_dates.loc[missing_mask] = pd.to_datetime(
            df.loc[missing_mask, "Date"],
            errors="coerce",
            dayfirst=True,
            format="%d/%m/%Y",
        )
    if parsed_dates.isna().any():
        missing_mask = parsed_dates.isna()
        parsed_dates.loc[missing_mask] = pd.to_datetime(
            df.loc[missing_mask, "Date"],
            errors="coerce",
            format="%Y-%m-%d",
        )
    df["Date"] = parsed_dates.dt.strftime("%Y-%m-%d")

    for col in [
        "home_score",
        "away_score",
        "home_score_ht",
        "away_score_ht",
        "home_shots",
        "away_shots",
        "home_shots_on_target",
        "away_shots_on_target",
        "home_corners",
        "away_corners",
        "home_fouls",
        "away_fouls",
        "home_yellow",
        "away_yellow",
        "home_red",
        "away_red",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["season_code"] = season_code
    df["league_name"] = league_name
    df["division"] = division

    columns = [
        "season_code",
        "league_name",
        "division",
        "Date",
        "HomeTeam",
        "AwayTeam",
        "home_score",
        "away_score",
        "result",
        "home_score_ht",
        "away_score_ht",
        "result_ht",
        "home_shots",
        "away_shots",
        "home_shots_on_target",
        "away_shots_on_target",
        "home_corners",
        "away_corners",
        "home_fouls",
        "away_fouls",
        "home_yellow",
        "away_yellow",
        "home_red",
        "away_red",
    ]
    existing_cols = [col for col in columns if col in df.columns]
    return df[existing_cols]

#Save data to CSV, sort and remove duplicates
def merge_and_save(df, filename, dedupe_keys, sort_keys):
    if filename.exists():
        existing = pd.read_csv(filename)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df.copy()

    combined = combined.copy()

    for col in dedupe_keys:
        if col in combined.columns:
            combined[col] = combined[col].astype(str).str.strip()

    if "Date" in combined.columns:
        combined["Date"] = pd.to_datetime(
            combined["Date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    for col in [
        "home_score",
        "away_score",
        "home_score_ht",
        "away_score_ht",
        "home_shots",
        "away_shots",
        "home_shots_on_target",
        "away_shots_on_target",
        "home_corners",
        "away_corners",
        "home_fouls",
        "away_fouls",
        "home_yellow",
        "away_yellow",
        "home_red",
        "away_red",
    ]:
        if col in combined.columns:
            combined[col] = pd.to_numeric(combined[col], errors="coerce").astype("Int64")
    combined = combined.drop_duplicates(subset=dedupe_keys, keep="last").reset_index(drop=True)
    combined = combined.sort_values(sort_keys).reset_index(drop=True)
    combined.to_csv(filename, index=False)
    logger.info("Saved %s rows to %s", len(combined), filename)

#Main function to fetch matches for all seasons and leagues, normalize, and save to CSV
def fetch_matches(output_folder):
    output_folder.mkdir(parents=True, exist_ok=True)
    season_codes = build_season_codes()

    all_frames = []
    for league_name, division in LEAGUES.items():
        for season_code in season_codes:
            logger.info("Fetching %s season %s", league_name, season_code)
            csv_text, _ = fetch_season_csv(season_code, division)
            if not csv_text:
                continue
            raw_df = pd.read_csv(StringIO(csv_text))
            normalized = normalize_matches(raw_df, season_code, league_name, division)
            if not normalized.empty:
                all_frames.append(normalized)
    if all_frames:
        matches_df = pd.concat(all_frames, ignore_index=True)
        
    else:
        matches_df = pd.DataFrame()
        logger.warning("No match data collected from any season or league.")
        return
    merge_and_save(
        matches_df,
        output_folder / "matches.csv",
        dedupe_keys=["season_code", "division", "league_name","Date", "HomeTeam", "AwayTeam"],
        sort_keys=["season_code", "division", "league_name", "Date", "HomeTeam", "AwayTeam"],
    )


def __main__():
    output_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    logger.info("Output folder: %s", output_folder)
    fetch_matches(output_folder)


if __name__ == "__main__":
    __main__()