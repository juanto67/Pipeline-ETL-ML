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

logger = logging.getLogger(__name__)

DATA_BASE = "https://www.football-data.co.uk/mmz4281"
REQUEST_DELAY_SECONDS = 0.2
SEASONS_BACK = 10

LEAGUES = {
    "La Liga": "SP1",
    "Premier League": "E0",
    "Ligue 1": "F1",
}


def build_season_codes(seasons_back=SEASONS_BACK, today=None):
    if today is None:
        today = date.today()
    current_start_year = today.year if today.month >= 7 else today.year - 1
    codes = []
    for offset in range(1, seasons_back + 1):
        start_year = current_start_year - offset
        end_year = start_year + 1
        codes.append(f"{start_year % 100:02d}{end_year % 100:02d}")
    return codes


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


def normalize_matches(raw_df, season_code, league_name, division, source_url):
    required_cols = {"Date", "HomeTeam", "AwayTeam"}
    if not required_cols.issubset(raw_df.columns):
        logger.warning("Missing required columns in %s", source_url)
        return pd.DataFrame()

    df = raw_df.rename(
        columns={
            "Date": "match_date",
            "HomeTeam": "home_team",
            "AwayTeam": "away_team",
            "FTHG": "home_score",
            "FTAG": "away_score",
            "FTR": "result",
            "HTHG": "home_score_ht",
            "HTAG": "away_score_ht",
            "HTR": "result_ht",
        }
    )

    parsed_dates = pd.to_datetime(df["match_date"], errors="coerce", format="%d/%m/%y")
    if parsed_dates.isna().any():
        parsed_dates = parsed_dates.fillna(
            pd.to_datetime(df["match_date"], errors="coerce", format="%d/%m/%Y")
        )
    df["match_date"] = parsed_dates.dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["match_date", "home_team", "away_team"])

    for col in ["home_score", "away_score", "home_score_ht", "away_score_ht"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["season_code"] = season_code
    df["league_name"] = league_name
    df["division"] = division
    df["source_url"] = source_url
    df["match_id"] = (
        df["season_code"].astype(str)
        + "-"
        + df["division"].astype(str)
        + "-"
        + df["match_date"].astype(str)
        + "-"
        + df["home_team"].astype(str)
        + "-"
        + df["away_team"].astype(str)
    )

    columns = [
        "match_id",
        "season_code",
        "league_name",
        "division",
        "match_date",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "result",
        "home_score_ht",
        "away_score_ht",
        "result_ht",
        "source_url",
    ]
    existing_cols = [col for col in columns if col in df.columns]
    return df[existing_cols]


def merge_and_save(df, filename, dedupe_keys, sort_keys):
    if filename.exists():
        existing = pd.read_csv(filename)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df
    combined = combined.drop_duplicates(subset=dedupe_keys, keep="last")
    combined = combined.sort_values(sort_keys).reset_index(drop=True)
    combined.to_csv(filename, index=False)
    logger.info("Saved %s rows to %s", len(combined), filename)


def fetch_matches(output_folder):
    output_folder.mkdir(parents=True, exist_ok=True)
    season_codes = build_season_codes()

    all_frames = []
    for league_name, division in LEAGUES.items():
        for season_code in season_codes:
            logger.info("Fetching %s season %s", league_name, season_code)
            csv_text, source_url = fetch_season_csv(season_code, division)
            if not csv_text:
                continue
            raw_df = pd.read_csv(StringIO(csv_text))
            normalized = normalize_matches(raw_df, season_code, league_name, division, source_url)
            if not normalized.empty:
                all_frames.append(normalized)
            time.sleep(REQUEST_DELAY_SECONDS)

    matches_df = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
    if matches_df.empty:
        logger.warning("No match data collected.")
        return

    merge_and_save(
        matches_df,
        output_folder / "matches.csv",
        dedupe_keys=["match_id"],
        sort_keys=["season_code", "division", "match_date", "home_team", "away_team"],
    )


def __main__():
    output_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    logger.info("Output folder: %s", output_folder)
    fetch_matches(output_folder)


if __name__ == "__main__":
    __main__()