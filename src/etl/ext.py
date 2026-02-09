#!/usr/bin/env python3
import logging
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
DEFAULT_SEASONS_BACK = 3
DIVISION = "SP1"


def build_season_codes(seasons_back=DEFAULT_SEASONS_BACK, today=None):
    if today is None:
        today = date.today()
    current_start_year = today.year if today.month >= 7 else today.year - 1
    codes = []
    for offset in range(1, seasons_back + 1):
        start_year = current_start_year - offset
        end_year = start_year + 1
        codes.append(f"{start_year % 100:02d}{end_year % 100:02d}")
    return codes


def fetch_season_csv(season_code, division=DIVISION):
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


def normalize_matches(raw_df, season_code, division, source_url):
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

    df["match_date"] = (
        pd.to_datetime(df["match_date"], errors="coerce", dayfirst=True)
        .dt.strftime("%Y-%m-%d")
    )
    df = df.dropna(subset=["match_date", "home_team", "away_team"])

    for col in ["home_score", "away_score", "home_score_ht", "away_score_ht"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["season_code"] = season_code
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


def build_standings(matches_df):
    if matches_df.empty:
        return pd.DataFrame()

    matches_df = matches_df.dropna(subset=["home_score", "away_score"])
    if matches_df.empty:
        return pd.DataFrame()

    home = matches_df.assign(
        team=matches_df["home_team"],
        goals_for=matches_df["home_score"],
        goals_against=matches_df["away_score"],
        won=(matches_df["home_score"] > matches_df["away_score"]).astype(int),
        draw=(matches_df["home_score"] == matches_df["away_score"]).astype(int),
        lost=(matches_df["home_score"] < matches_df["away_score"]).astype(int),
    )[["season_code", "division", "team", "goals_for", "goals_against", "won", "draw", "lost"]]

    away = matches_df.assign(
        team=matches_df["away_team"],
        goals_for=matches_df["away_score"],
        goals_against=matches_df["home_score"],
        won=(matches_df["away_score"] > matches_df["home_score"]).astype(int),
        draw=(matches_df["away_score"] == matches_df["home_score"]).astype(int),
        lost=(matches_df["away_score"] < matches_df["home_score"]).astype(int),
    )[["season_code", "division", "team", "goals_for", "goals_against", "won", "draw", "lost"]]

    totals = pd.concat([home, away], ignore_index=True)
    grouped = totals.groupby(["season_code", "division", "team"], as_index=False).sum(numeric_only=True)
    grouped["played"] = grouped["won"] + grouped["draw"] + grouped["lost"]
    grouped["goal_diff"] = grouped["goals_for"] - grouped["goals_against"]
    grouped["points"] = grouped["won"] * 3 + grouped["draw"]

    grouped = grouped.sort_values(
        ["season_code", "division", "points", "goal_diff", "goals_for", "team"],
        ascending=[True, True, False, False, False, True],
    )
    grouped["position"] = grouped.groupby(["season_code", "division"]).cumcount() + 1
    grouped = grouped.rename(columns={"team": "team_name"})

    return grouped[
        [
            "season_code",
            "division",
            "position",
            "team_name",
            "played",
            "won",
            "draw",
            "lost",
            "points",
            "goals_for",
            "goals_against",
            "goal_diff",
        ]
    ]


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


def fetch_spain_matches_and_standings(output_folder, seasons_back=DEFAULT_SEASONS_BACK):
    output_folder.mkdir(parents=True, exist_ok=True)
    season_codes = build_season_codes(seasons_back=seasons_back)
    logger.info("Fetching Spain %s for seasons: %s", DIVISION, ", ".join(season_codes))

    match_frames = []
    for season_code in season_codes:
        csv_text, source_url = fetch_season_csv(season_code)
        if not csv_text:
            continue
        raw_df = pd.read_csv(StringIO(csv_text))
        normalized = normalize_matches(raw_df, season_code, DIVISION, source_url)
        if not normalized.empty:
            match_frames.append(normalized)

    matches_df = pd.concat(match_frames, ignore_index=True) if match_frames else pd.DataFrame()
    if not matches_df.empty:
        merge_and_save(
            matches_df,
            output_folder / "matches.csv",
            dedupe_keys=["match_id"],
            sort_keys=["season_code", "match_date", "home_team", "away_team"],
        )
    else:
        logger.warning("No match data collected.")

    standings_df = build_standings(matches_df)
    if not standings_df.empty:
        merge_and_save(
            standings_df,
            output_folder / "standings.csv",
            dedupe_keys=["season_code", "division", "team_name"],
            sort_keys=["season_code", "division", "position"],
        )
    else:
        logger.warning("No standings data collected.")


def __main__():
    output_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    logger.info("Output folder: %s", output_folder)
    fetch_spain_matches_and_standings(output_folder, seasons_back=DEFAULT_SEASONS_BACK)


if __name__ == "__main__":
    __main__()