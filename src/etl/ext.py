#!/usr/bin/env python3
import logging
import os
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

API_BASE = "https://api.football-data.org/v4"
REQUEST_DELAY_SECONDS = 1.0
DEFAULT_LOOKBACK_DAYS = 730


def get_api_key():
    api_key = os.getenv("FOOTBALL_DATA_API_KEY", "").strip()
    if not api_key:
        logger.error("Missing FOOTBALL_DATA_API_KEY environment variable.")
        return None
    return api_key


def fetch_json(endpoint, api_key, params=None, max_retries=3):
    headers = {"X-Auth-Token": api_key}
    url = f"{API_BASE}{endpoint}"

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else 10
                logger.warning("Rate limit hit. Waiting %s seconds.", wait_seconds)
                time.sleep(wait_seconds)
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            logger.exception("Request failed (%s/%s): %s", attempt, max_retries, url)
            time.sleep(2)
    return None


def list_european_competitions(api_key):
    payload = fetch_json("/competitions", api_key=api_key)
    if not payload:
        return []
    competitions = []
    for item in payload.get("competitions", []):
        area = item.get("area", {})
        if area.get("parentArea") == "Europe" or area.get("name") == "Europe":
            competitions.append(item)
    return competitions


def build_match_rows(competition, matches):
    rows = []
    for match in matches:
        season = match.get("season") or {}
        score = match.get("score", {})
        full_time = score.get("fullTime", {})
        home_team = match.get("homeTeam", {})
        away_team = match.get("awayTeam", {})
        rows.append(
            {
                "competition_id": competition.get("id"),
                "competition_name": competition.get("name"),
                "season_id": season.get("id"),
                "season_start": season.get("startDate"),
                "season_end": season.get("endDate"),
                "match_id": match.get("id"),
                "utc_date": match.get("utcDate"),
                "status": match.get("status"),
                "matchday": match.get("matchday"),
                "stage": match.get("stage"),
                "group": match.get("group"),
                "home_team_id": home_team.get("id"),
                "home_team": home_team.get("name"),
                "away_team_id": away_team.get("id"),
                "away_team": away_team.get("name"),
                "home_score": full_time.get("home"),
                "away_score": full_time.get("away"),
                "winner": score.get("winner"),
            }
        )
    return rows


def build_standing_rows(competition, standings, season):
    rows = []
    for standing in standings:
        for entry in standing.get("table", []):
            team = entry.get("team", {})
            rows.append(
                {
                    "competition_id": competition.get("id"),
                    "competition_name": competition.get("name"),
                    "season_id": season.get("id"),
                    "season_start": season.get("startDate"),
                    "season_end": season.get("endDate"),
                    "stage": standing.get("stage"),
                    "type": standing.get("type"),
                    "group": standing.get("group"),
                    "position": entry.get("position"),
                    "team_id": team.get("id"),
                    "team_name": team.get("name"),
                    "played": entry.get("playedGames"),
                    "won": entry.get("won"),
                    "draw": entry.get("draw"),
                    "lost": entry.get("lost"),
                    "points": entry.get("points"),
                    "goals_for": entry.get("goalsFor"),
                    "goals_against": entry.get("goalsAgainst"),
                    "goal_diff": entry.get("goalDifference"),
                    "form": entry.get("form"),
                }
            )
    return rows


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


def fetch_matches_and_standings(output_folder, lookback_days=DEFAULT_LOOKBACK_DAYS):
    api_key = get_api_key()
    if not api_key:
        return



    competitions = list_european_competitions(api_key)
    logger.info("Found %s European competitions", len(competitions))

    date_to = date.today()
    date_from = date_to - timedelta(days=lookback_days)
    date_from_str = date_from.isoformat()
    date_to_str = date_to.isoformat()

    match_rows = []
    standing_rows = []

    for competition in competitions:
        competition_id = competition.get("id")
        if not competition_id:
            continue

        logger.info("Fetching matches for %s", competition.get("name"))
        matches_payload = fetch_json(
            f"/competitions/{competition_id}/matches",
            api_key=api_key,
            params={"dateFrom": date_from_str, "dateTo": date_to_str},
        )
        if matches_payload:
            match_rows.extend(build_match_rows(competition, matches_payload.get("matches", [])))

        time.sleep(REQUEST_DELAY_SECONDS)

        logger.info("Fetching standings for %s", competition.get("name"))
        standings_payload = fetch_json(
            f"/competitions/{competition_id}/standings",
            api_key=api_key,
        )
        if standings_payload:
            standings_season = standings_payload.get("season") or {}
            standing_rows.extend(
                build_standing_rows(competition, standings_payload.get("standings", []), standings_season)
            )

        time.sleep(REQUEST_DELAY_SECONDS)

    if match_rows:
        matches_df = pd.DataFrame(match_rows)
        merge_and_save(
            matches_df,
            output_folder / "matches.csv",
            dedupe_keys=["match_id"],
            sort_keys=["utc_date", "competition_id", "match_id"],
        )
    else:
        logger.warning("No match data collected.")

    if standing_rows:
        standings_df = pd.DataFrame(standing_rows)
        merge_and_save(
            standings_df,
            output_folder / "standings.csv",
            dedupe_keys=["competition_id", "season_id", "stage", "group", "type", "position", "team_id"],
            sort_keys=["competition_id", "season_id", "stage", "type", "group", "position"],
        )
    else:
        logger.warning("No standings data collected.")


def __main__():
    output_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    logger.info("Output folder: %s", output_folder)
    fetch_matches_and_standings(output_folder, lookback_days=DEFAULT_LOOKBACK_DAYS)


if __name__ == "__main__":
    __main__()