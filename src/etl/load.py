#!/usr/bin/env python3
import logging
import sys
import pandas as pd
from pathlib import Path
from psycopg2 import sql
from psycopg2.extras import execute_values
#Get the conection to the database
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from database import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("load")

#Cast the types of the columns to be able to insert into the database
def cast_dim_types(df, table_name):
    type_map = {
        "dim_team": {"team_name": "str"},
        "dim_season": {"season_code": "int"},
        "dim_date": {
            "date_match": "date",
            "day_": "int",
            "month_": "int",
            "year_": "int",
            "week_": "int",
        },
        "dim_league": {"league_name": "str"},
        "dim_division": {"division": "str", "league_id": "int"},
        "fact_matches": {
            "season_id": "int",
            "division_id": "int",
            "match_id": "int",
            "date_id": "int",
            "home_team_id": "int",
            "away_team_id": "int",
            "home_score_ht": "int",
            "away_score_ht": "int",
            "result_match": "int",
            "result_ht": "int",
            "home_score": "int",
            "away_score": "int",
            "home_shots": "int",
            "away_shots": "int",
            "home_shots_on_target": "int",
            "away_shots_on_target": "int",
            "home_fouls": "int",
            "away_fouls": "int",
            "home_corners": "int",
            "away_corners": "int",
            "home_yellow": "int",
            "away_yellow": "int",
            "home_red": "int",
            "away_red": "int",
        },
    }

    schema = type_map.get(table_name, {})
    df_casted = df.copy()

    for col, target_type in schema.items():
        if col not in df_casted.columns:
            continue
        if target_type == "int":
            df_casted[col] = pd.to_numeric(df_casted[col], errors="coerce").astype("Int64")
        elif target_type == "date":
            df_casted[col] = pd.to_datetime(df_casted[col], errors="coerce").dt.date
        elif target_type == "str":
            df_casted[col] = df_casted[col].astype("string")

    return df_casted

#Load every csv file into database
def load_data_to_db(conn, df, table_name,list_of_columns,colum_distinct):
    
    try:
        with conn.cursor() as cursor:
            query = """
            INSERT INTO etl.{} ({})
            VALUES %s
            ON CONFLICT ({}) DO NOTHING
            """.format(table_name, ",".join(list_of_columns), ",".join(colum_distinct))
            if df.empty:
                logger.warning("No data to load for table %s", table_name)
                return
            
            df_prepared = df[list_of_columns].astype(object).where(pd.notna(df[list_of_columns]), None)
            
            rows = [tuple(row) for row in df_prepared.itertuples(index=False, name=None)]
            
            execute_values(
                cursor,
                query,
                rows
            )
            logger.info("Data loaded into table %s successfully", table_name)
    except Exception:
        conn.rollback()
        logger.exception("Data load failed")
        raise
#Get id and change it in the df to be able to insert into the fact table
def change_id(conn, df, new_id_cols, old_id_cols,name_tables,name_where):
    cursor = conn.cursor()
    colums= ["result_ht","result"]

    for col in colums:
        if col in df.columns:
            df[col] = df[col].map({"H": 0, "D": 1, "A": 2})
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for new_col, old_col,name_table,name_where_col in zip(new_id_cols, old_id_cols,name_tables,name_where):
        if name_where_col == "date_match" and old_col in df.columns:
            df[old_col] = pd.to_datetime(df[old_col], errors="coerce").dt.date
        if new_col == "home_team_id" or new_col == "away_team_id":
            copy = new_col
            new_col = "team_id"
        cursor.execute(f"SELECT {new_col}, {name_where_col} FROM etl.{name_table}")
        rows = cursor.fetchall()
        id_map = {row[1]: row[0] for row in rows}
        df[old_col] = df[old_col].map(id_map)
        if new_col == "team_id":
            new_col = copy
        df.rename(columns={old_col: new_col}, inplace=True)
    cursor.close()
    logger.info("IDs changed for fact table")
    return df
#Funcion to get df to insert into the tables of database
def create_power_bi():
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    csv_files = list(entry_folder.glob("*.csv"))
    if not csv_files:
        logger.warning("No CSV files found in %s", entry_folder)
        return
    conn = db.get_db_connection()
    cursor = conn.cursor()
    new_cols = ["season_id", "division_id", "date_id", "home_team_id", "away_team_id"]
    old_cols = ["season_code", "division", "Date", "HomeTeam", "AwayTeam"]
    name_tables = ["dim_season", "dim_division", "dim_date", "dim_team", "dim_team"]
    name_where = ["season_code", "division", "date_match", "team_name", "team_name"]
    try:
        for csv_file in csv_files:
            logger.info("Reading file: %s", csv_file)
            df_all = pd.read_csv(csv_file)
            if df_all.empty:
                logger.warning("No data found in file: %s", csv_file)
                continue

            if "league_name" not in df_all.columns:
                logger.error("Missing column league_name in file: %s", csv_file)
                raise ValueError("Missing column league_name")

            league_names = df_all["league_name"].dropna().unique()
            if len(league_names) == 0:
                logger.error("No league_name values found in file: %s", csv_file)
                raise ValueError("No league_name values found")

            for league_name in league_names:
                df = df_all[df_all["league_name"] == league_name].copy()
                if df.empty:
                    continue

                logger.info("Processing league %s from file: %s", league_name, csv_file)

                teams = pd.concat([df["HomeTeam"], df["AwayTeam"]]).unique()
                dim_team = pd.DataFrame({"team_name": teams})
                dim_season = pd.DataFrame({"season_code": df["season_code"].unique()})
                dim_division = pd.DataFrame({"division": df["division"].unique()})
                dim_date = pd.DataFrame({"date_match": df["Date"].unique()})
                dim_date["day_"] = pd.to_datetime(dim_date["date_match"]).dt.day
                dim_date["month_"] = pd.to_datetime(dim_date["date_match"]).dt.month
                dim_date["year_"] = pd.to_datetime(dim_date["date_match"]).dt.year
                dim_date["week_"] = pd.to_datetime(dim_date["date_match"]).dt.isocalendar().week
                dim_league = pd.DataFrame({"league_name": [league_name]})
                logger.info("Dataframes for dimensions created for file: %s (league=%s)", csv_file, league_name)
                fact_matches = df.copy()
                fact_matches.drop(columns=["league_name"], inplace=True)
                
                dim_team = cast_dim_types(dim_team, "dim_team")
                dim_season = cast_dim_types(dim_season, "dim_season")
                dim_date = cast_dim_types(dim_date, "dim_date")
                dim_league = cast_dim_types(dim_league, "dim_league")
                
                load_data_to_db(conn, dim_team, "dim_team", dim_team.columns.tolist(), ["team_name"])
                
                load_data_to_db(conn, dim_season, "dim_season", dim_season.columns.tolist(), ["season_code"])
                
                load_data_to_db(conn, dim_date, "dim_date", dim_date.columns.tolist(), ["date_match"])
                
                load_data_to_db(conn, dim_league, "dim_league", dim_league.columns.tolist(), ["league_name"])
    
                #Get id of league to insert into the division table
                cursor.execute(
                    "SELECT d.league_id FROM etl.dim_league d where d.league_name = %s",
                    (league_name,),
                )
            
                row = cursor.fetchone()
                if row is None:
                    logger.error("League ID not found for league_name: %s", league_name)
                    raise ValueError("League ID not found")
                league_id = row[0]
                dim_division["league_id"] = league_id
                dim_division = cast_dim_types(dim_division, "dim_division")
                load_data_to_db(conn, dim_division, "dim_division", dim_division.columns.tolist(), ["division"])
            
                fact_matches = change_id(conn, fact_matches, new_cols, old_cols,name_tables,name_where)
                fact_matches = fact_matches.rename(columns={"result": "result_match"})
                fact_matches = cast_dim_types(fact_matches, "fact_matches")
                load_data_to_db(conn, fact_matches, "fact_matches", fact_matches.columns.tolist(), new_cols)
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Power BI data load failed")
        raise
    finally:
        cursor.close()
        conn.close()
#Truncate the matches table and load the new data from the csv files into the database
def __main__():

    logger.info("Starting data load process")

    conn = db.get_db_connection()
    logger.info("Database connection established")
    cursor = conn.cursor()
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "final"
    csv_files = list(entry_folder.glob("*.csv"))

    if not csv_files:
        logger.warning("No CSV files found in %s", entry_folder)
        conn.close()
        return

    schema_name = "etl"
    table_name = "matches"

    try:
        logger.info("Truncating destination table %s.%s", schema_name, table_name)
        conn.commit()  # Ensure any pending transactions are committed before truncating
        cursor.execute(
            sql.SQL("TRUNCATE TABLE {}.{}")
            .format(sql.Identifier(schema_name), sql.Identifier(table_name))
        )

        # IMPORTANT:
        # The CSV column order in src/data/final/*.csv is not the same as the table definition order.
        # COPY without a column list loads by position and will misalign values (e.g., elo_diff can end up in avg_*).
        # Define the COPY column list to match the CSV order.
        csv_order_columns = [
            "season_code",
            "league_name",
            "division",
            "date_match",      # CSV header uses 'Date' but COPY maps by position
            "hometeam",        # CSV header uses 'HomeTeam'
            "awayteam",        # CSV header uses 'AwayTeam'
            "result_match",    # CSV header uses 'result'
            "result_ht",
            "league_id",
            "home_cluster",
            "away_cluster",
            "elo",
            "elo_away",
            "elo_diff",
            "avg_home_score_5",
            "avg_home_score_ht_5",
            "avg_home_shots_on_target_5",
            "avg_home_shots_5",
            "avg_home_corners_5",
            "avg_home_fouls_5",
            "avg_home_yellow_5",
            "avg_home_red_5",
            "avg_away_score_5",
            "avg_away_score_ht_5",
            "avg_away_shots_on_target_5",
            "avg_away_shots_5",
            "avg_away_corners_5",
            "avg_away_fouls_5",
            "avg_away_yellow_5",
            "avg_away_red_5",
            "home_score",
            "away_score",
        ]

        copy_query = sql.SQL(
            "COPY {}.{} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)"
        ).format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(c) for c in csv_order_columns),
        )

        for csv_file in csv_files:
            logger.info("Loading %s into %s.%s using COPY", csv_file, schema_name, table_name)
            conn.commit()  # Ensure any pending transactions are committed before loading
            with open(csv_file, "r", encoding="utf-8") as file_obj:
                cursor.copy_expert(copy_query.as_string(conn), file_obj)

        conn.commit()
        logger.info("Data load process completed")
    except Exception:
        conn.rollback()
        logger.exception("Data load failed")
        raise
    finally:
        conn.close()
    
    create_power_bi()
    
if __name__ == "__main__":
    __main__()