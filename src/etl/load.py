#!/usr/bin/env python3
import logging
import sys
from pathlib import Path
from psycopg2 import sql

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from database import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("load")

def create_power_bi():
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    csv_files = list(entry_folder.glob("*.csv"))
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        teams = pd.concat([df["HomeTeam"], df["AwayTeam"]]).unique()
        dim_team = pd.DataFrame({"team_name": teams})
        dim_season = pd.DataFrame({"season_code": df["season_code"].unique()})
        dim_division = pd.DataFrame({"division_name": df["division"].unique()})
        
        dim_league = pd.DataFrame({"league_name": df["league_name"].unique()})

        
def __main__():

    logger.info("Starting data load process")

    conn = db.get_db_connection()
    logger.info("Database connection established")
    cursor = conn.cursor()
    entry_folder = Path(__file__).resolve().parents[1] / "data" / "proc"
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

        copy_query = sql.SQL(
            "COPY {}.{} FROM STDIN WITH (FORMAT csv, HEADER true)"
        ).format(sql.Identifier(schema_name), sql.Identifier(table_name))

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
    
    
if __name__ == "__main__":
    __main__()