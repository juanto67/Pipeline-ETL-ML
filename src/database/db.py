#!/usr/bin/env python3
import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

def get_db_connection():
    db_host = os.getenv("DB_HOST", "localhost")

    conn = psycopg2.connect(
        host="localhost",
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "db_etl"),
        user=os.getenv("POSTGRES_USER", "etl_user"),
        password=os.getenv("POSTGRES_PASSWORD", "etl_pass"),
    )
    return conn