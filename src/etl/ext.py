#!/usr/bin/env python3
import yfinance as yf
import pandas as pd
from pathlib import Path
import os

def fetch_and_save(symbols, asset_type, output_folder, interval="1d"):
    filename = output_folder / f"{asset_type}.csv"

    for symbol in symbols:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="60d", interval=interval)
        if data.empty:
            continue
        data = data.reset_index().rename(columns={"index": "Date"})
        data["Date"] = pd.to_datetime(data["Date"], errors="coerce", utc=True).dt.normalize()
        data = data.dropna(subset=["Date"])
        
        data["Symbol"] = symbol

        if os.path.exists(filename):
            existing_data = pd.read_csv(filename, parse_dates=["Date"])
            existing_data["Date"] = pd.to_datetime(existing_data["Date"], errors="coerce", utc=True).dt.normalize()
            combined = pd.concat([existing_data, data], ignore_index=True)
        else:
            combined = data

        combined = combined.drop_duplicates(subset=["Date", "Symbol"], keep="last")
        combined = combined.sort_values(["Date", "Symbol"]).reset_index(drop=True)
        combined["Date"] = pd.to_datetime(combined["Date"], errors="coerce", utc=True)
        combined["Date"] = combined["Date"].dt.tz_localize(None).dt.strftime("%Y-%m-%d")
        combined.to_csv(filename, index=False)

def __main__():
    # Lista de activos
    stocks = ["AAPL", "TSLA", "MSFT"]
    forex = ["EURUSD=X", "GBPUSD=X", "JPY=X"]
    crypto = ["BTC-USD", "ETH-USD"]
    output_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    
    # Ejecutar ETL
    fetch_and_save(stocks, asset_type="stocks", output_folder=output_folder, interval="1d")
    fetch_and_save(forex, asset_type="forex", output_folder=output_folder, interval="1d")
    fetch_and_save(crypto, asset_type="crypto", output_folder=output_folder, interval="1d")

if __name__ == "__main__":
    __main__()