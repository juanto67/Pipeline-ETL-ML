#!/usr/bin/env python3
import yfinance as yf
import pandas as pd
from pathlib import Path

# Carpeta donde guardar los CSV
output_folder = Path("/home/juanto/etl/Pipeline-ETL-ML/src/data/entry")

# Lista de activos
stocks = ["AAPL", "TSLA", "MSFT"]
forex = ["EURUSD=X", "GBPUSD=X", "JPY=X"]
crypto = ["BTC-USD", "ETH-USD"]

def fetch_and_save(symbols, interval="1d", asset_type="stocks"):
    filename = output_folder / f"{asset_type}.csv"
    for symbol in symbols:
        ticker = yf.Ticker(symbol)
        data = ticker.history(period="60d", interval=interval)
        data.index = data.index.date  # índice solo fecha
        data["Symbol"] = symbol

        if filename.exists():
            existing_data = pd.read_csv(filename)
            combined = pd.concat([existing_data, data.reset_index()])
            # Eliminar duplicados basados en 'index' y 'Symbol', manteniendo el último registro
            combined = combined.drop_duplicates(subset=["index", "Symbol"], keep="last")
            combined.to_csv(filename, index=False)
        else:
            data.reset_index().to_csv(filename, index=False)

def __main__():
    
    # Carpeta donde guardar los CSV
    output_folder = Path("/mnt/c/Users/juanm/etl/Pipeline-ETL-ML/src/data/entry")

    # Lista de activos
    stocks = ["AAPL", "TSLA", "MSFT"]
    forex = ["EURUSD=X", "GBPUSD=X", "JPY=X"]
    crypto = ["BTC-USD", "ETH-USD"]
    
    
    # Ejecutar ETL
    fetch_and_save(stocks, interval="1d", asset_type="stocks")
    fetch_and_save(forex, interval="1d", asset_type="forex")
    fetch_and_save(crypto, interval="1d", asset_type="crypto")
