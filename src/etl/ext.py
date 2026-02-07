#!/usr/bin/env python3
import yfinance as yf
import pandas as pd
from pathlib import Path



# Lista de activos
stocks = ["AAPL", "TSLA", "MSFT"]
forex = ["EURUSD=X", "GBPUSD=X", "JPY=X"]
crypto = ["BTC-USD", "ETH-USD"]

def fetch_and_save(symbols, interval="1d", asset_type="stocks",output_folder=Path(__file__).resolve().parents[1] / "data" / "entry"):
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
    

    # Lista de activos
    stocks = ["AAPL", "TSLA", "MSFT"]
    forex = ["EURUSD=X", "GBPUSD=X", "JPY=X"]
    crypto = ["BTC-USD", "ETH-USD"]
    output_folder=Path(__file__).resolve().parents[1] / "data" / "entry"
    print(f"Guardando datos en: {output_folder}")
    
    # Ejecutar ETL
    fetch_and_save(stocks, interval="1d", asset_type="stocks")
    fetch_and_save(forex, interval="1d", asset_type="forex")
    fetch_and_save(crypto, interval="1d", asset_type="crypto")

if __name__ == "__main__":
    __main__()