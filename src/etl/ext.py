#!/usr/bin/env python3
import yfinance as yf
import pandas as pd
from pathlib import Path
import os
import logging
#Configuration of logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)
# Fetch data for a list of symbols and save to CSV
def fetch_and_save(symbols, asset_type, output_folder, interval="1d"):
    filename = output_folder / f"{asset_type}.csv"

    logger.info("Starting %s fetch (%s symbols)", asset_type, len(symbols))

    for symbol in symbols:
        logger.info("Fetching %s", symbol)
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="60d", interval=interval)
        except Exception:
            logger.exception("Failed to fetch %s", symbol)
            continue
        if data.empty:
            logger.warning("No data for %s", symbol)
            continue
        #Normalize the date and ensure it's in the correct format
        data = data.reset_index().rename(columns={"index": "Date"})
        data["Date"] = pd.to_datetime(data["Date"], errors="coerce", utc=True).dt.normalize()
        data = data.dropna(subset=["Date"])
        #Add symbol column for later merging
        data["Symbol"] = symbol
        #Combine with existing data if file exists, otherwise use new data
        if os.path.exists(filename):
            existing_data = pd.read_csv(filename, parse_dates=["Date"])
            existing_data["Date"] = pd.to_datetime(existing_data["Date"], errors="coerce", utc=True).dt.normalize()
            combined = pd.concat([existing_data, data], ignore_index=True)
        else:
            combined = data
        #Remove duplicates, keeping the most recent entry for each date and symbol
        combined = combined.drop_duplicates(subset=["Date", "Symbol"], keep="last")
        combined = combined.sort_values(["Date", "Symbol"]).reset_index(drop=True)
        combined["Date"] = pd.to_datetime(combined["Date"], errors="coerce", utc=True)
        combined["Date"] = combined["Date"].dt.tz_localize(None).dt.strftime("%Y-%m-%d")
        combined.to_csv(filename, index=False)
        logger.info("Saved %s rows to %s", len(combined), filename)

def __main__():
    # symbols to fetch
    stocks = ["AAPL", "TSLA", "MSFT"]
    forex = ["EURUSD=X", "GBPUSD=X", "JPY=X"]
    crypto = ["BTC-USD", "ETH-USD"]
    output_folder = Path(__file__).resolve().parents[1] / "data" / "entry"
    logger.info("Output folder: %s", output_folder)
    # Run ETL
    fetch_and_save(stocks, asset_type="stocks", output_folder=output_folder, interval="1d")
    fetch_and_save(forex, asset_type="forex", output_folder=output_folder, interval="1d")
    fetch_and_save(crypto, asset_type="crypto", output_folder=output_folder, interval="1d")

if __name__ == "__main__":
    __main__()