import logging
import time
import datetime
import os
import azure.functions as func
import yfinance as yf
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()


ASSETS = {                          
    "NASDAQ": "^IXIC",
    "SP500":  "^GSPC",
    "EURUSD": "EURUSD=X",
    "USDPLN": "USDPLN=X",
}
START_DATE  = "2005-01-01"          
FILE_SYSTEM = "datalake"            
MAX_RETRIES = 3                     


@app.timer_trigger(schedule="0 0 22 * * *", arg_name="myTimer",
                   run_on_startup=False, use_monitor=True)
def get_stock_data(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.warning("Timer delayed")

    try:
        service_client = DataLakeServiceClient.from_connection_string(
            os.environ["DATALAKE_CONNECTION_STRING"])     
        fs_client = service_client.get_file_system_client(FILE_SYSTEM)
    except KeyError:
        logging.error("No environmental variable DATALAKE_CONNECTION_STRING.")
        return
    except Exception as e:
        logging.error(f"No connection to datalake: {e}")
        return

    ingest_date = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    ingest_ts   = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for folder_name, ticker_symbol in ASSETS.items():
        try:
            df = download_full_history(ticker_symbol)
        except Exception as e:
            logging.error(f"[{folder_name}] The download errored: {e}")
            continue                                       

        if df is None or df.empty:
            logging.warning(f"[{folder_name}] Empty")
            continue

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)   

        df = df.reset_index()                              

        date_col = "Date" if "Date" in df.columns else "Datetime"
        df[date_col] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")

        df["_ticker"] = ticker_symbol
        df["_ingested_at_utc"] = ingest_ts

        json_data = df.to_json(orient="records")           

        path = f"bronze/yfinance/{folder_name}/ingest_date={ingest_date}/data.json"
        fs_client.get_file_client(path).upload_data(json_data, overwrite=True)
        logging.info(f"[{folder_name}] Saved {len(df)} rows -> {path}")


def download_full_history(ticker_symbol: str):
    for attempt in range(1, MAX_RETRIES + 1):
        df = yf.download(ticker_symbol, start=START_DATE, auto_adjust=True, progress=False)
        if df is not None and not df.empty:
            return df
        wait = 2 ** attempt                                  
        logging.warning(f"[{ticker_symbol}] Empty (attempt {attempt}/{MAX_RETRIES}), waiting {wait}s.")
        time.sleep(wait)
    return None
