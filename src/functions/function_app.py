import logging
import azure.functions as func
import yfinance as yf
import pandas as pd
import datetime
import os
from azure.storage.filedatalake import DataLakeServiceClient

app = func.FunctionApp()

@app.timer_trigger(schedule="0 0 22 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def get_stock_data(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('Timer opóźniony!')

    assets = {
        "NASDAQ": "^IXIC",
        "SP500": "^GSPC",
        "EURUSD": "EURUSD=X",
        "USDPLN": "USDPLN=X"
    }

    try:
        service_client = DataLakeServiceClient.from_connection_string(os.environ["DATALAKE_CONNECTION_STRING"])
        file_system_client = service_client.get_file_system_client(file_system="datalake")

    except Exception as e:
        logging.error(f"No connection with Data Lake: {e}")
        return
    
    today_str = datetime.datetime.now().strftime("%Y/%m/%d")

    for folder_name, ticker_symbol in assets.items():
        logging.info(f"{folder_name} and {ticker_symbol}")
        history_path = f"bronze/yfinance/{folder_name}/history_full.json"
        file_client_history = file_system_client.get_file_client(history_path)

        try:
            file_client_history.get_file_properties()
            logging.info(f"{folder_name} Historic Data found")
            data = yf.download(ticker_symbol, period="1d")
            path = f"bronze/yfinance/{folder_name}/daily/{today_str}/data.json"

        except Exception:
            logging.info(f"{folder_name} Historic Data not found")
            data = yf.download(ticker_symbol, period="20y")
            path = history_path
        
        data.reset_index(inplace=True)

        if 'Date' in data.columns:
            data['Date'] = data['Date'].astype(str)
        elif 'Datetime' in data.columns:
            data['Datetime'] = data['Datetime'].astype(str)

        json_data = data.to_json(orient="records")
        target_file_client = file_system_client.get_file_client(path)
        target_file_client.upload_data(json_data, overwrite=True)