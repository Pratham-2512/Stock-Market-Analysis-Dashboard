# Import necessary libraries
import yfinance as yf
import pandas as pd
import json
import logging
from datetime import datetime, timedelta

# --- Configuration for Logging ---
# Set up a more advanced logging configuration to log messages to both a file and the console.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stock_data_pipeline.log"),  # Saves logs to a file
        logging.StreamHandler()  # Prints logs to the console
    ]
)

# --- Configuration for Data Source ---
# This is the path to the JSON file that will contain your stock tickers and sectors.
CONFIG_FILE = "stocks_config.json"


# --- Functions ---
def load_config(file_path):
    """
    Loads stock ticker and sector mapping from a JSON file.

    Args:
        file_path (str): The path to the JSON configuration file.

    Returns:
        dict: A dictionary containing stock tickers and their sectors, or None if an error occurs.
    """
    try:
        with open(file_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file '{file_path}' not found. Please create it.")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from '{file_path}'. Please check file format.")
        return None


def download_and_process_stock(ticker, sector, start_date, end_date):
    """
    Downloads historical stock data, adds sector info, and calculates SMAs, EMAs, and volatility.

    Args:
        ticker (str): The stock ticker symbol.
        sector (str): The sector of the stock.
        start_date (datetime): The start date for the data.
        end_date (datetime): The end date for the data.

    Returns:
        pd.DataFrame: A processed DataFrame for the stock, or None if an error occurs.
    """
    try:
        logging.info(f"Downloading data for {ticker}...")
        stock_data = yf.download(ticker, start=start_date, end=end_date)

        if not stock_data.empty:
            # Add 'Stock Ticker' and 'Sector' columns
            stock_data['Stock Ticker'] = ticker
            stock_data['Sector'] = sector

            # --- Add new metrics ---
            # Calculate Daily Returns and Volatility (30-day standard deviation)
            stock_data['Daily_Return'] = stock_data['Close'].pct_change()
            stock_data['Volatility'] = stock_data['Daily_Return'].rolling(window=30).std()

            # Calculate Simple Moving Averages (SMA) for 50 and 200 days
            stock_data['SMA_50'] = stock_data['Close'].rolling(window=50).mean()
            stock_data['SMA_200'] = stock_data['Close'].rolling(window=200).mean()

            # Calculate Exponential Moving Average (EMA) for 50 days
            stock_data['EMA_50'] = stock_data['Close'].ewm(span=50, adjust=False).mean()

            # Reset the index to make 'Date' a column for Power BI
            stock_data.reset_index(inplace=True)

            logging.info(f"Successfully processed data for {ticker}.")
            return stock_data
        else:
            logging.warning(f"No data found for {ticker}. Skipping.")
            return None
    except Exception as e:
        logging.error(f"Error downloading data for {ticker}: {e}")
        return None


# --- Main execution block ---
if __name__ == "__main__":
    # Define the date range for the data download (e.g., last 3 years)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 3)

    # First, let's make sure the configuration file exists.
    # Create a simple JSON file named 'stocks_config.json' with your tickers and sectors.
    # This makes the script dynamic and easy to update.
    # Example content for 'stocks_config.json':
    # {
    #     "RELIANCE.NS": "Energy",
    #     "TCS.NS": "Information Technology",
    #     "HDFCBANK.NS": "Financial Services"
    # }

    stocks_config = load_config(CONFIG_FILE)

    if stocks_config:
        all_stocks_df = []
        logging.info("Starting data download...")

        # Loop through the stocks from the loaded configuration
        for ticker, sector in stocks_config.items():
            stock_df = download_and_process_stock(ticker, sector, start_date, end_date)
            if stock_df is not None:
                all_stocks_df.append(stock_df)

        if all_stocks_df:
            # Concatenate all individual stock dataframes into one large dataframe
            final_df = pd.concat(all_stocks_df, ignore_index=True)

            # Save the combined data to a single CSV file
            final_df.to_csv("stock_data_with_metrics.csv", index=False)
            logging.info("\nSuccessfully saved all stock data with metrics to 'stock_data_with_metrics.csv'.")
        else:
            logging.warning(
                "\nNo stock data was successfully downloaded. Please check your configuration and internet connection.")

    logging.info("\nData preparation complete!")
