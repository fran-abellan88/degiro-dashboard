#!/usr/bin/env python3
"""
Stock Data Manager
Handles stock price data storage and retrieval using SQLite
"""

import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class StockDataManager:
    def __init__(self, db_path="stock_data.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize the SQLite database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    date DATE NOT NULL,
                    close_price REAL NOT NULL,
                    open_price REAL,
                    high_price REAL,
                    low_price REAL,
                    volume INTEGER,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date)
                )
            """
            )

            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_symbol_date 
                ON stock_prices (symbol, date)
            """
            )

            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS data_updates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    last_update_date DATE NOT NULL,
                    symbols_updated TEXT NOT NULL,
                    records_added INTEGER NOT NULL,
                    update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            conn.commit()
            logger.info("Database initialized successfully")

    def get_portfolio_symbols(self):
        """Get list of symbols from current portfolio"""
        try:
            df_stocks = pd.read_csv("output/current_stock_values.csv")
            symbols = df_stocks["symbol"].dropna().unique().tolist()
            logger.info(f"Found {len(symbols)} symbols in portfolio")
            return symbols
        except Exception as e:
            logger.error(f"Error reading portfolio symbols: {e}")
            return []

    def get_last_update_date(self):
        """Get the last date we have data for"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT MAX(date) FROM stock_prices
            """
            )
            result = cursor.fetchone()[0]

            if result:
                return pd.to_datetime(result).date()
            else:
                # If no data exists, start from 2015
                return datetime(2015, 1, 1).date()

    def download_stock_data(self, symbols, start_date, end_date):
        """Download stock data using yfinance"""
        logger.info(f"Downloading data for {len(symbols)} symbols from {start_date} to {end_date}")

        try:
            # Download all stocks at once for efficiency
            data = yf.download(symbols, start=start_date, end=end_date, progress=False)

            if data.empty:
                logger.warning("No data downloaded")
                return pd.DataFrame()

            # Process the data based on whether we have single or multiple stocks
            if len(symbols) == 1:
                # Single stock case
                data_processed = data.reset_index()
                data_processed["symbol"] = symbols[0]
                data_processed = data_processed.rename(
                    columns={
                        "Date": "date",
                        "Close": "close_price",
                        "Open": "open_price",
                        "High": "high_price",
                        "Low": "low_price",
                        "Volume": "volume",
                    }
                )
            else:
                # Multiple stocks case - handle MultiIndex columns
                processed_dfs = []

                for symbol in symbols:
                    try:
                        symbol_data = data.xs(symbol, level=1, axis=1)
                        symbol_df = symbol_data.reset_index()
                        symbol_df["symbol"] = symbol
                        symbol_df = symbol_df.rename(
                            columns={
                                "Date": "date",
                                "Close": "close_price",
                                "Open": "open_price",
                                "High": "high_price",
                                "Low": "low_price",
                                "Volume": "volume",
                            }
                        )
                        processed_dfs.append(symbol_df)
                    except KeyError:
                        logger.warning(f"No data found for symbol {symbol}")
                        continue

                if processed_dfs:
                    data_processed = pd.concat(processed_dfs, ignore_index=True)
                else:
                    data_processed = pd.DataFrame()

            # Clean data
            data_processed = data_processed.dropna(subset=["close_price"])
            data_processed["date"] = pd.to_datetime(data_processed["date"]).dt.date
            data_processed["last_updated"] = datetime.now()

            logger.info(f"Processed {len(data_processed)} price records")
            return data_processed

        except Exception as e:
            logger.error(f"Error downloading stock data: {e}")
            return pd.DataFrame()

    def store_stock_data(self, data_df):
        """Store stock data in the database in chunks to handle large datasets"""
        if data_df.empty:
            logger.warning("No data to store")
            return 0

        # Process in chunks to avoid SQLite variable limits
        chunk_size = 10000
        total_records = 0

        with sqlite3.connect(self.db_path) as conn:
            try:
                for i in range(0, len(data_df), chunk_size):
                    chunk = data_df.iloc[i : i + chunk_size]

                    # Store chunk
                    chunk.to_sql("stock_prices", conn, if_exists="append", index=False, chunksize=1000)

                    total_records += len(chunk)
                    logger.info(
                        f"Stored chunk {i//chunk_size + 1}: {len(chunk)} records ({total_records}/{len(data_df)} total)"
                    )

                # Remove duplicates that might have been created
                logger.info("Removing duplicates...")
                conn.execute(
                    """
                    DELETE FROM stock_prices 
                    WHERE id NOT IN (
                        SELECT MIN(id) 
                        FROM stock_prices 
                        GROUP BY symbol, date
                    )
                """
                )

                conn.commit()
                logger.info(f"Successfully stored {total_records} records in database")
                return total_records

            except Exception as e:
                logger.error(f"Error storing data: {e}")
                return 0

    def log_update(self, symbols, records_added):
        """Log the update operation"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO data_updates (last_update_date, symbols_updated, records_added)
                VALUES (?, ?, ?)
            """,
                (datetime.now().date(), ",".join(symbols), records_added),
            )
            conn.commit()

    def initial_load(self):
        """Perform initial data load with 10+ years of history (from 2015)"""
        logger.info("Starting initial data load...")

        symbols = self.get_portfolio_symbols()
        if not symbols:
            logger.error("No symbols found in portfolio")
            return

        # Load data from 2015 for comprehensive historical analysis
        end_date = datetime.now().date()
        start_date = datetime(2015, 1, 1).date()  # From January 1, 2015

        data_df = self.download_stock_data(symbols, start_date, end_date)
        records_added = self.store_stock_data(data_df)

        if records_added > 0:
            self.log_update(symbols, records_added)
            logger.info(f"Initial load completed: {records_added} records added")
        else:
            logger.error("Initial load failed")

    def incremental_update(self):
        """Perform incremental update from last update date to today"""
        logger.info("Starting incremental update...")

        symbols = self.get_portfolio_symbols()
        if not symbols:
            logger.error("No symbols found in portfolio")
            return

        last_date = self.get_last_update_date()
        end_date = datetime.now().date()

        # Start from the day after last update
        start_date = last_date + timedelta(days=1)

        if start_date >= end_date:
            logger.info("Data is already up to date")
            return

        logger.info(f"Updating data from {start_date} to {end_date}")

        data_df = self.download_stock_data(symbols, start_date, end_date)
        records_added = self.store_stock_data(data_df)

        if records_added > 0:
            self.log_update(symbols, records_added)
            logger.info(f"Incremental update completed: {records_added} records added")
        else:
            logger.info("No new data to add")

    def get_stock_data(self, symbol, days=None):
        """Retrieve stock data for a specific symbol"""
        with sqlite3.connect(self.db_path) as conn:
            if days:
                cutoff_date = datetime.now().date() - timedelta(days=days)
                query = """
                    SELECT date, close_price, open_price, high_price, low_price, volume
                    FROM stock_prices 
                    WHERE symbol = ? AND date >= ?
                    ORDER BY date
                """
                df = pd.read_sql_query(query, conn, params=[symbol, cutoff_date])
            else:
                query = """
                    SELECT date, close_price, open_price, high_price, low_price, volume
                    FROM stock_prices 
                    WHERE symbol = ?
                    ORDER BY date
                """
                df = pd.read_sql_query(query, conn, params=[symbol])

            return df

    def get_available_symbols(self):
        """Get list of symbols we have data for"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT DISTINCT symbol 
                FROM stock_prices 
                ORDER BY symbol
            """
            )
            return [row[0] for row in cursor.fetchall()]

    def get_database_stats(self):
        """Get statistics about the database"""
        with sqlite3.connect(self.db_path) as conn:
            stats = {}

            # Total records
            cursor = conn.execute("SELECT COUNT(*) FROM stock_prices")
            stats["total_records"] = cursor.fetchone()[0]

            # Number of symbols
            cursor = conn.execute("SELECT COUNT(DISTINCT symbol) FROM stock_prices")
            stats["total_symbols"] = cursor.fetchone()[0]

            # Date range
            cursor = conn.execute("SELECT MIN(date), MAX(date) FROM stock_prices")
            date_range = cursor.fetchone()
            stats["date_range"] = {"start": date_range[0], "end": date_range[1]}

            # Last update
            cursor = conn.execute("SELECT MAX(update_timestamp) FROM data_updates")
            stats["last_update"] = cursor.fetchone()[0]

            return stats


def main():
    """Main function for command line usage"""
    import argparse

    parser = argparse.ArgumentParser(description="Stock Data Manager")
    parser.add_argument("--initial-load", action="store_true", help="Perform initial data load")
    parser.add_argument("--update", action="store_true", help="Perform incremental update")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")

    args = parser.parse_args()

    manager = StockDataManager()

    if args.initial_load:
        manager.initial_load()
    elif args.update:
        manager.incremental_update()
    elif args.stats:
        stats = manager.get_database_stats()
        print(f"\n📊 Database Statistics:")
        print(f"   Total records: {stats['total_records']:,}")
        print(f"   Total symbols: {stats['total_symbols']}")
        print(f"   Date range: {stats['date_range']['start']} to {stats['date_range']['end']}")
        print(f"   Last update: {stats['last_update']}")
    else:
        print("Use --initial-load, --update, or --stats")


if __name__ == "__main__":
    main()
