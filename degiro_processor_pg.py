#!/usr/bin/env python3
"""
PostgreSQL-Compatible DeGiro Data Processor
Processes DeGiro CSV files and stores data in PostgreSQL database
"""

import asyncio
import logging
import re
import time
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from database_models import db_manager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DeGiroProcessorPG:
    def __init__(self):
        self.finnhub_api_key = "cq8fj91r01qhgeb9c7s0cq8fj91r01qhgeb9c7sg"

    def load_degiro_data(self, csv_content: str) -> pd.DataFrame:
        """Load and clean DeGiro CSV data"""
        try:
            # Parse CSV content
            from io import StringIO

            df = pd.read_csv(StringIO(csv_content))
            
            # Debug: Log column names
            logger.info(f"CSV columns found: {list(df.columns)}")

            # Drop unnecessary columns
            cols_to_drop = ["Fecha valor", "ID Orden", "Tipo"]
            columns_to_drop = [col for col in cols_to_drop if col in df.columns]
            if columns_to_drop:
                df.drop(columns=columns_to_drop, inplace=True)

            # Clean data - BEFORE renaming (like in generate_datasets.py)
            df.dropna(subset=["Fecha"], inplace=True)
            df["Fecha"] = pd.to_datetime(df["Fecha"], dayfirst=True)
            df["year_month"] = df["Fecha"].dt.strftime("%Y-%m")
            df["year"] = df["Fecha"].dt.year

            # Rename columns (like in generate_datasets.py)
            df = df.rename(
                columns={
                    "Fecha": "date",
                    "Hora": "hour",
                    "Producto": "product",
                    "Descripción": "original_description",
                }
            )
            
            # Process amount and balance columns (exactly like generate_datasets.py)
            df["amount"] = (df["Unnamed: 8"].astype(str) + " " + df["Variación"]).fillna("0 EUR")
            df["balance"] = (df["Unnamed: 10"].astype(str) + " " + df["Saldo"]).fillna("0 EUR")

            # Drop original columns
            drop_cols = ["Unnamed: 8", "Unnamed: 10", "Variación", "Saldo"]
            existing_cols = [col for col in drop_cols if col in df.columns]
            if existing_cols:
                df.drop(columns=existing_cols, inplace=True)

            # Extract currency information (exactly like generate_datasets.py)
            df[["amount", "amount_currency"]] = df["amount"].str.extract(r"([\d\.,\-]+)\s*(EUR|USD|GBP)")
            df[["balance", "balance_currency"]] = df["balance"].str.extract(r"([\d\.,\-]+)\s*(EUR|USD|GBP)")

            # Convert to numeric (exactly like generate_datasets.py)
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
            df["balance"] = pd.to_numeric(df["balance"], errors="coerce")
            
            # For compatibility with PostgreSQL processor, also create amount_EUR
            df["amount_EUR"] = df["amount"]
            df["balance_EUR"] = df["balance"]

            # Clean product names and extract ISIN
            if 'product' in df.columns:
                df["ISIN"] = df["product"].str.extract(r"\(([A-Z]{2}[A-Z0-9]{9}[0-9])\)")
                df["product"] = df["product"].str.replace(r"\s*\([A-Z]{2}[A-Z0-9]{9}[0-9]\)", "", regex=True)
            else:
                df["ISIN"] = None

            logger.info(f"Loaded {len(df)} records from DeGiro CSV")
            return df

        except Exception as e:
            logger.error(f"Error loading DeGiro data: {e}")
            raise

    def categorize_transactions(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Categorize transactions into different types"""

        # Clean and normalize descriptions
        df["description"] = df["original_description"].str.lower().str.strip()

        # Initialize results
        results = {
            "buys": pd.DataFrame(),
            "sells": pd.DataFrame(),
            "dividends": pd.DataFrame(),
            "deposits": pd.DataFrame(),
            "fees": pd.DataFrame(),
        }

        # Define patterns for each category
        buy_patterns = [r"compra", r"buy", r"escisión.*compra"]

        sell_patterns = [r"venta", r"sell"]

        dividend_patterns = [r"dividend", r"dividendo", r"div\.", r"distribution"]

        deposit_patterns = [r"depósito", r"deposit", r"transferencia", r"transfer"]

        fee_patterns = [r"comisión", r"commission", r"fee", r"cargo", r"coste", r"cost"]

        # Categorize transactions
        for category, patterns in [
            ("buys", buy_patterns),
            ("sells", sell_patterns),
            ("dividends", dividend_patterns),
            ("deposits", deposit_patterns),
            ("fees", fee_patterns),
        ]:
            mask = df["description"].str.contains("|".join(patterns), regex=True, na=False)
            category_df = df[mask].copy()

            if not category_df.empty:
                # Add category-specific processing
                if category in ["buys", "sells"]:
                    category_df = self._process_trades(category_df, category)
                elif category == "dividends":
                    category_df = self._process_dividends(category_df)
                elif category == "deposits":
                    category_df = self._process_deposits(category_df)
                elif category == "fees":
                    category_df = self._process_fees(category_df)

                results[category] = category_df
                logger.info(f"Found {len(category_df)} {category} transactions")

        return results

    def _process_trades(self, df: pd.DataFrame, trade_type: str) -> pd.DataFrame:
        """Process buy/sell transactions to extract shares and price"""
        df = df.copy()

        # Extract shares from description
        # Pattern: "Compra 4 Procte" or "compra 1 united"
        shares_pattern = r"(?:compra|buy|venta|sell)\s+(\d+)"
        
        shares_match = df["original_description"].str.extract(shares_pattern, flags=re.IGNORECASE)
        df["shares"] = pd.to_numeric(shares_match[0], errors="coerce").fillna(0).astype(int)
        
        # Calculate price from amount and shares (amount_EUR / shares)
        df["price"] = 0.0
        valid_shares_mask = df["shares"] > 0
        if valid_shares_mask.any():
            df.loc[valid_shares_mask, "price"] = abs(df.loc[valid_shares_mask, "amount_EUR"]) / df.loc[valid_shares_mask, "shares"]

        # Add validation
        df["is_valid"] = (df["shares"] > 0) & (abs(df["amount_EUR"]) > 0)

        # Add category
        df["category"] = trade_type.rstrip("s")  # 'buys' -> 'buy', 'sells' -> 'sell'

        return df

    def _process_dividends(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process dividend transactions"""
        df = df.copy()
        df["shares"] = 0
        df["price"] = 0.0
        df["is_valid"] = df["amount_EUR"] != 0
        df["category"] = "dividend"
        return df

    def _process_deposits(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process deposit transactions"""
        df = df.copy()
        df["shares"] = 0
        df["price"] = 0.0
        df["is_valid"] = df["amount_EUR"] > 0
        df["category"] = "deposit"
        return df

    def _process_fees(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process fee transactions"""
        df = df.copy()
        df["shares"] = 0
        df["price"] = 0.0
        df["is_valid"] = df["amount_EUR"] < 0
        df["category"] = "fee"
        return df

    def get_current_prices(self, symbols: List[str]) -> Dict[str, Dict]:
        """Get current stock prices from Finnhub API"""
        prices = {}

        for symbol in symbols:
            try:
                if pd.isna(symbol) or not symbol:
                    continue

                url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={self.finnhub_api_key}"
                response = requests.get(url, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    if "c" in data and data["c"] is not None:
                        prices[symbol] = {
                            "current_price": data["c"],
                            "currency": "USD",  # Most symbols are USD
                            "source": "finnhub",
                            "fetch_timestamp": datetime.now(),
                        }

                time.sleep(0.1)  # Rate limiting

            except Exception as e:
                logger.warning(f"Failed to get price for {symbol}: {e}")
                continue

        logger.info(f"Retrieved prices for {len(prices)} symbols")
        return prices

    def calculate_holdings(self, transactions: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Calculate current holdings from transaction data"""
        holdings = []

        # Get all unique ISINs from buy transactions
        if not transactions["buys"].empty:
            buys_df = transactions["buys"].copy()
            
            # Extract ISIN from description if ISIN column is empty
            if buys_df["ISIN"].isnull().all():
                # Pattern: "....(US1234567890)" - ISIN in parentheses
                isin_pattern = r"\(([A-Z]{2}[A-Z0-9]{10})\)"
                buys_df["ISIN"] = buys_df["original_description"].str.extract(isin_pattern)[0]
                logger.info(f"Extracted ISIN from descriptions: {buys_df['ISIN'].notna().sum()} valid ISINs found")
            
            # Group by ISIN and product (skip rows without ISIN)
            valid_buys = buys_df[buys_df["ISIN"].notna()]
            
            if valid_buys.empty:
                logger.warning("No valid buys with ISIN found")
                return pd.DataFrame()
            
            # Group by ISIN only to avoid duplicates (same ISIN can have different product names)
            unique_holdings = (
                valid_buys
                .groupby(["ISIN"])
                .agg({
                    "shares": "sum", 
                    "amount_EUR": "sum",
                    "product": "first"  # Take first product name for the ISIN
                })
                .reset_index()
            )
            
            logger.info(f"Grouped holdings: {len(unique_holdings)} unique ISINs")

            # Subtract sells
            if not transactions["sells"].empty:
                sells_summary = transactions["sells"].groupby(["ISIN"]).agg({"shares": "sum"}).reset_index()

                unique_holdings = unique_holdings.merge(sells_summary, on="ISIN", how="left", suffixes=("", "_sold"))
                unique_holdings["shares_sold"] = unique_holdings["shares_sold"].fillna(0)
                unique_holdings["shares"] = unique_holdings["shares"] - unique_holdings["shares_sold"]

            # Filter out zero holdings
            unique_holdings = unique_holdings[unique_holdings["shares"] > 0].copy()

            if not unique_holdings.empty:
                # Extract symbols (simplified - you might need better symbol extraction)
                unique_holdings["symbol"] = unique_holdings["product"].str.extract(r"([A-Z]{1,5})")[0]

                # Get current prices
                symbols = unique_holdings["symbol"].dropna().unique().tolist()
                current_prices = self.get_current_prices(symbols)

                # Build holdings dataframe
                for _, row in unique_holdings.iterrows():
                    symbol = row["symbol"]
                    price_data = current_prices.get(symbol, {})

                    holdings.append(
                        {
                            "isin": row["ISIN"],
                            "company_name": row["product"],
                            "symbol": symbol,
                            "shares_held": int(row["shares"]),
                            "current_price": price_data.get("current_price"),
                            "currency": price_data.get("currency", "EUR"),
                            "position_value": (
                                (row["shares"] * price_data.get("current_price", 0))
                                if price_data.get("current_price")
                                else 0
                            ),
                            "fetch_date": date.today(),
                            "fetch_timestamp": price_data.get("fetch_timestamp"),
                            "source": price_data.get("source", "unknown"),
                        }
                    )

        holdings_df = pd.DataFrame(holdings)
        logger.info(f"Calculated {len(holdings_df)} current holdings")
        return holdings_df

    async def process_and_store(self, user_id: str, csv_content: str) -> Dict:
        """Main processing function - load CSV and store in PostgreSQL"""
        try:
            # Load and categorize data
            df = self.load_degiro_data(csv_content)
            transactions = self.categorize_transactions(df)

            # Calculate holdings
            holdings_df = self.calculate_holdings(transactions)

            # Store in database
            await db_manager.create_or_get_user(user_id)

            # Store raw data
            await db_manager.store_raw_data(user_id, f"account_{datetime.now().strftime('%Y%m%d')}.csv", csv_content)

            # Store categorized transactions
            transaction_counts = {}
            for trans_type, trans_df in transactions.items():
                if not trans_df.empty:
                    await db_manager.store_transactions(user_id, trans_df, trans_type.rstrip("s"))
                    transaction_counts[trans_type] = len(trans_df)

            # Store holdings
            holdings_count = 0
            if not holdings_df.empty:
                await db_manager.store_holdings(user_id, holdings_df)
                holdings_count = len(holdings_df)

            # Calculate summary
            total_transactions = sum(transaction_counts.values())

            result = {
                "success": True,
                "user_id": user_id,
                "transactions_count": total_transactions,
                "holdings_count": holdings_count,
                "transaction_breakdown": transaction_counts,
                "processing_timestamp": datetime.now().isoformat(),
            }

            logger.info(
                f"Successfully processed data for user {user_id}: {total_transactions} transactions, {holdings_count} holdings"
            )
            return result

        except Exception as e:
            logger.error(f"Error processing data for user {user_id}: {e}")
            raise


# Global processor instance
degiro_processor = DeGiroProcessorPG()
