#!/usr/bin/env python3
"""
PostgreSQL Database Models for DeGiro Dashboard
Handles user portfolio data, transactions, and stock information
"""

import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import asyncpg
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.database_url = os.environ.get("DATABASE_URL")
        if not self.database_url:
            # Local development fallback
            self.database_url = "postgresql://localhost:5432/degiro_dashboard"
        self.pool = None

    async def init_pool(self):
        """Initialize the connection pool"""
        try:
            self.pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=10, command_timeout=60)
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise

    async def close_pool(self):
        """Close the connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")

    @asynccontextmanager
    async def get_connection(self):
        """Get a database connection from the pool"""
        if not self.pool:
            await self.init_pool()

        async with self.pool.acquire() as connection:
            yield connection

    async def create_tables(self):
        """Create all necessary database tables"""
        async with self.get_connection() as conn:
            # Users table for multi-user support
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_upload TIMESTAMP,
                    portfolio_name VARCHAR(255)
                )
            """
            )

            # Raw DeGiro data (for backup and reprocessing)
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS degiro_raw_data (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL REFERENCES users(user_id),
                    filename VARCHAR(255) NOT NULL,
                    file_data TEXT NOT NULL,
                    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Processed transactions
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL REFERENCES users(user_id),
                    date DATE NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    isin VARCHAR(12),
                    original_description TEXT,
                    description VARCHAR(255),
                    category VARCHAR(50),
                    country VARCHAR(3),
                    amount_eur DECIMAL(12,2),
                    is_valid BOOLEAN DEFAULT TRUE,
                    shares INTEGER,
                    price DECIMAL(10,4),
                    transaction_type VARCHAR(20) CHECK (transaction_type IN ('buy', 'sell', 'dividend', 'deposit', 'fee')),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Current holdings
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS holdings (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL REFERENCES users(user_id),
                    isin VARCHAR(12) NOT NULL,
                    company_name VARCHAR(255) NOT NULL,
                    symbol VARCHAR(10),
                    current_price DECIMAL(10,4),
                    currency VARCHAR(3),
                    shares_held INTEGER,
                    position_value DECIMAL(12,2),
                    fetch_date DATE,
                    fetch_timestamp TIMESTAMP,
                    source VARCHAR(50),
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, isin)
                )
            """
            )

            # Stock price history (shared across users)
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS stock_prices (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    close_price DECIMAL(10,4) NOT NULL,
                    open_price DECIMAL(10,4),
                    high_price DECIMAL(10,4),
                    low_price DECIMAL(10,4),
                    volume BIGINT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(symbol, date)
                )
            """
            )

            # Portfolio summary cache
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS portfolio_summary (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL REFERENCES users(user_id),
                    total_value DECIMAL(12,2),
                    cash_balance DECIMAL(12,2),
                    total_invested DECIMAL(12,2),
                    total_profit_loss DECIMAL(12,2),
                    last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id)
                )
            """
            )

            # Create indexes for better performance
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_user_date ON transactions(user_id, date)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_isin ON transactions(isin)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_holdings_user ON holdings(user_id)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_date ON stock_prices(symbol, date)")

            logger.info("Database tables created successfully")

    async def store_raw_data(self, user_id: str, filename: str, file_content: str):
        """Store raw uploaded file data"""
        async with self.get_connection() as conn:
            await conn.execute(
                """
                INSERT INTO degiro_raw_data (user_id, filename, file_data)
                VALUES ($1, $2, $3)
            """,
                user_id,
                filename,
                file_content,
            )

    async def store_transactions(self, user_id: str, transactions_df: pd.DataFrame, transaction_type: str):
        """Store processed transactions"""
        async with self.get_connection() as conn:
            # Clear existing transactions of this type for the user
            await conn.execute(
                """
                DELETE FROM transactions 
                WHERE user_id = $1 AND transaction_type = $2
            """,
                user_id,
                transaction_type,
            )

            # Insert new transactions
            for _, row in transactions_df.iterrows():
                # Helper function to safely get string values
                def safe_str(value, default=""):
                    if pd.isna(value) or value is None:
                        return default
                    return str(value)
                
                await conn.execute(
                    """
                    INSERT INTO transactions 
                    (user_id, date, product, isin, original_description, description, 
                     category, country, amount_eur, is_valid, shares, price, transaction_type)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """,
                    user_id,
                    pd.to_datetime(row["date"]).date() if pd.notna(row["date"]) else None,
                    safe_str(row.get("product")),
                    safe_str(row.get("ISIN")),
                    safe_str(row.get("original_description")),
                    safe_str(row.get("description")),
                    safe_str(row.get("category")),
                    safe_str(row.get("country")),
                    float(row["amount_EUR"]) if pd.notna(row["amount_EUR"]) else 0.0,
                    bool(row.get("is_valid", True)),
                    int(row["shares"]) if pd.notna(row["shares"]) else 0,
                    float(row["price"]) if pd.notna(row["price"]) else 0.0,
                    transaction_type,
                )

    async def store_holdings(self, user_id: str, holdings_df: pd.DataFrame):
        """Store current holdings"""
        async with self.get_connection() as conn:
            # Clear existing holdings for the user
            await conn.execute("DELETE FROM holdings WHERE user_id = $1", user_id)

            # Insert new holdings
            for _, row in holdings_df.iterrows():
                # Helper function to safely get string values
                def safe_str(value, default=""):
                    if pd.isna(value) or value is None:
                        return default
                    return str(value)
                    
                await conn.execute(
                    """
                    INSERT INTO holdings 
                    (user_id, isin, company_name, symbol, current_price, currency, 
                     shares_held, position_value, fetch_date, fetch_timestamp, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                    user_id,
                    safe_str(row.get("isin")),
                    safe_str(row.get("company_name")),
                    safe_str(row.get("symbol")),
                    float(row["current_price"]) if pd.notna(row["current_price"]) else None,
                    safe_str(row.get("currency")),
                    int(row["shares_held"]) if pd.notna(row["shares_held"]) else 0,
                    float(row["position_value"]) if pd.notna(row["position_value"]) else 0.0,
                    pd.to_datetime(row["fetch_date"]).date() if pd.notna(row["fetch_date"]) else None,
                    pd.to_datetime(row["fetch_timestamp"]) if pd.notna(row["fetch_timestamp"]) else None,
                    safe_str(row.get("source")),
                )

    async def get_user_transactions(self, user_id: str, transaction_type: str = None) -> List[Dict]:
        """Get user transactions"""
        async with self.get_connection() as conn:
            if transaction_type:
                query = """
                    SELECT * FROM transactions 
                    WHERE user_id = $1 AND transaction_type = $2 
                    ORDER BY date DESC
                """
                rows = await conn.fetch(query, user_id, transaction_type)
            else:
                query = """
                    SELECT * FROM transactions 
                    WHERE user_id = $1 
                    ORDER BY date DESC
                """
                rows = await conn.fetch(query, user_id)

            return [dict(row) for row in rows]

    async def get_user_holdings(self, user_id: str) -> List[Dict]:
        """Get user holdings"""
        async with self.get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM holdings 
                WHERE user_id = $1 
                ORDER BY company_name
            """,
                user_id,
            )

            return [dict(row) for row in rows]

    async def get_stock_symbols(self, user_id: str) -> List[str]:
        """Get unique stock symbols for a user"""
        async with self.get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT symbol 
                FROM holdings 
                WHERE user_id = $1 AND symbol IS NOT NULL
            """,
                user_id,
            )

            return [row["symbol"] for row in rows]

    async def create_or_get_user(self, user_id: str) -> Dict:
        """Create or get user record"""
        async with self.get_connection() as conn:
            # Try to get existing user
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

            if row:
                return dict(row)
            else:
                # Create new user
                await conn.execute(
                    """
                    INSERT INTO users (user_id) VALUES ($1)
                """,
                    user_id,
                )

                row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
                return dict(row)

    async def get_all_users(self) -> List[Dict]:
        """Get all users"""
        async with self.get_connection() as conn:
            rows = await conn.fetch("SELECT * FROM users ORDER BY created_at DESC")
            return [dict(row) for row in rows]

    async def get_user_raw_data(self, user_id: str) -> List[Dict]:
        """Get raw CSV data for a user"""
        async with self.get_connection() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM degiro_raw_data 
                WHERE user_id = $1 
                ORDER BY upload_timestamp DESC
            """,
                user_id,
            )
            return [dict(row) for row in rows]

    async def clear_user_holdings(self, user_id: str):
        """Clear all holdings for a user"""
        async with self.get_connection() as conn:
            await conn.execute("DELETE FROM holdings WHERE user_id = $1", user_id)


# Global database manager instance
db_manager = DatabaseManager()
