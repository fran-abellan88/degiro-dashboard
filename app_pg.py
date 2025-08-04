#!/usr/bin/env python3
"""
DeGiro Dashboard - PostgreSQL Version
Unified Application for Render Deployment with PostgreSQL support
"""

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

from database_models import db_manager
from degiro_processor_pg import degiro_processor
from stock_data_manager import StockDataManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting DeGiro Dashboard with PostgreSQL support...")
    await db_manager.init_pool()
    await db_manager.create_tables()
    logger.info("Database initialized successfully")

    yield

    # Shutdown
    await db_manager.close_pool()
    logger.info("Database connections closed")


# Initialize FastAPI app
app = FastAPI(title="DeGiro Portfolio Dashboard", version="2.0.0", lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize stock data manager (for historical prices)
stock_manager = StockDataManager()


# Health check endpoint for Render
@app.get("/health")
async def health_check():
    """Health check endpoint for Render"""
    try:
        # Check PostgreSQL connection
        async with db_manager.get_connection() as conn:
            result = await conn.fetchval("SELECT COUNT(*) FROM users")
            user_count = result or 0

        # Check stock data
        stock_stats = stock_manager.get_database_stats() if Path("stock_data.db").exists() else {}

        return {
            "status": "healthy",
            "database_type": "postgresql",
            "user_count": user_count,
            "stock_records": stock_stats.get("total_records", 0),
            "stock_symbols": stock_stats.get("total_symbols", 0),
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "error": str(e)}


# Dashboard routes
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the main dashboard"""
    dashboard_file = Path("portfolio_dashboard.html")
    if dashboard_file.exists():
        return FileResponse("portfolio_dashboard.html")
    else:
        return HTMLResponse("Dashboard not found", status_code=404)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_redirect():
    """Redirect /dashboard to main page"""
    return await dashboard()


# File upload endpoint
@app.post("/api/upload-degiro-data")
async def upload_degiro_data(file: UploadFile = File(...), user_id: str = Form(...)):
    """Upload and process DeGiro CSV data"""
    try:
        # Validate file
        if not file.filename.lower().endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")

        # Read file content
        content = await file.read()
        csv_content = content.decode("utf-8")

        logger.info(f"Processing upload for user {user_id}, file: {file.filename}")

        # Process and store data
        result = await degiro_processor.process_and_store(user_id, csv_content)

        return JSONResponse(content=result)

    except Exception as e:
        logger.error(f"Upload processing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Portfolio status endpoint
@app.get("/api/portfolio-status/{user_id}")
async def get_portfolio_status(user_id: str):
    """Get portfolio status for a user"""
    try:
        # Check if user has data
        holdings = await db_manager.get_user_holdings(user_id)
        transactions = await db_manager.get_user_transactions(user_id)

        if not holdings and not transactions:
            return JSONResponse(content={"has_data": False}, status_code=404)

        return {
            "has_data": True,
            "holdings_count": len(holdings),
            "transactions_count": len(transactions),
            "last_updated": datetime.now().isoformat(),
        }

    except Exception as e:
        logger.error(f"Error getting portfolio status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Portfolio data endpoints
@app.get("/api/portfolio/{user_id}")
async def get_portfolio_data(user_id: str):
    """Get complete portfolio data for a user"""
    try:
        holdings = await db_manager.get_user_holdings(user_id)

        if not holdings:
            raise HTTPException(status_code=404, detail="No portfolio data found")

        # Calculate totals
        total_value = sum(h.get("position_value", 0) for h in holdings)

        # Get cash balance from deposits minus investments
        transactions = await db_manager.get_user_transactions(user_id)
        deposits = sum(t.get("amount_eur", 0) for t in transactions if t.get("transaction_type") == "deposit")
        investments = sum(abs(t.get("amount_eur", 0)) for t in transactions if t.get("transaction_type") == "buy")
        cash_balance = deposits - investments + total_value  # Rough calculation

        return {
            "portfolio_value": total_value,
            "cash_balance": max(0, cash_balance - total_value),  # Available cash
            "total_invested": investments,
            "holdings": holdings,
            "summary": {"total_positions": len(holdings), "last_updated": datetime.now().isoformat()},
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting portfolio data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Transactions endpoints
@app.get("/api/transactions/{user_id}")
async def get_user_transactions_api(user_id: str, transaction_type: str = None):
    """Get user transactions"""
    try:
        transactions = await db_manager.get_user_transactions(user_id, transaction_type)

        # Convert to the format expected by the frontend
        formatted_transactions = []
        for t in transactions:
            formatted_transactions.append(
                {
                    "date": t["date"].isoformat() if t["date"] else None,
                    "product": t["product"],
                    "description": t["original_description"],
                    "amount_EUR": float(t["amount_eur"]) if t["amount_eur"] else 0,
                    "shares": t["shares"],
                    "price": float(t["price"]) if t["price"] else 0,
                    "type": t["transaction_type"],
                    "category": t["category"],
                }
            )

        return {"transactions": formatted_transactions, "count": len(formatted_transactions)}

    except Exception as e:
        logger.error(f"Error getting transactions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Stock analysis endpoints (using existing stock_data_manager)
@app.get("/api/available-stocks/{user_id}")
async def get_available_stocks(user_id: str):
    """Get list of available stocks with historical data for a user"""
    try:
        # Get user holdings
        holdings = await db_manager.get_user_holdings(user_id)

        if not holdings:
            return []

        # Get symbols that have historical data
        db_symbols = set(stock_manager.get_available_symbols()) if Path("stock_data.db").exists() else set()

        portfolio_stocks = []
        for holding in holdings:
            symbol = holding.get("symbol")
            if symbol and symbol in db_symbols:
                portfolio_stocks.append(
                    {
                        "symbol": symbol,
                        "company_name": holding.get("company_name", ""),
                        "isin": holding.get("isin", ""),
                        "shares_held": holding.get("shares_held", 0),
                        "current_price": (
                            float(holding.get("current_price", 0)) if holding.get("current_price") else None
                        ),
                        "has_historical_data": True,
                    }
                )

        # Sort by company name
        portfolio_stocks.sort(key=lambda x: x["company_name"])

        logger.info(f"Returning {len(portfolio_stocks)} stocks with historical data for user {user_id}")
        return portfolio_stocks

    except Exception as e:
        logger.error(f"Error getting available stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock-analysis/{user_id}/{symbol}")
async def get_stock_analysis(user_id: str, symbol: str, days: int = None, range_type: str = "5Y"):
    """Get stock analysis data for a specific symbol and user"""
    try:
        # Verify user has this stock
        holdings = await db_manager.get_user_holdings(user_id)
        user_symbols = [h.get("symbol") for h in holdings if h.get("symbol")]

        if symbol not in user_symbols:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found in user portfolio")

        # Get price data (using existing stock manager logic)
        if range_type == "YTD":
            current_year = datetime.now().year
            ytd_start = datetime(current_year, 1, 1).date()

            import sqlite3

            with sqlite3.connect(stock_manager.db_path) as conn:
                query = """
                    SELECT date, close_price, open_price, high_price, low_price, volume
                    FROM stock_prices
                    WHERE symbol = ? AND date >= ?
                    ORDER BY date
                """
                price_data = pd.read_sql_query(query, conn, params=[symbol, ytd_start])
        else:
            if days is None:
                range_mapping = {"6M": 180, "1Y": 365, "3Y": 1095, "5Y": 1825, "ALL": None}
                days = range_mapping.get(range_type, 1825)

            price_data = stock_manager.get_stock_data(symbol, days=days)

        if price_data.empty:
            raise HTTPException(status_code=404, detail=f"No price data found for symbol {symbol}")

        # Format price data
        price_records = []
        for _, row in price_data.iterrows():
            price_records.append(
                {
                    "date": row["date"],
                    "price": float(row["close_price"]),
                    "open": float(row["open_price"]) if pd.notna(row["open_price"]) else None,
                    "high": float(row["high_price"]) if pd.notna(row["high_price"]) else None,
                    "low": float(row["low_price"]) if pd.notna(row["low_price"]) else None,
                    "volume": int(row["volume"]) if pd.notna(row["volume"]) else 0,
                }
            )

        # Get user transactions for this stock
        transactions = await get_user_stock_transactions(user_id, symbol)

        return {
            "symbol": symbol,
            "price_data": price_records,
            "transactions": transactions,
            "data_range": {"start": str(price_data["date"].min()), "end": str(price_data["date"].max())},
            "data_source": "postgresql_cache",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stock analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def get_user_stock_transactions(user_id: str, symbol: str):
    """Get user transactions for a specific stock symbol"""
    try:
        # Get user holdings to find ISIN for the symbol
        holdings = await db_manager.get_user_holdings(user_id)
        target_isin = None

        for holding in holdings:
            if holding.get("symbol") == symbol:
                target_isin = holding.get("isin")
                break

        if not target_isin:
            return []

        # Get transactions for this ISIN
        all_transactions = await db_manager.get_user_transactions(user_id)
        stock_transactions = []

        for transaction in all_transactions:
            if transaction.get("isin") == target_isin and transaction.get("transaction_type") in ["buy", "sell"]:
                stock_transactions.append(
                    {
                        "date": transaction["date"].isoformat() if transaction["date"] else None,
                        "type": transaction["transaction_type"],
                        "shares": transaction.get("shares", 0),
                        "price": float(transaction.get("price", 0)) if transaction.get("price") else None,
                        "amount_eur": float(transaction.get("amount_eur", 0)) if transaction.get("amount_eur") else 0,
                        "description": transaction.get("original_description", ""),
                    }
                )

        # Sort by date
        stock_transactions.sort(key=lambda x: x["date"] or "")
        return stock_transactions

    except Exception as e:
        logger.error(f"Error getting stock transactions: {e}")
        return []


# Legacy endpoints for backward compatibility
@app.get("/output/{filename}")
async def serve_output_file(filename: str):
    """Serve files from output directory (legacy support)"""
    file_path = Path(f"output/{filename}")
    if file_path.exists():
        return FileResponse(file_path)
    else:
        raise HTTPException(status_code=404, detail="File not found - data now stored in database")


@app.get("/api/database-stats")
async def get_database_stats():
    """Get database statistics"""
    try:
        async with db_manager.get_connection() as conn:
            user_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            transaction_count = await conn.fetchval("SELECT COUNT(*) FROM transactions")
            holding_count = await conn.fetchval("SELECT COUNT(*) FROM holdings")

        stock_stats = stock_manager.get_database_stats() if Path("stock_data.db").exists() else {}

        return {
            "database_type": "postgresql",
            "users": user_count or 0,
            "transactions": transaction_count or 0,
            "holdings": holding_count or 0,
            "stock_records": stock_stats.get("total_records", 0),
            "stock_symbols": stock_stats.get("total_symbols", 0),
        }
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    # Get port from environment (Render sets this)
    port = int(os.environ.get("PORT", 8000))

    logger.info("Starting DeGiro Dashboard with PostgreSQL support...")

    # Start the server
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
