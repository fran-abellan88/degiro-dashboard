#!/usr/bin/env python3
"""
Stock API Server
FastAPI server to serve stock data from SQLite database
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from stock_data_manager import StockDataManager
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Stock Data API", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize the stock data manager
stock_manager = StockDataManager()

@app.get("/")
async def root():
    """API health check"""
    return {"message": "Stock Data API is running", "version": "1.0.0"}

@app.get("/available-stocks")
async def get_available_stocks():
    """Get list of available stocks with portfolio information"""
    try:
        # Get symbols from database
        db_symbols = set(stock_manager.get_available_symbols())
        
        # Get portfolio information
        portfolio_df = pd.read_csv('output/current_stock_values.csv')
        portfolio_stocks = []
        
        for _, row in portfolio_df.iterrows():
            if pd.notna(row['symbol']) and row['symbol'] in db_symbols:
                portfolio_stocks.append({
                    'symbol': row['symbol'],
                    'company_name': row['company_name'],
                    'isin': row['isin'],
                    'shares_held': float(row['shares_held']) if pd.notna(row['shares_held']) else 0,
                    'current_price': float(row['current_price']) if pd.notna(row['current_price']) else None,
                    'has_historical_data': True
                })
        
        # Sort by company name
        portfolio_stocks.sort(key=lambda x: x['company_name'])
        
        logger.info(f"Returning {len(portfolio_stocks)} stocks with historical data")
        return portfolio_stocks
        
    except Exception as e:
        logger.error(f"Error getting available stocks: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stock-analysis/{symbol}")
async def get_stock_analysis(symbol: str, days: int = None, range_type: str = "5Y"):
    """Get stock analysis data for a specific symbol with time range filtering"""
    try:
        # Calculate days based on range_type if days not specified
        if days is None:
            range_mapping = {
                "YTD": None,  # Special handling for year-to-date
                "6M": 180,    # 6 months
                "1Y": 365,    # 1 year
                "3Y": 1095,   # 3 years
                "5Y": 1825,   # 5 years
                "ALL": None   # All available data
            }
            days = range_mapping.get(range_type, 1825)
        
        # Get stock price data
        if range_type == "YTD":
            # Special handling for year-to-date
            current_year = datetime.now().year
            ytd_start = datetime(current_year, 1, 1).date()
            cutoff_date = ytd_start
            
            with sqlite3.connect(stock_manager.db_path) as conn:
                query = """
                    SELECT date, close_price, open_price, high_price, low_price, volume
                    FROM stock_prices 
                    WHERE symbol = ? AND date >= ?
                    ORDER BY date
                """
                price_data = pd.read_sql_query(query, conn, params=[symbol, cutoff_date])
        else:
            price_data = stock_manager.get_stock_data(symbol, days=days)
        
        if price_data.empty:
            raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")
        
        # Convert to the format expected by the frontend
        price_records = []
        for _, row in price_data.iterrows():
            price_records.append({
                'date': row['date'],
                'price': float(row['close_price']),
                'open': float(row['open_price']) if pd.notna(row['open_price']) else None,
                'high': float(row['high_price']) if pd.notna(row['high_price']) else None,
                'low': float(row['low_price']) if pd.notna(row['low_price']) else None,
                'volume': int(row['volume']) if pd.notna(row['volume']) else 0
            })
        
        # Get user transactions for this stock (reuse existing logic)
        transactions = get_user_transactions(symbol)
        
        # Calculate date range
        start_date = price_data['date'].min()
        end_date = price_data['date'].max()
        
        response_data = {
            'symbol': symbol,
            'price_data': price_records,
            'transactions': transactions,
            'data_range': {
                'start': str(start_date),
                'end': str(end_date)
            },
            'data_source': 'sqlite_cache'
        }
        
        logger.info(f"Returning {len(price_records)} price records and {len(transactions)} transactions for {symbol}")
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting stock analysis for {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def get_user_transactions(symbol):
    """Get user's buy/sell transactions for the given symbol"""
    transactions = []
    
    try:
        # Get the ISIN for this symbol from current stock values
        stock_values_file = Path("output/current_stock_values.csv")
        target_isin = None
        
        if stock_values_file.exists():
            df_stocks = pd.read_csv(stock_values_file)
            matching_stock = df_stocks[df_stocks['symbol'] == symbol]
            if not matching_stock.empty:
                target_isin = matching_stock.iloc[0]['isin']
        
        # Load buy transactions
        buys_file = Path("output/degiro_buys.csv")
        if buys_file.exists():
            df_buys = pd.read_csv(buys_file)
            
            # Find transactions for this symbol using ISIN
            if target_isin:
                symbol_buys = df_buys[df_buys['ISIN'] == target_isin]
            else:
                # Fallback to product name matching
                symbol_buys = df_buys[df_buys['product'].str.contains(symbol, case=False, na=False)]
            
            for _, buy in symbol_buys.iterrows():
                if pd.notna(buy['date']):
                    transactions.append({
                        'date': pd.to_datetime(buy['date']).strftime('%Y-%m-%d'),
                        'type': 'buy',
                        'shares': int(buy['shares']) if pd.notna(buy['shares']) else 0,
                        'price': float(buy['price']) if pd.notna(buy['price']) else None,
                        'amount_eur': float(buy['amount_EUR']) if pd.notna(buy['amount_EUR']) else 0,
                        'description': str(buy['original_description']) if pd.notna(buy['original_description']) else ''
                    })
        
        # Load sell transactions
        sells_file = Path("output/degiro_sells.csv")
        if sells_file.exists():
            df_sells = pd.read_csv(sells_file)
            
            # Find transactions for this symbol using ISIN
            if target_isin:
                symbol_sells = df_sells[df_sells['ISIN'] == target_isin]
            else:
                # Fallback to product name matching
                symbol_sells = df_sells[df_sells['product'].str.contains(symbol, case=False, na=False)]
            
            for _, sell in symbol_sells.iterrows():
                if pd.notna(sell['date']):
                    transactions.append({
                        'date': pd.to_datetime(sell['date']).strftime('%Y-%m-%d'),
                        'type': 'sell',
                        'shares': int(sell['shares']) if pd.notna(sell['shares']) else 0,
                        'price': float(sell['price']) if pd.notna(sell['price']) else None,
                        'amount_eur': float(sell['amount_EUR']) if pd.notna(sell['amount_EUR']) else 0,
                        'description': str(sell['original_description']) if pd.notna(sell['original_description']) else ''
                    })
        
        # Sort transactions by date
        transactions.sort(key=lambda x: x['date'])
        
    except Exception as e:
        logger.error(f"Error loading transactions for {symbol}: {e}")
    
    return transactions

@app.get("/database-stats")
async def get_database_stats():
    """Get database statistics"""
    try:
        stats = stock_manager.get_database_stats()
        return stats
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/update-data")
async def update_data():
    """Trigger an incremental data update"""
    try:
        stock_manager.incremental_update()
        stats = stock_manager.get_database_stats()
        return {
            "message": "Data update completed",
            "stats": stats
        }
    except Exception as e:
        logger.error(f"Error updating data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    
    # Check if database exists and has data
    if not Path("stock_data.db").exists():
        logger.warning("Database not found. Run 'python stock_data_manager.py --initial-load' first")
    else:
        stats = stock_manager.get_database_stats()
        if stats['total_records'] == 0:
            logger.warning("Database is empty. Run 'python stock_data_manager.py --initial-load' first")
        else:
            logger.info(f"Database ready with {stats['total_records']:,} records for {stats['total_symbols']} symbols")
    
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")