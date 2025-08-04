#!/usr/bin/env python3
"""
DeGiro Dashboard - Simplified Version for Render
Uses SQLAlchemy instead of asyncpg to avoid compilation issues
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import tempfile

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="DeGiro Portfolio Dashboard", version="2.0.0-simple")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database setup
DATABASE_URL = os.environ.get('DATABASE_URL')
if DATABASE_URL and DATABASE_URL.startswith('postgres://'):
    # Render sometimes uses postgres:// instead of postgresql://
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

engine = None
SessionLocal = None

def init_database():
    """Initialize database connection"""
    global engine, SessionLocal
    
    try:
        if not DATABASE_URL:
            logger.warning("No DATABASE_URL found, running in local mode")
            return False
            
        logger.info("Connecting to PostgreSQL database...")
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        
        # Create tables
        with engine.connect() as conn:
            # Users table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255) UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_upload TIMESTAMP,
                    portfolio_name VARCHAR(255)
                )
            """))
            
            # Transactions table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL,
                    date DATE NOT NULL,
                    product VARCHAR(255) NOT NULL,
                    isin VARCHAR(12),
                    original_description TEXT,
                    amount_eur DECIMAL(12,2),
                    shares INTEGER DEFAULT 0,
                    price DECIMAL(10,4) DEFAULT 0,
                    transaction_type VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # Holdings table  
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS holdings (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL,
                    isin VARCHAR(12) NOT NULL,
                    company_name VARCHAR(255) NOT NULL,
                    symbol VARCHAR(10),
                    shares_held INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, isin)
                )
            """))
            
            conn.commit()
            
        logger.info("Database initialized successfully")
        return True
        
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        return False

# Initialize database on startup
DB_AVAILABLE = init_database()

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "database": "connected" if DB_AVAILABLE else "local_mode",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the main dashboard"""
    dashboard_file = Path("portfolio_dashboard.html")
    if dashboard_file.exists():
        return FileResponse("portfolio_dashboard.html")
    else:
        return HTMLResponse("Dashboard not found", status_code=404)

@app.post("/api/upload-degiro-data")
async def upload_degiro_data(
    file: UploadFile = File(...),
    user_id: str = Form(...)
):
    """Upload and process DeGiro CSV data"""
    try:
        # Validate file
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed")

        # Read file content
        content = await file.read()
        csv_content = content.decode('utf-8')
        
        logger.info(f"Processing upload for user {user_id}, file: {file.filename}")

        if DB_AVAILABLE:
            return await process_with_database(user_id, csv_content, file.filename)
        else:
            return await process_locally(user_id, csv_content, file.filename)
        
    except Exception as e:
        logger.error(f"Upload processing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def process_with_database(user_id: str, csv_content: str, filename: str):
    """Process CSV and store in PostgreSQL"""
    try:
        from io import StringIO
        
        # Parse CSV
        df = pd.read_csv(StringIO(csv_content))
        
        # Basic data cleaning
        if 'Fecha' in df.columns:
            df['Fecha'] = pd.to_datetime(df['Fecha'], dayfirst=True)
        
        with engine.connect() as conn:
            # Create or get user
            result = conn.execute(
                text("SELECT id FROM users WHERE user_id = :user_id"),
                {"user_id": user_id}
            )
            
            if not result.fetchone():
                conn.execute(
                    text("INSERT INTO users (user_id) VALUES (:user_id)"),
                    {"user_id": user_id}
                )
            
            # Update last upload
            conn.execute(
                text("UPDATE users SET last_upload = CURRENT_TIMESTAMP WHERE user_id = :user_id"),
                {"user_id": user_id}
            )
            
            # Process transactions (simplified)
            transaction_count = 0
            if not df.empty:
                for _, row in df.iterrows():
                    try:
                        # Extract basic info
                        date_val = row.get('Fecha')
                        product = str(row.get('Producto', ''))[:255]
                        description = str(row.get('Descripción', ''))
                        amount = float(row.get('Cambio', 0)) if pd.notna(row.get('Cambio')) else 0.0
                        
                        # Extract ISIN if present
                        isin = None
                        if 'ISIN' in row:
                            isin = row['ISIN']
                        elif '(' in product and ')' in product:
                            # Try to extract ISIN from product name
                            import re
                            isin_match = re.search(r'\(([A-Z]{2}[A-Z0-9]{9}[0-9])\)', product)
                            if isin_match:
                                isin = isin_match.group(1)
                        
                        # Determine transaction type
                        trans_type = 'other'
                        desc_lower = description.lower()
                        if 'compra' in desc_lower or 'buy' in desc_lower:
                            trans_type = 'buy'
                        elif 'venta' in desc_lower or 'sell' in desc_lower:
                            trans_type = 'sell'
                        elif 'dividend' in desc_lower:
                            trans_type = 'dividend'
                        elif 'depósito' in desc_lower or 'deposit' in desc_lower:
                            trans_type = 'deposit'
                        
                        conn.execute(text("""
                            INSERT INTO transactions 
                            (user_id, date, product, isin, original_description, amount_eur, transaction_type)
                            VALUES (:user_id, :date, :product, :isin, :description, :amount, :trans_type)
                        """), {
                            "user_id": user_id,
                            "date": date_val.date() if pd.notna(date_val) else None,
                            "product": product,
                            "isin": isin,
                            "description": description,
                            "amount": amount,
                            "trans_type": trans_type
                        })
                        
                        transaction_count += 1
                        
                    except Exception as row_error:
                        logger.warning(f"Error processing row: {row_error}")
                        continue
            
            conn.commit()
            
        result = {
            "success": True,
            "user_id": user_id,
            "transactions_count": transaction_count,
            "holdings_count": 0,  # Simplified for now
            "processing_timestamp": datetime.now().isoformat(),
            "mode": "database"
        }
        
        logger.info(f"Database processing completed for user {user_id}: {transaction_count} transactions")
        return result
        
    except Exception as e:
        logger.error(f"Database processing failed: {e}")
        raise

async def process_locally(user_id: str, csv_content: str, filename: str):
    """Fallback local processing"""
    try:
        # Create temp directory
        temp_dir = Path("temp_output") / user_id
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Save CSV
        csv_path = temp_dir / filename
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        # Basic processing
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))
        
        result = {
            "success": True,
            "user_id": user_id,
            "transactions_count": len(df),
            "holdings_count": 0,
            "processing_timestamp": datetime.now().isoformat(),
            "mode": "local"
        }
        
        # Save result
        with open(temp_dir / 'result.json', 'w') as f:
            json.dump(result, f, default=str)
        
        logger.info(f"Local processing completed for user {user_id}")
        return result
        
    except Exception as e:
        logger.error(f"Local processing failed: {e}")
        raise

@app.get("/api/portfolio-status/{user_id}")
async def get_portfolio_status(user_id: str):
    """Get portfolio status for a user"""
    try:
        if DB_AVAILABLE:
            with engine.connect() as conn:
                # Check if user exists and has data
                result = conn.execute(
                    text("SELECT COUNT(*) as count FROM transactions WHERE user_id = :user_id"),
                    {"user_id": user_id}
                ).fetchone()
                
                transaction_count = result.count if result else 0
                
                if transaction_count == 0:
                    return JSONResponse(content={"has_data": False}, status_code=404)
                
                return {
                    "has_data": True,
                    "transactions_count": transaction_count,
                    "holdings_count": 0,  # Simplified
                    "last_updated": datetime.now().isoformat()
                }
        else:
            # Check local files
            result_file = Path("temp_output") / user_id / "result.json"
            if result_file.exists():
                with open(result_file) as f:
                    data = json.load(f)
                return {
                    "has_data": True,
                    "transactions_count": data.get("transactions_count", 0),
                    "holdings_count": data.get("holdings_count", 0),
                    "last_updated": data.get("processing_timestamp", datetime.now().isoformat())
                }
            else:
                return JSONResponse(content={"has_data": False}, status_code=404)
        
    except Exception as e:
        logger.error(f"Error getting portfolio status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/portfolio/{user_id}")
async def get_portfolio_data(user_id: str):
    """Get portfolio data for a user"""
    try:
        if DB_AVAILABLE:
            with engine.connect() as conn:
                # Get transactions
                transactions = conn.execute(
                    text("SELECT * FROM transactions WHERE user_id = :user_id ORDER BY date DESC"),
                    {"user_id": user_id}
                ).fetchall()
                
                if not transactions:
                    raise HTTPException(status_code=404, detail="No portfolio data found")
                
                return {
                    "portfolio_value": 0,  # Simplified
                    "cash_balance": 0,
                    "total_invested": 0,
                    "holdings": [],  # Simplified
                    "summary": {
                        "total_positions": 0,
                        "total_transactions": len(transactions),
                        "last_updated": datetime.now().isoformat()
                    }
                }
        else:
            raise HTTPException(status_code=404, detail="No portfolio data found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting portfolio data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.environ.get("PORT", 8000))
    
    logger.info("Starting DeGiro Dashboard (Simplified Version)...")
    logger.info(f"Database available: {DB_AVAILABLE}")
    
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")