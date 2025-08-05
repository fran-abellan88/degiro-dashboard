#!/usr/bin/env python3
"""
Debug Holdings - Inspect why holdings calculation is returning 0
"""

import asyncio
import logging
import os
import pandas as pd

# Set environment variable BEFORE importing modules
os.environ['DATABASE_URL'] = "postgresql://degiro_user:YlkhxaOEmwJZTVn42necg4p1fsljD7u8@dpg-d28c8vbipnbc739jbkr0-a.oregon-postgres.render.com/degiro_dashboard"

from database_models import db_manager
from degiro_processor_pg import degiro_processor

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def debug_holdings():
    """Debug holdings calculation step by step"""
    try:
        logger.info("🐛 Starting holdings debug process...")
        
        # Get first user
        users = await db_manager.get_all_users()
        if not users:
            logger.error("No users found")
            return
            
        user = users[0]
        user_id = user['user_id']
        logger.info(f"Debugging user: {user_id}")
        
        # Get raw CSV data
        raw_data = await db_manager.get_user_raw_data(user_id)
        if not raw_data:
            logger.error("No raw data found")
            return
            
        latest_data = max(raw_data, key=lambda x: x['upload_timestamp'])
        csv_content = latest_data['file_data']
        
        # Process step by step
        logger.info("1. Loading CSV data...")
        df = degiro_processor.load_degiro_data(csv_content)
        logger.info(f"   Loaded {len(df)} rows")
        
        logger.info("2. Categorizing transactions...")
        
        # Check column names and ISIN data before categorization
        logger.info(f"   Available columns: {list(df.columns)}")
        logger.info(f"   ISIN column sample values: {df['ISIN'].head(5).tolist()}")
        logger.info(f"   ISIN null count: {df['ISIN'].isnull().sum()} out of {len(df)}")
        
        transactions = degiro_processor.categorize_transactions(df)
        
        for category, trans_df in transactions.items():
            if not trans_df.empty:
                logger.info(f"   {category}: {len(trans_df)} transactions")
                
                # Show first few for debugging
                if category == "buys":
                    logger.info("   Sample buy transactions:")
                    for i, row in trans_df.head(3).iterrows():
                        logger.info(f"     • {row['original_description']} | Shares: {row.get('shares', 'N/A')} | Amount: {row.get('amount_EUR', 'N/A')} | Valid: {row.get('is_valid', 'N/A')}")
        
        logger.info("3. Calculating holdings...")
        buys_df = transactions["buys"]
        
        if buys_df.empty:
            logger.error("   No buy transactions found!")
            return
            
        logger.info(f"   Buy transactions shape: {buys_df.shape}")
        logger.info(f"   Buy transactions columns: {list(buys_df.columns)}")
        
        # Check if buys have valid shares and ISIN
        valid_buys = buys_df[buys_df['shares'] > 0]
        logger.info(f"   Valid buys (shares > 0): {len(valid_buys)}")
        
        if len(valid_buys) > 0:
            logger.info("   Sample valid buys:")
            for i, row in valid_buys.head(3).iterrows():
                logger.info(f"     • Product: {row['product']} | ISIN: {row.get('ISIN', 'N/A')} | Shares: {row['shares']} | Amount: {row['amount_EUR']}")
        
        # Check grouping
        if len(valid_buys) > 0:
            logger.info("4. Grouping by ISIN and product...")
            unique_holdings = (
                valid_buys
                .groupby(["ISIN", "product"])
                .agg({"shares": "sum", "amount_EUR": "sum"})
                .reset_index()
            )
            logger.info(f"   Unique holdings before sell subtraction: {len(unique_holdings)}")
            
            if len(unique_holdings) > 0:
                logger.info("   Sample grouped holdings:")
                for i, row in unique_holdings.head(3).iterrows():
                    logger.info(f"     • {row['product']} ({row['ISIN']}): {row['shares']} shares")
        
        # Full calculation
        holdings_df = degiro_processor.calculate_holdings(transactions)
        logger.info(f"5. Final holdings calculated: {len(holdings_df)}")
        
        if not holdings_df.empty:
            logger.info("   Holdings summary:")
            for i, row in holdings_df.iterrows():
                logger.info(f"     • {row['company_name']}: {row['shares_held']} shares")
        
    except Exception as e:
        logger.error(f"❌ Error in debug: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(debug_holdings())