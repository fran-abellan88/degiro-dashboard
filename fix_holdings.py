#!/usr/bin/env python3
"""
Fix Holdings - Reprocess existing transactions to generate holdings
"""

import asyncio
import logging
import os
from datetime import datetime

# Set environment variable BEFORE importing modules
os.environ['DATABASE_URL'] = "postgresql://degiro_user:YlkhxaOEmwJZTVn42necg4p1fsljD7u8@dpg-d28c8vbipnbc739jbkr0-a.oregon-postgres.render.com/degiro_dashboard"

from database_models import db_manager
from degiro_processor_pg import degiro_processor

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fix_holdings_for_all_users():
    """Reprocess transactions and regenerate holdings for all users"""
    try:
        logger.info("🔧 Starting holdings fix process...")
        
        # Get all users
        users = await db_manager.get_all_users()
        logger.info(f"Found {len(users)} users to process")
        
        for user in users:
            user_id = user['user_id']
            logger.info(f"\n📊 Processing user: {user_id}")
            
            # Get raw CSV data for this user
            raw_data = await db_manager.get_user_raw_data(user_id)
            if not raw_data:
                logger.warning(f"No raw data found for user {user_id}")
                continue
                
            # Use the most recent CSV file
            latest_data = max(raw_data, key=lambda x: x['upload_timestamp'])
            csv_content = latest_data['file_data']
            
            logger.info(f"Reprocessing CSV data from {latest_data['upload_timestamp']}")
            
            # Load and categorize data using the fixed processor
            df = degiro_processor.load_degiro_data(csv_content)
            transactions = degiro_processor.categorize_transactions(df)
            
            # Calculate holdings with fixed patterns
            holdings_df = degiro_processor.calculate_holdings(transactions)
            
            # Store holdings (store_holdings method already clears existing)
            holdings_count = 0
            if not holdings_df.empty:
                await db_manager.store_holdings(user_id, holdings_df)
                holdings_count = len(holdings_df)
                
            logger.info(f"✅ Generated {holdings_count} holdings for user {user_id}")
            
        logger.info("🎉 Holdings fix process completed!")
        
    except Exception as e:
        logger.error(f"❌ Error fixing holdings: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(fix_holdings_for_all_users())