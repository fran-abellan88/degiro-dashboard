#!/usr/bin/env python3
"""
Database Initialization Script for Render Deployment
Initializes the stock database if needed during deployment
"""

import logging
import os
import sys
from pathlib import Path

from stock_data_manager import StockDataManager

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def init_database_for_deployment():
    """Initialize database for deployment"""
    try:
        logger.info("🚀 Starting database initialization for deployment...")

        # Check if we're in a deployment environment
        is_deployment = os.environ.get("RENDER") or os.environ.get("PORT")

        # Initialize the stock data manager
        stock_manager = StockDataManager()

        # Check if database already has data
        stats = stock_manager.get_database_stats()

        if stats["total_records"] > 0:
            logger.info(f"✅ Database already initialized with {stats['total_records']:,} records")
            return True

        # Check if we have portfolio data
        portfolio_file = Path("output/current_stock_values.csv")
        if not portfolio_file.exists():
            logger.warning("⚠️ Portfolio data not found. Database will be initialized but empty.")
            logger.warning("   Upload portfolio data and run manual initialization later.")
            return True

        # Only perform initial load if we have portfolio data
        if is_deployment:
            logger.info("🔄 Deployment environment detected - performing initial data load...")
            stock_manager.initial_load()

            # Verify the load was successful
            final_stats = stock_manager.get_database_stats()
            if final_stats["total_records"] > 0:
                logger.info(f"✅ Database initialization complete! {final_stats['total_records']:,} records loaded")
                return True
            else:
                logger.warning("⚠️ Database initialization completed but no data was loaded")
                return True
        else:
            logger.info("🏠 Local environment - skipping automatic data load")
            return True

    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        # Don't fail the deployment if database init fails
        return True


if __name__ == "__main__":
    success = init_database_for_deployment()
    if not success:
        logger.error("Database initialization failed")
        sys.exit(1)
    else:
        logger.info("Database initialization completed successfully")
