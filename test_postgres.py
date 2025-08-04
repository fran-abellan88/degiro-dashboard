#!/usr/bin/env python3
"""
Test PostgreSQL Integration
Quick test script to verify PostgreSQL setup works
"""

import asyncio
import logging
import os

from database_models import db_manager
from degiro_processor_pg import degiro_processor

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test CSV content (minimal example)
TEST_CSV_CONTENT = """Fecha,Producto,Descripción,Cambio,Saldo EUR
01-01-2024,APPLE INC (US0378331005),Compra 10 APPLE@150.25 USD (US0378331005),-1502.50,10000.00
15-01-2024,APPLE INC (US0378331005),Dividend APPLE (US0378331005),15.50,10015.50
01-02-2024,APPLE INC (US0378331005),Venta 5 APPLE@155.75 USD (US0378331005),778.75,10794.25
"""


async def test_postgresql_integration():
    """Test the complete PostgreSQL integration"""
    test_user_id = "test_user_001"

    try:
        logger.info("🧪 Starting PostgreSQL integration test...")

        # 1. Test database connection
        logger.info("1. Testing database connection...")
        await db_manager.init_pool()
        await db_manager.create_tables()
        logger.info("✅ Database connection and tables created successfully")

        # 2. Test data processing and storage
        logger.info("2. Testing data processing and storage...")
        result = await degiro_processor.process_and_store(test_user_id, TEST_CSV_CONTENT)
        logger.info(f"✅ Data processing completed: {result}")

        # 3. Test data retrieval
        logger.info("3. Testing data retrieval...")

        # Get user data
        user = await db_manager.create_or_get_user(test_user_id)
        logger.info(f"✅ User retrieved: {user}")

        # Get transactions
        transactions = await db_manager.get_user_transactions(test_user_id)
        logger.info(f"✅ Retrieved {len(transactions)} transactions")

        # Get holdings
        holdings = await db_manager.get_user_holdings(test_user_id)
        logger.info(f"✅ Retrieved {len(holdings)} holdings")

        # 4. Test API compatibility (simulate FastAPI calls)
        logger.info("4. Testing API data format...")

        # Format transactions like the API would
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

        logger.info(f"✅ API-formatted transactions: {len(formatted_transactions)} items")

        # Calculate portfolio summary
        total_value = sum(h.get("position_value", 0) for h in holdings)
        logger.info(f"✅ Portfolio total value: €{total_value:.2f}")

        # 5. Clean up test data
        logger.info("5. Cleaning up test data...")
        async with db_manager.get_connection() as conn:
            await conn.execute("DELETE FROM transactions WHERE user_id = $1", test_user_id)
            await conn.execute("DELETE FROM holdings WHERE user_id = $1", test_user_id)
            await conn.execute("DELETE FROM degiro_raw_data WHERE user_id = $1", test_user_id)
            await conn.execute("DELETE FROM users WHERE user_id = $1", test_user_id)
        logger.info("✅ Test data cleaned up")

        # Close connections
        await db_manager.close_pool()

        logger.info("🎉 PostgreSQL integration test completed successfully!")
        return True

    except Exception as e:
        logger.error(f"❌ PostgreSQL integration test failed: {e}")
        logger.exception("Full error details:")
        return False


async def main():
    """Main test function"""
    # Set a local database URL for testing (you'll need to set this)
    if not os.environ.get("DATABASE_URL"):
        logger.warning("DATABASE_URL not set. Using localhost fallback.")
        os.environ["DATABASE_URL"] = "postgresql://localhost:5432/degiro_test"

    success = await test_postgresql_integration()
    if success:
        logger.info("✅ All tests passed! PostgreSQL integration is working.")
    else:
        logger.error("❌ Tests failed. Check the error messages above.")
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())
