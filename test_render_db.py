#!/usr/bin/env python3
"""
Test connection to Render PostgreSQL database
"""

import asyncio
import logging
import os
import asyncpg
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Render database connection string
DATABASE_URL = "postgresql://degiro_user:YlkhxaOEmwJZTVn42necg4p1fsljD7u8@dpg-d28c8vbipnbc739jbkr0-a.oregon-postgres.render.com/degiro_dashboard"

async def test_render_database():
    """Test connection to Render PostgreSQL database"""
    try:
        logger.info("🔗 Testing connection to Render PostgreSQL database...")
        
        # Test basic connection
        logger.info("1. Testing basic connection...")
        conn = await asyncpg.connect(DATABASE_URL)
        logger.info("✅ Connected successfully!")
        
        # Test basic query
        logger.info("2. Testing basic query...")
        result = await conn.fetchval("SELECT version()")
        logger.info(f"✅ PostgreSQL version: {result}")
        
        # Test database info
        logger.info("3. Getting database information...")
        db_name = await conn.fetchval("SELECT current_database()")
        user_name = await conn.fetchval("SELECT current_user")
        logger.info(f"✅ Database: {db_name}, User: {user_name}")
        
        # Test table creation
        logger.info("4. Testing table creation...")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("✅ Test table created successfully")
        
        # Test data insertion
        logger.info("5. Testing data insertion...")
        await conn.execute("""
            INSERT INTO test_table (name) VALUES ($1)
        """, f"test_entry_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        logger.info("✅ Data inserted successfully")
        
        # Test data retrieval
        logger.info("6. Testing data retrieval...")
        rows = await conn.fetch("SELECT * FROM test_table ORDER BY created_at DESC LIMIT 5")
        logger.info(f"✅ Retrieved {len(rows)} rows")
        for row in rows:
            logger.info(f"   - ID: {row['id']}, Name: {row['name']}, Created: {row['created_at']}")
        
        # Test our actual schema creation
        logger.info("7. Testing DeGiro schema creation...")
        
        # Users table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_upload TIMESTAMP,
                portfolio_name VARCHAR(255)
            )
        """)
        
        # Transactions table  
        await conn.execute("""
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
        """)
        
        # Holdings table
        await conn.execute("""
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
        """)
        
        logger.info("✅ DeGiro schema created successfully")
        
        # Test indexes
        logger.info("8. Creating indexes...")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_user_date ON transactions(user_id, date)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_isin ON transactions(isin)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_holdings_user ON holdings(user_id)")
        logger.info("✅ Indexes created successfully")
        
        # Get table info
        logger.info("9. Getting table information...")
        tables = await conn.fetch("""
            SELECT schemaname, tablename, tableowner 
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
        """)
        
        logger.info(f"✅ Found {len(tables)} tables:")
        for table in tables:
            logger.info(f"   - {table['tablename']} (owner: {table['tableowner']})")
        
        # Test a sample user creation
        logger.info("10. Testing sample user creation...")
        test_user_id = f"test_user_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        await conn.execute("""
            INSERT INTO users (user_id, portfolio_name) 
            VALUES ($1, $2)
        """, test_user_id, "Test Portfolio")
        
        user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", test_user_id)
        logger.info(f"✅ Test user created: {user['user_id']} (ID: {user['id']})")
        
        # Clean up test data
        logger.info("11. Cleaning up test data...")
        await conn.execute("DELETE FROM users WHERE user_id = $1", test_user_id)
        await conn.execute("DROP TABLE IF EXISTS test_table")
        logger.info("✅ Test data cleaned up")
        
        # Close connection
        await conn.close()
        logger.info("✅ Connection closed")
        
        logger.info("🎉 All database tests passed! Render PostgreSQL is working perfectly.")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
        logger.exception("Full error details:")
        return False

async def main():
    """Main test function"""
    success = await test_render_database()
    if success:
        logger.info("✅ Render PostgreSQL database is ready for deployment!")
    else:
        logger.error("❌ Database tests failed. Check connection and credentials.")

if __name__ == "__main__":
    asyncio.run(main())