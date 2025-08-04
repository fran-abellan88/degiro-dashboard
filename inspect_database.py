#!/usr/bin/env python3
"""
Database Inspector for Render PostgreSQL
View tables, data, and database structure
"""

import asyncio
import json
import logging
from datetime import datetime

import asyncpg

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Render database connection string
DATABASE_URL = "postgresql://degiro_user:YlkhxaOEmwJZTVn42necg4p1fsljD7u8@dpg-d28c8vbipnbc739jbkr0-a.oregon-postgres.render.com/degiro_dashboard"


async def inspect_database():
    """Comprehensive database inspection"""
    try:
        logger.info("🔍 Connecting to database...")
        conn = await asyncpg.connect(DATABASE_URL)

        # 1. Database Overview
        logger.info("\n" + "=" * 60)
        logger.info("📊 DATABASE OVERVIEW")
        logger.info("=" * 60)

        db_name = await conn.fetchval("SELECT current_database()")
        version = await conn.fetchval("SELECT version()")
        current_time = await conn.fetchval("SELECT NOW()")

        logger.info(f"Database: {db_name}")
        logger.info(f"Time: {current_time}")
        logger.info(f"Version: {version.split(',')[0]}")  # Just the first part

        # 2. List All Tables
        logger.info("\n" + "=" * 60)
        logger.info("📋 ALL TABLES")
        logger.info("=" * 60)

        tables = await conn.fetch(
            """
            SELECT 
                schemaname, 
                tablename, 
                tableowner,
                pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
        """
        )

        if tables:
            for table in tables:
                logger.info(f"📄 {table['tablename']:<20} (Owner: {table['tableowner']}, Size: {table['size']})")
        else:
            logger.info("No tables found")

        # 3. Inspect Each Table
        for table in tables:
            table_name = table["tablename"]
            logger.info(f"\n{'='*60}")
            logger.info(f"🔍 TABLE: {table_name.upper()}")
            logger.info("=" * 60)

            # Get table structure
            columns = await conn.fetch(
                """
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_name = $1 AND table_schema = 'public'
                ORDER BY ordinal_position
            """,
                table_name,
            )

            logger.info("📋 COLUMNS:")
            for col in columns:
                nullable = "NULL" if col["is_nullable"] == "YES" else "NOT NULL"
                default = f" DEFAULT {col['column_default']}" if col["column_default"] else ""
                logger.info(f"  • {col['column_name']:<20} {col['data_type']:<15} {nullable}{default}")

            # Get row count
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
            logger.info(f"\n📊 ROWS: {count}")

            # Show sample data if table has rows
            if count > 0:
                logger.info(f"\n📄 SAMPLE DATA (first 5 rows):")
                rows = await conn.fetch(f"SELECT * FROM {table_name} LIMIT 5")

                if rows:
                    # Show column headers
                    headers = list(rows[0].keys())
                    logger.info("  " + " | ".join(f"{h:<15}" for h in headers))
                    logger.info("  " + "-" * (len(headers) * 18))

                    # Show data rows
                    for row in rows:
                        values = []
                        for key in headers:
                            value = row[key]
                            if value is None:
                                values.append("NULL".ljust(15))
                            elif isinstance(value, datetime):
                                values.append(str(value)[:15].ljust(15))
                            else:
                                values.append(str(value)[:15].ljust(15))
                        logger.info("  " + " | ".join(values))

            # Show indexes
            indexes = await conn.fetch(
                """
                SELECT 
                    indexname,
                    indexdef
                FROM pg_indexes 
                WHERE tablename = $1 AND schemaname = 'public'
                ORDER BY indexname
            """,
                table_name,
            )

            if indexes:
                logger.info(f"\n🔗 INDEXES:")
                for idx in indexes:
                    logger.info(f"  • {idx['indexname']}")

        # 4. Check for specific DeGiro data
        logger.info(f"\n{'='*60}")
        logger.info("🎯 DEGIRO DATA SUMMARY")
        logger.info("=" * 60)

        # Users summary
        try:
            users_count = await conn.fetchval("SELECT COUNT(*) FROM users")
            logger.info(f"👥 Total Users: {users_count}")

            if users_count > 0:
                recent_users = await conn.fetch(
                    """
                    SELECT user_id, created_at, last_upload 
                    FROM users 
                    ORDER BY created_at DESC 
                    LIMIT 5
                """
                )
                logger.info("   Recent users:")
                for user in recent_users:
                    upload_info = f" (last upload: {user['last_upload']})" if user["last_upload"] else " (no uploads)"
                    logger.info(f"   • {user['user_id']} - {user['created_at']}{upload_info}")
        except:
            logger.info("👥 Users table not accessible")

        # Transactions summary
        try:
            transactions_count = await conn.fetchval("SELECT COUNT(*) FROM transactions")
            logger.info(f"💰 Total Transactions: {transactions_count}")

            if transactions_count > 0:
                transaction_types = await conn.fetch(
                    """
                    SELECT transaction_type, COUNT(*) as count
                    FROM transactions 
                    GROUP BY transaction_type 
                    ORDER BY count DESC
                """
                )
                logger.info("   By type:")
                for t_type in transaction_types:
                    logger.info(f"   • {t_type['transaction_type']}: {t_type['count']}")
        except:
            logger.info("💰 Transactions table not accessible")

        # Holdings summary
        try:
            holdings_count = await conn.fetchval("SELECT COUNT(*) FROM holdings")
            logger.info(f"📈 Total Holdings: {holdings_count}")

            if holdings_count > 0:
                top_holdings = await conn.fetch(
                    """
                    SELECT company_name, symbol, shares_held, position_value
                    FROM holdings 
                    ORDER BY position_value DESC NULLS LAST
                    LIMIT 5
                """
                )
                logger.info("   Top holdings by value:")
                for holding in top_holdings:
                    value = f"€{holding['position_value']:.2f}" if holding["position_value"] else "N/A"
                    logger.info(
                        f"   • {holding['company_name']} ({holding['symbol']}): {holding['shares_held']} shares, {value}"
                    )
        except:
            logger.info("📈 Holdings table not accessible")

        # 5. Database size and performance
        logger.info(f"\n{'='*60}")
        logger.info("📊 DATABASE STATISTICS")
        logger.info("=" * 60)

        db_size = await conn.fetchval("SELECT pg_size_pretty(pg_database_size(current_database()))")
        logger.info(f"💾 Database Size: {db_size}")

        # Connection info
        connection_count = await conn.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
        )
        logger.info(f"🔗 Active Connections: {connection_count}")

        await conn.close()
        logger.info(f"\n✅ Database inspection completed!")

    except Exception as e:
        logger.error(f"❌ Error inspecting database: {e}")
        logger.exception("Full error details:")


async def export_data_to_json():
    """Export all data to JSON files for easy viewing"""
    try:
        logger.info("📤 Exporting data to JSON files...")
        conn = await asyncpg.connect(DATABASE_URL)

        # Get all tables
        tables = await conn.fetch(
            """
            SELECT tablename 
            FROM pg_tables 
            WHERE schemaname = 'public'
            ORDER BY tablename
        """
        )

        export_data = {}

        for table in tables:
            table_name = table["tablename"]
            logger.info(f"   Exporting {table_name}...")

            rows = await conn.fetch(f"SELECT * FROM {table_name}")

            # Convert to JSON-serializable format
            table_data = []
            for row in rows:
                row_dict = {}
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row_dict[key] = value.isoformat()
                    elif hasattr(value, "isoformat"):  # date objects
                        row_dict[key] = value.isoformat()
                    else:
                        row_dict[key] = value
                table_data.append(row_dict)

            export_data[table_name] = table_data

        # Save to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"database_export_{timestamp}.json"

        with open(filename, "w") as f:
            json.dump(export_data, f, indent=2, default=str)

        logger.info(f"✅ Data exported to {filename}")

        await conn.close()

    except Exception as e:
        logger.error(f"❌ Error exporting data: {e}")


async def main():
    """Main function"""
    logger.info("🔍 DeGiro Database Inspector - Full Inspection")
    await inspect_database()


if __name__ == "__main__":
    asyncio.run(main())
