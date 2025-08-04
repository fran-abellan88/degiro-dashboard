# PostgreSQL Migration Guide

## 🚀 **Overview**

The DeGiro Dashboard has been upgraded from CSV file-based storage to PostgreSQL database with file upload functionality. This provides:

- **Multi-user support** - Each user uploads their own data
- **Production-ready storage** - PostgreSQL instead of CSV files
- **Better performance** - Database queries vs file reading
- **Scalability** - Handle multiple users and large datasets
- **Data persistence** - Survives app restarts and redeployments

## 📁 **New Files Created**

### **Database Layer**
- `database_models.py` - PostgreSQL models and database manager
- `degiro_processor_pg.py` - PostgreSQL-compatible data processor
- `app_pg.py` - New FastAPI application with PostgreSQL support

### **Testing**
- `test_postgres.py` - PostgreSQL integration test script

### **Documentation**
- `POSTGRESQL_MIGRATION.md` - This migration guide

## 🗄️ **Database Schema**

### **Tables Created**
1. **`users`** - Multi-user support
2. **`degiro_raw_data`** - Raw uploaded CSV files (backup)
3. **`transactions`** - Processed buy/sell/dividend/deposit/fee transactions
4. **`holdings`** - Current stock positions
5. **`stock_prices`** - Historical price data (shared across users)
6. **`portfolio_summary`** - Cached portfolio calculations

### **Key Features**
- **User isolation** - Each user's data is separate
- **ISIN-based matching** - Accurate stock identification
- **Transaction categorization** - Automatic buy/sell/dividend classification
- **Real-time price integration** - Finnhub API for current prices

## 🔄 **Updated Components**

### **HTML Dashboard** (`portfolio_dashboard.html`)
- ➕ **File upload interface** with drag-and-drop
- ➕ **Progress indicators** and status messages
- ➕ **User session management** with unique user IDs
- 🔄 **Data loading** now uses PostgreSQL API endpoints
- 🔄 **Stock analysis** updated for new API structure

### **FastAPI Application** (`app_pg.py`)
- ➕ **File upload endpoint** `/api/upload-degiro-data`
- ➕ **Portfolio data API** `/api/portfolio/{user_id}`
- ➕ **Transactions API** `/api/transactions/{user_id}`
- ➕ **Stock analysis API** `/api/stock-analysis/{user_id}/{symbol}`
- ➕ **Database health checks** for monitoring

### **Deployment Configuration**
- 🔄 **render.yaml** updated with PostgreSQL database
- 🔄 **requirements.txt** includes `asyncpg` and `psycopg2-binary`
- 🔄 **Startup command** now uses `app_pg.py`

## 🛠️ **API Endpoints**

### **File Upload**
```
POST /api/upload-degiro-data
- Accepts CSV file upload
- Processes and stores in PostgreSQL
- Returns processing statistics
```

### **Portfolio Data**
```
GET /api/portfolio/{user_id}
- Returns complete portfolio overview
- Holdings, totals, cash balance
```

### **Transactions**
```
GET /api/transactions/{user_id}?transaction_type=buy
- Returns user transactions by type
- Supports filtering: buy, sell, dividend, deposit, fee
```

### **Stock Analysis**
```
GET /api/stock-analysis/{user_id}/{symbol}?range_type=5Y
- Returns price data + user transactions for symbol
- Supports time ranges: YTD, 6M, 1Y, 3Y, 5Y, ALL
```

### **Status & Health**
```
GET /api/portfolio-status/{user_id}
- Check if user has uploaded data
GET /health
- Application health check for Render
```

## 🔧 **Data Processing Flow**

1. **Upload** - User drags/drops DeGiro CSV file
2. **Parse** - Extract transactions, clean data, categorize
3. **Enrich** - Get current stock prices from Finnhub API
4. **Store** - Save to PostgreSQL with user isolation
5. **Display** - Dashboard loads from database via API

## 🚀 **Deployment Changes**

### **Render Configuration**
```yaml
services:
  - type: web
    name: degiro-dashboard
    startCommand: "python app_pg.py"
    envVars:
      - key: DATABASE_URL
        fromDatabase:
          name: degiro-postgres
          property: connectionString

databases:
  - name: degiro-postgres
    databaseName: degiro_dashboard
    user: degiro_user
    plan: free
```

### **Environment Variables**
- `DATABASE_URL` - PostgreSQL connection string (auto-set by Render)
- `PORT` - Application port (auto-set by Render)
- `RENDER` - Deployment environment flag

## 🧪 **Testing Locally**

### **Prerequisites**
```bash
# Install PostgreSQL locally
brew install postgresql  # macOS
sudo apt install postgresql  # Ubuntu

# Create test database
createdb degiro_test
```

### **Run Tests**
```bash
# Set database URL
export DATABASE_URL="postgresql://localhost:5432/degiro_test"

# Run integration test
python test_postgres.py

# Start application
python app_pg.py
```

## 📊 **User Experience**

### **First Visit**
1. User sees upload interface
2. Drags DeGiro CSV file
3. Processing happens automatically
4. Dashboard populates with data

### **Return Visits**
1. User data persists in database
2. Dashboard loads immediately
3. Can refresh/re-upload as needed

### **Multi-User**
- Each user gets unique session ID
- Data is completely isolated
- No interference between users

## 🔒 **Security & Privacy**

- **No file storage** - CSV content processed immediately
- **User isolation** - Database-level separation
- **Session-based** - Temporary user IDs
- **No persistent sessions** - Each visit gets new ID

## 🚨 **Migration Checklist**

### **Before Deployment**
- [ ] Test PostgreSQL integration locally
- [ ] Verify file upload functionality
- [ ] Check all API endpoints work
- [ ] Test stock analysis features
- [ ] Validate multi-user isolation

### **Render Deployment**
- [ ] Push updated code to GitHub
- [ ] PostgreSQL database provisions automatically
- [ ] Application starts with `app_pg.py`
- [ ] Health checks pass at `/health`
- [ ] File upload interface appears

### **Post-Deployment**
- [ ] Test file upload in production
- [ ] Verify database connectivity
- [ ] Check stock analysis works
- [ ] Validate performance with multiple users

## 🎯 **Benefits Achieved**

✅ **Eliminated CSV file dependency**
✅ **Multi-user capability**  
✅ **Production-ready architecture**
✅ **Better error handling**
✅ **Improved user experience**
✅ **Scalable data storage**
✅ **Real-time processing**

The PostgreSQL migration transforms the DeGiro Dashboard from a single-user, file-based system into a robust, multi-user, database-driven application ready for production deployment on Render.