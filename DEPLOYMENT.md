# DeGiro Dashboard - Render Deployment Guide

## 🚀 Quick Deployment Steps

### 1. Push to GitHub
```bash
# Initialize git repository (if not already done)
git init
git add .
git commit -m "Initial commit for Render deployment"

# Push to GitHub
git remote add origin https://github.com/yourusername/degiro-analyzer.git
git branch -M main
git push -u origin main
```

### 2. Deploy on Render
1. Go to [render.com](https://render.com) and sign up/login
2. Click "New +" → "Web Service"
3. Connect your GitHub repository
4. Render will automatically detect the `render.yaml` configuration
5. Click "Deploy"

### 3. Upload Portfolio Data (After Deployment)
Since your portfolio CSV files contain personal data, you'll need to upload them after deployment:

1. Access your deployed dashboard
2. Use the web interface to upload your DeGiro CSV files
3. The system will automatically process them and initialize the stock database

## 📁 Files Created for Deployment

- **`app.py`** - Unified FastAPI application combining dashboard and API
- **`render.yaml`** - Render deployment configuration
- **`requirements.txt`** - Python dependencies (updated for deployment)
- **`init_db.py`** - Database initialization script
- **`.gitignore`** - Git ignore rules (excludes personal CSV files)

## 🔧 Environment Variables

The following environment variables are automatically set by Render:
- `PORT` - Server port (set by Render)
- `RENDER` - Deployment environment flag
- `PYTHON_VERSION` - Python version (3.11.9)

## 🏥 Health Check

The deployment includes a health check endpoint at `/health` that:
- Verifies the application is running
- Checks database connectivity
- Reports database statistics

## 📊 Features Available After Deployment

1. **Portfolio Overview**
   - Total portfolio value
   - Cash balance
   - Asset allocation charts

2. **Individual Stock Analysis**
   - Historical price charts (from 2015)
   - Transaction markers (buy/sell points)
   - Time range filters (YTD, 6M, 1Y, 3Y, 5Y, ALL)

3. **Multi-User Access**
   - Each user can upload their own DeGiro data
   - Isolated data processing per session

## 🔒 Data Security

- Personal CSV files are not included in the Git repository
- Data is processed server-side and not shared between users
- SQLite database is created per deployment instance

## 🛠️ Troubleshooting

### If the deployment fails:
1. Check Render build logs for specific error messages
2. Ensure all required files are present in the repository
3. Verify the GitHub repository is accessible to Render

### If stock analysis isn't working:
1. Upload your DeGiro CSV files through the web interface
2. Wait for the database to initialize (may take a few minutes)
3. Check the `/health` endpoint for database status

### For local testing:
```bash
# Install dependencies
pip install -r requirements.txt

# Initialize database (optional - will be empty without CSV files)
python init_db.py

# Run the application
python app.py

# Access at http://localhost:8000
```

## 🎯 Next Steps After Deployment

1. **Upload Data**: Use the web interface to upload your DeGiro CSV files
2. **Test Features**: Verify portfolio overview and stock analysis work correctly  
3. **Share URL**: Your dashboard will be available at `https://your-app-name.onrender.com`
4. **Monitor**: Use the `/health` endpoint to monitor application status

## 📞 Support

If you encounter issues:
1. Check Render deployment logs
2. Verify CSV file formats match expected structure
3. Test locally first to isolate deployment-specific issues