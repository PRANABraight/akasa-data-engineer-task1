# 🔧 Setup Guide

## Prerequisites
- **Python 3.8+**
- **MySQL 8.0+** (Optional - for database approach)

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run Pipeline
```bash
python main.py
```

## Database Setup (Optional)

### 1. Create `.env` file
```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=akasa_analytics
```

### 2. MySQL Setup
```sql
CREATE DATABASE akasaair_analytics;
```

## File Structure Expected
```
akasa-data-engineer/
├── assets/
│   ├── task_DE_new_customers.csv
│   └── task_DE_new_orders.xml
├── data/ (auto-created)
├── assets/analytics_dashboards/ (auto-created)
└── logs/ (auto-created)
```

## Verify Installation
1. Run `python main.py`
2. Check for "Pipeline execution completed" message
3. Look for generated dashboard in `assets/analytics_dashboards/`

**Troubleshooting**: Ensure CSV and XML files are in `assets/` folder