# Akasa Air - Medallion Architecture Data Platform

## 🏢 Enterprise Data Analytics Solution

A production-grade data engineering platform implementing the Medallion Architecture for comprehensive customer and order analytics. This scalable solution processes multi-format data sources through Bronze, Silver, and Gold layers to deliver actionable business intelligence.

![Medallion Architecture](https://img.shields.io/badge/Architecture-Medallion-blue)
![Python](https://img.shields.io/badge/Python-3.8%2B-green)
![MySQL](https://img.shields.io/badge/MySQL-8.0%2B-orange)

## 🎯 Business Value Proposition

**Transform raw customer and order data into strategic business insights** through automated data pipelines that ensure data quality, reliability, and actionable intelligence delivery.

## 📊 Core Business KPIs

| KPI | Business Impact | Use Case |
|-----|-----------------|----------|
| **Repeat Customer Analysis** | Customer retention & loyalty programs | Identify high-value returning customers |
| **Monthly Business Trends** | Revenue forecasting & performance tracking | Time-series analysis of order patterns |
| **Regional Revenue Distribution** | Market penetration & resource allocation | Geographic performance optimization |
| **Top Customers (30-Day)** | VIP customer identification | Targeted marketing & personalized offers |

## 🏗️ Technical Architecture

### Medallion Architecture Implementation

```
Raw Data Sources
      ↓
┌─────────────────┐
│   BRONZE LAYER  │ → Raw Data Ingestion & Preservation
│  - CSV/XML      │ → Schema Validation
│  - Audit Trail  │ → Source System Metadata
└─────────────────┘
      ↓
┌─────────────────┐
│   SILVER LAYER  │ → Data Cleaning & Validation
│  - Data Quality │ → Business Rule Enforcement
│  - Enrichment   │ → Feature Engineering
└─────────────────┘
      ↓
┌─────────────────┐
│    GOLD LAYER   │ → Business Intelligence
│  - KPI Calculation │ → Advanced Analytics
│  - Visualization │ → Executive Reporting
└─────────────────┘
```

### Dual Processing Engine

| Approach | Use Case | Advantages |
|----------|----------|------------|
| **In-Memory (Pandas)** | Rapid development & testing | Zero dependencies, instant execution |
| **Database (MySQL)** | Production workloads | Scalability, ACID compliance, SQL power |

## 🚀 Quick Start

### Prerequisites

- **Python 3.8+**
- **MySQL 8.0+** (Optional, for database approach)
- **Required Packages**: `pip install -r requirements.txt`

### Installation & Setup

```bash
# 1. Clone and navigate to project
cd akasa-data-engineer-task1

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment (Optional - for database features)
cp .env.example .env
# Edit .env with your database credentials

# 4. Run the pipeline
python main.py
```

### Environment Configuration

```env
# Database Configuration (Optional)
DB_HOST=localhost
DB_PORT=3306
DB_NAME=akasaair_analytics
DB_USER=your_username
DB_PASSWORD=your_secure_password

# Application Settings
LOG_LEVEL=INFO
DATA_DIR=./data
```

## 📁 Data Sources & Schema

### Customer Data (CSV)
```csv
customer_id,customer_name,mobile_number,region
CUST001,Aarav Mehta,9876543210,North
CUST002,Priya Singh,9876543211,South
```

### Order Data (XML)
```xml
<order>
    <order_id>ORD001</order_id>
    <mobile_number>9876543210</mobile_number>
    <order_date_time>2024-01-15 14:30:00</order_date_time>
    <sku_id>SKU1001</sku_id>
    <sku_count>2</sku_count>
    <total_amount>35720</total_amount>
</order>
```

## 🔧 Core Components

### Bronze Layer - Raw Data Ingestion
- **CSVIngestor**: Customer data processing with schema validation
- **XMLIngestor**: Order data extraction with structure preservation
- **DataLoader**: Coordination of multi-source data ingestion

### Silver Layer - Data Refinement
- **DataCleaner**: Data quality enforcement & standardization
- **DataValidator**: Comprehensive validation rules & integrity checks
- **DataEnricher**: Feature engineering & business context addition

### Gold Layer - Business Intelligence
- **KPICalculator**: Core KPI computation engine
- **BusinessMetrics**: Advanced analytics & customer segmentation
- **Visualization Engine**: Professional dashboard generation
- **ReportGenerator**: Multi-format business reporting

## 📈 Output & Deliverables

### KPI Results
```
BUSINESS KPI RESULTS
==========================================

REPEAT CUSTOMERS:
--------------------------------------------------
customer_name    | number_of_orders
Aarav Mehta      | 2
Priya Singh      | 3

MONTHLY TRENDS:
--------------------------------------------------
month    | total_orders | total_revenue
2024-01  | 15           | 245,800
2024-02  | 18           | 298,450

REGIONAL REVENUE:
--------------------------------------------------
region | regional_revenue
North  | 545,200
South  | 398,750
```

### Visualization Assets
- **Comprehensive Business Dashboard** (`assets/analytics_dashboards/`)
- **Regional Performance Analysis**
- **Customer Segmentation Charts**
- **Revenue Trend Analysis**

### Data Artifacts
- **Bronze**: Raw data preservation (Parquet format)
- **Silver**: Cleaned & enriched datasets
- **Gold**: KPI results & business metrics

## 🛡️ Security & Compliance

### Data Protection
- Environment-based credential management
- Parameterized SQL queries preventing injection
- Input validation & sanitization pipelines
- Secure audit logging

### Access Control
- Database credential isolation
- File system permission enforcement
- Comprehensive audit trails

## 📊 Performance & Monitoring

### Processing Capabilities
- **Bronze Layer**: Multi-format ingestion with validation
- **Silver Layer**: Data quality enforcement & enrichment
- **Gold Layer**: Real-time KPI computation & visualization

### Monitoring Features
- Structured logging with rotation
- Performance metrics tracking
- Data quality validation reporting
- Pipeline health monitoring

## 🔄 Execution Flow

```python
# Complete pipeline execution
pipeline = MedallionArchitecturePipeline()
pipeline.run_complete_pipeline()

# Individual layer execution
bronze_data = pipeline.run_bronze_layer()      # Raw ingestion
silver_data = pipeline.run_silver_layer()      # Cleaning & validation  
gold_data = pipeline.run_gold_layer()          # KPI calculation
```

## 🎨 Visualization Features

### Business Dashboards
- **Regional Revenue Distribution**
- **Customer Segmentation Analysis**
- **Monthly Performance Trends**
- **Top Customer Identification**

### Export Formats
- High-resolution PNG dashboards
- Text-based business reports
- JSON-structured data exports
- Database-persisted results

## 🔧 Extension Framework

### Adding New KPIs
```python
class ExtendedKPICalculator(KPICalculator):
    def calculate_customer_lifetime_value(self):
        """
        Template for new KPI implementation
        """
        # Data extraction
        # Business logic application  
        # Result formatting
        return kpi_dataframe
```

### Integration Opportunities
- **Data Warehouses**: Snowflake, BigQuery, Redshift
- **BI Tools**: Tableau, Power BI, Looker
- **Orchestration**: Apache Airflow, Prefect, Dagster
- **APIs**: RESTful endpoints for external consumption

## 📞 Support & Maintenance

### Logging & Diagnostics
- Comprehensive log files in `logs/` directory
- Detailed error tracking with tracebacks
- Performance timing metrics
- Data validation summaries

### Troubleshooting
```bash
# Check pipeline health
tail -f logs/akasaair_processing.log

# Verify data quality
python -m src.utils.data_validator

# Test individual components
python -m src.bronze.data_loader
```



---

**Built with ❤️ by Pranab**