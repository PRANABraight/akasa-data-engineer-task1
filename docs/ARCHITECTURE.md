# 🏗️ Technical Architecture & Design Decisions

## System Architecture Overview

```
Raw Data Sources
      ↓
┌─────────────────┐
│   BRONZE LAYER  │ → Raw Data Preservation
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

## Key Design Decisions

### 1. **Medallion Architecture Pattern**
**Decision**: Implement Bronze-Silver-Gold layers instead of traditional ETL
**Rationale**:
- Maintains raw data history for audit purposes
- Enables incremental data quality improvements
- Supports reprocessing without losing source data
- Industry standard for modern data platforms

### 2. **Dual Processing Engine**
**Decision**: Support both in-memory and database approaches
**Rationale**:
- **In-Memory (Pandas)**: Fast development, zero dependencies
- **Database (MySQL)**: Production scalability, SQL power
- Provides flexibility for different deployment scenarios

### 3. **Modular Component Design**
```
src/
├── bronze/          # Data ingestion
├── silver/          # Data quality  
├── gold/            # Business logic
└── presentation/    # Visualization
```
**Rationale**: Separation of concerns, easier testing, team collaboration

## Data Flow Details

### Bronze Layer (Raw Data)
```python
# Design: Preserve source data exactly
class CSVIngestor:
    def load_raw_data() -> pd.DataFrame:
        # Preserve all fields as strings initially
        # Add metadata: ingestion_timestamp, source_file, record_hash
```

### Silver Layer (Trusted Data)
```python
# Design: Incremental data quality
class SilverProcessor:
    def process_to_silver():
        # 1. Clean (handle missing values, standardize formats)
        # 2. Validate (business rules, constraints)  
        # 3. Enrich (add derived fields, business context)
```

### Gold Layer (Business Data)
```python
# Design: Business-focused metrics
class GoldProcessor:
    def process_to_gold():
        # 1. Calculate KPIs (repeat customers, trends, etc.)
        # 2. Generate visualizations
        # 3. Create business reports
```
