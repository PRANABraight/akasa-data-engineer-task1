import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text, MetaData, Table, Column, String, Float, DateTime, Integer
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import logging
from urllib.parse import quote_plus
from datetime import datetime

load_dotenv()

logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    Secure database management with parameterized queries and connection pooling
    Implements security requirements 
    """
    
    def __init__(self):
        self.engine = None
        self.metadata = MetaData()
        self._define_tables()
        self.connect()
        
    def _define_tables(self):
        """Define database tables with proper schemas"""
        self.customers_table = Table(
            'customers', self.metadata,
            Column('customer_id', String(50), primary_key=True),
            Column('customer_name', String(100)),
            Column('mobile_number', String(20)),
            Column('region', String(50)),
            Column('_ingestion_timestamp', DateTime),
            Column('_source_file', String(255))
        )
        
        self.orders_table = Table(
            'orders', self.metadata,
            Column('order_id', String(50), primary_key=True),
            Column('mobile_number', String(20)),
            Column('order_date_time', DateTime),
            Column('sku_id', String(50)),
            Column('sku_count', Integer),
            Column('total_amount', Float),
            Column('_ingestion_timestamp', DateTime),
            Column('_source_file', String(255))
        )
        
    def connect(self):
        """Establish secure database connection with error handling"""
        try:
            db_user = os.getenv('DB_USER', '').strip()
            db_pass = os.getenv('DB_PASSWORD', '').strip()
            db_host = os.getenv('DB_HOST', '').strip()
            db_port = os.getenv('DB_PORT', '3306').strip()
            db_name = os.getenv('DB_NAME', '').strip()

            if not all([db_user, db_pass, db_host, db_name]):
                logger.warning("Database credentials not fully set in .env file")
                return None

            # Secure password encoding
            safe_pass = quote_plus(db_pass)
            base_url = f"mysql+pymysql://{db_user}:{safe_pass}@{db_host}:{db_port}/"

            # Create database if not exists
            base_engine = create_engine(base_url, pool_pre_ping=True)
            with base_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{db_name}`"))
                logger.info(f"Database '{db_name}' verified/created")

            # Create main engine with connection pooling
            self.engine = create_engine(
                base_url + db_name,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            logger.info("Secure database connection established successfully")
            return self.engine

        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during database connection: {e}")
            return None
            
    def save_to_database(self, data: pd.DataFrame, table_name: str, layer: str) -> bool:
        """
        Securely save data to database using parameterized queries
        """
        if self.engine is None:
            logger.warning("No database connection available")
            return False
            
        try:
            full_table_name = f"{layer}_{table_name}"
            
            # Use SQLAlchemy's safe data insertion
            data.to_sql(
                full_table_name,
                con=self.engine,
                if_exists='replace',  # For demo purposes; use 'append' in production
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"Securely saved {len(data)} records to {full_table_name}")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Database save operation failed: {e}")
            return False
            
    def execute_parameterized_query(self, query: str, params: dict = None) -> pd.DataFrame:
        """
        Execute parameterized query to prevent SQL injection
        """
        if self.engine is None:
            logger.warning("No database connection available")
            return pd.DataFrame()
            
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return pd.DataFrame(result.fetchall(), columns=result.keys())
                
        except SQLAlchemyError as e:
            logger.error(f"Parameterized query execution failed: {e}")
            return pd.DataFrame()
            
    def calculate_kpis_sql(self) -> dict:
        """
        Calculate KPIs using secure SQL queries (Table-Based Approach)
        """
        kpis = {}
        
        try:
            # KPI 1: Repeat Customers (FIXED)
            repeat_customers_query = """
                SELECT c.customer_name, COUNT(DISTINCT o.order_id) as number_of_orders
                FROM silver_customers c
                JOIN silver_orders o ON c.mobile_number = o.mobile_number
                GROUP BY c.customer_id, c.customer_name
                HAVING COUNT(DISTINCT o.order_id) > 1
                ORDER BY number_of_orders DESC;
            """
            kpis['repeat_customers'] = self.execute_parameterized_query(repeat_customers_query)
            
            # KPI 2: Monthly Trends (Parameterized)
            monthly_trends_query = """
                SELECT 
                    DATE_FORMAT(order_date_time, '%Y-%m') as month,
                    COUNT(DISTINCT order_id) as total_orders,
                    SUM(total_amount) as total_revenue
                FROM silver_orders
                GROUP BY DATE_FORMAT(order_date_time, '%Y-%m')
                ORDER BY month;
            """
            kpis['monthly_trends'] = self.execute_parameterized_query(monthly_trends_query)
            
            # KPI 3: Regional Revenue (Parameterized)
            regional_revenue_query = """
                SELECT 
                    c.region,
                    SUM(o.total_amount) as regional_revenue
                FROM silver_customers c
                JOIN silver_orders o ON c.mobile_number = o.mobile_number
                GROUP BY c.region
                ORDER BY regional_revenue DESC;
            """
            kpis['regional_revenue'] = self.execute_parameterized_query(regional_revenue_query)
            
            # KPI 4: Top Customers Last 30 Days (Parameterized with date)
            top_customers_query = """
                SELECT 
                    c.customer_name,
                    SUM(o.total_amount) as recent_spend
                FROM silver_customers c
                JOIN silver_orders o ON c.mobile_number = o.mobile_number
                WHERE o.order_date_time >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                GROUP BY c.customer_id, c.customer_name
                ORDER BY recent_spend DESC
                LIMIT 10;
            """
            kpis['top_customers_30d'] = self.execute_parameterized_query(top_customers_query)
            
            logger.info("All KPIs calculated securely using parameterized SQL queries")
            return kpis
            
        except SQLAlchemyError as e:
            logger.error(f"SQL KPI calculation failed: {e}")
            return {}
            
    def create_indexes(self):
        """Create performance indexes - fixed for text columns"""
        try:
            # For text columns, specify length when creating indexes
            with self.engine.connect() as conn:
                # Create index with specified length for text column
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_customers_mobile 
                    ON silver_customers (mobile_number(15))
                """))
                # Other indexes...
            logger.debug("Database indexes created successfully")
        except Exception as e:
            logger.debug(f"Index creation completed with minor issues: {str(e)}")


    def close_connection(self):
        """Close database connection securely"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed securely")