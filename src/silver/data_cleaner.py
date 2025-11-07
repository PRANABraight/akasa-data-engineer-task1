
import pandas as pd
import logging
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

class DataCleaner:
    """
    Silver Layer: Data cleaning and standardization
    Implements data quality rules and normalization
    """
    
    def __init__(self):
        self.cleaning_stats = {}
        
    def clean_customers_data(self, raw_customers: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize customer data
        """
        try:
            logger.info("Starting customer data cleaning")
            initial_count = len(raw_customers)
            
            # Create working copy
            customers_clean = raw_customers.copy()
            
            # Remove bronze metadata columns for cleaning
            metadata_cols = [col for col in customers_clean.columns if col.startswith('_')]
            customers_data = customers_clean.drop(columns=metadata_cols)
            
            # Standardize column names
            customers_data.columns = customers_data.columns.str.strip().str.lower()
            
            # Handle missing values - remove records with critical missing data
            customers_data = customers_data.dropna(subset=['customer_id', 'mobile_number'])
            
            # Standardize data types and formats
            customers_data['customer_id'] = customers_data['customer_id'].astype(str).str.strip()
            customers_data['customer_name'] = customers_data['customer_name'].astype(str).str.strip().str.title()
            customers_data['mobile_number'] = customers_data['mobile_number'].astype(str).str.strip()
            customers_data['region'] = customers_data['region'].astype(str).str.strip().str.title()
            
            # Remove duplicates based on customer_id
            customers_data = customers_data.drop_duplicates(subset=['customer_id'], keep='first')
            
            # Validate and clean mobile numbers (keep only digits)
            customers_data['mobile_number'] = customers_data['mobile_number'].str.replace(r'\D', '', regex=True)
            
            # Standardize regions
            valid_regions = ['North', 'South', 'East', 'West']
            customers_data['region'] = customers_data['region'].apply(
                lambda x: x if x in valid_regions else 'Unknown'
            )
            
            # Add cleaning metadata
            final_count = len(customers_data)
            self.cleaning_stats['customers'] = {
                'initial_records': initial_count,
                'final_records': final_count,
                'records_removed': initial_count - final_count,
                'cleaning_timestamp': datetime.utcnow()
            }
            
            logger.info(f"Customer cleaning completed: {final_count}/{initial_count} records retained")
            return customers_data
            
        except Exception as e:
            logger.error(f"Customer data cleaning failed: {e}")
            raise
            
    def clean_orders_data(self, raw_orders: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize order data
        """
        try:
            logger.info("Starting order data cleaning")
            initial_count = len(raw_orders)
            
            # Create working copy
            orders_clean = raw_orders.copy()
            
            # Remove bronze metadata columns for cleaning
            metadata_cols = [col for col in orders_clean.columns if col.startswith('_')]
            orders_data = orders_clean.drop(columns=metadata_cols)
            
            # Standardize column names
            orders_data.columns = orders_data.columns.str.strip().str.lower()
            
            # Handle missing values - remove records with critical missing data
            orders_data = orders_data.dropna(subset=['order_id', 'mobile_number', 'order_date_time'])
            
            # Convert and validate data types
            orders_data['order_id'] = orders_data['order_id'].astype(str).str.strip()
            orders_data['mobile_number'] = orders_data['mobile_number'].astype(str).str.strip()
            orders_data['sku_id'] = orders_data['sku_id'].astype(str).str.strip()
            
            # Clean mobile numbers
            orders_data['mobile_number'] = orders_data['mobile_number'].str.replace(r'\D', '', regex=True)
            
            # Safe numeric conversions with error handling
            orders_data['sku_count'] = pd.to_numeric(orders_data['sku_count'], errors='coerce')
            orders_data['total_amount'] = pd.to_numeric(orders_data['total_amount'], errors='coerce')
            
            # Handle datetime conversion with timezone awareness
            orders_data['order_date_time'] = pd.to_datetime(
                orders_data['order_date_time'], 
                utc=True,
                errors='coerce'
            )
            
            # Remove invalid records
            valid_orders = orders_data[
                (orders_data['sku_count'] > 0) & 
                (orders_data['total_amount'] > 0) &
                (orders_data['order_date_time'].notna())
            ].copy()
            
            # Remove future dates (data quality check)
            current_time = pd.Timestamp.now(tz='UTC')
            valid_orders = valid_orders[valid_orders['order_date_time'] <= current_time]
            
            # Add cleaning metadata
            final_count = len(valid_orders)
            self.cleaning_stats['orders'] = {
                'initial_records': initial_count,
                'final_records': final_count,
                'records_removed': initial_count - final_count,
                'cleaning_timestamp': datetime.utcnow()
            }
            
            logger.info(f"Order cleaning completed: {final_count}/{initial_count} records retained")
            return valid_orders
            
        except Exception as e:
            logger.error(f"Order data cleaning failed: {e}")
            raise
            
    def get_cleaning_stats(self) -> dict:
        """Get cleaning statistics"""
        return self.cleaning_stats.copy()
