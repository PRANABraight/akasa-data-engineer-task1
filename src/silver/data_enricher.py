
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataEnricher:
    """
    Silver Layer: Data enrichment and feature engineering
    Adds derived fields and business context
    """
    
    def __init__(self):
        self.enrichment_stats = {}
        
    def enrich_customers_data(self, cleaned_customers: pd.DataFrame, orders_data: pd.DataFrame = None) -> pd.DataFrame:
        """
        Enrich customer data with additional features and derived fields
        """
        try:
            logger.info("Starting customer data enrichment")
            
            # Create enriched copy
            customers_enriched = cleaned_customers.copy()
            
            # Add customer segmentation based on name analysis
            customers_enriched['name_length'] = customers_enriched['customer_name'].str.len()
            customers_enriched['word_count'] = customers_enriched['customer_name'].str.split().str.len()
            
            # Add region grouping - FIXED: Create region_group here
            region_grouping = {
                'North': 'Northern',
                'South': 'Southern', 
                'East': 'Eastern',
                'West': 'Western',
                'Unknown': 'Other'
            }
            customers_enriched['region_group'] = customers_enriched['region'].map(region_grouping)
            
            # Add enrichment metadata
            customers_enriched['_enriched_at'] = datetime.utcnow()
            customers_enriched['_enrichment_version'] = '1.0'
            
            self.enrichment_stats['customers'] = {
                'original_columns': len(cleaned_customers.columns),
                'enriched_columns': len(customers_enriched.columns),
                'enriched_at': datetime.utcnow()
            }
            
            logger.info(f"Customer enrichment completed: added {len(customers_enriched.columns) - len(cleaned_customers.columns)} new columns")
            return customers_enriched
            
        except Exception as e:
            logger.error(f"Customer data enrichment failed: {e}")
            raise
            
    def enrich_orders_data(self, cleaned_orders: pd.DataFrame, customers_data: pd.DataFrame = None) -> pd.DataFrame:
        """
        Enrich order data with temporal features and business context
        FIXED: Handle missing region_group column gracefully
        """
        try:
            logger.info("Starting order data enrichment")
            
            # Create enriched copy
            orders_enriched = cleaned_orders.copy()
            
            # Extract comprehensive temporal features
            orders_enriched['order_date'] = orders_enriched['order_date_time'].dt.date
            orders_enriched['order_year'] = orders_enriched['order_date_time'].dt.year
            orders_enriched['order_month'] = orders_enriched['order_date_time'].dt.month
            orders_enriched['order_quarter'] = orders_enriched['order_date_time'].dt.quarter
            orders_enriched['order_day'] = orders_enriched['order_date_time'].dt.day
            orders_enriched['order_day_of_week'] = orders_enriched['order_date_time'].dt.day_name()
            orders_enriched['order_hour'] = orders_enriched['order_date_time'].dt.hour
            orders_enriched['order_week'] = orders_enriched['order_date_time'].dt.isocalendar().week
            
            # Calculate derived business metrics
            orders_enriched['avg_item_price'] = orders_enriched['total_amount'] / orders_enriched['sku_count']
            orders_enriched['order_size_category'] = pd.cut(
                orders_enriched['total_amount'],
                bins=[0, 100, 500, 1000, float('inf')],
                labels=['Small', 'Medium', 'Large', 'VIP'],
                right=False
            )
            
            # Time-based categorizations
            orders_enriched['time_of_day'] = pd.cut(
                orders_enriched['order_hour'],
                bins=[0, 6, 12, 18, 24],
                labels=['Night', 'Morning', 'Afternoon', 'Evening'],
                right=False
            )
            
            # Add customer enrichment if available - FIXED: Safe column access
            if customers_data is not None:
                # Select only the columns that definitely exist in customers_data
                customer_columns_to_merge = ['mobile_number', 'customer_name', 'region']
                
                # Check if region_group exists before including it
                if 'region_group' in customers_data.columns:
                    customer_columns_to_merge.append('region_group')
                    logger.info("Including region_group in order enrichment")
                else:
                    logger.warning("region_group not found in customers data, skipping for order enrichment")
                
                orders_enriched = orders_enriched.merge(
                    customers_data[customer_columns_to_merge],
                    on='mobile_number',
                    how='left',
                    suffixes=('', '_customer')
                )
            
            # Add enrichment metadata
            orders_enriched['_enriched_at'] = datetime.utcnow()
            orders_enriched['_enrichment_version'] = '1.0'
            
            self.enrichment_stats['orders'] = {
                'original_columns': len(cleaned_orders.columns),
                'enriched_columns': len(orders_enriched.columns),
                'enriched_at': datetime.utcnow()
            }
            
            logger.info(f"Order enrichment completed: added {len(orders_enriched.columns) - len(cleaned_orders.columns)} new columns")
            return orders_enriched
            
        except Exception as e:
            logger.error(f"Order data enrichment failed: {e}")
            raise
            
    def get_enrichment_stats(self) -> dict:
        """Get enrichment statistics"""
        return self.enrichment_stats.copy()