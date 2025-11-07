import pandas as pd
import logging
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class KPICalculator:
    """
    Gold Layer: Business KPI calculation 
    Implements the 4 required KPIs exactly as specified
    """
    
    def __init__(self, silver_data: dict):
        self.customers = silver_data.get('customers')
        self.orders = silver_data.get('orders')
        self.kpi_results = {}
        
    def calculate_all_kpis(self) -> dict:
        """
        Calculate all 4 required business KPIs
        """
        try:
            logger.info("Calculating business KPIs")
            
            # Calculate each KPI as per requirements
            self.kpi_results = {
                'repeat_customers': self.calculate_repeat_customers(),
                'monthly_trends': self.calculate_monthly_trends(),
                'regional_revenue': self.calculate_regional_revenue(),
                'top_customers_30d': self.calculate_top_customers_last_30_days()
            }
            
            # Add KPI metadata
            self.kpi_results['metadata'] = {
                'calculation_timestamp': datetime.utcnow(),
                'kpis_calculated': list(self.kpi_results.keys()),
                'data_sources': {
                    'customers_count': len(self.customers) if self.customers is not None else 0,
                    'orders_count': len(self.orders) if self.orders is not None else 0
                }
            }
            
            logger.info("All KPIs calculated successfully")
            return self.kpi_results
            
        except Exception as e:
            logger.error(f"KPI calculation failed: {str(e)}")
            raise
            
    def calculate_repeat_customers(self) -> pd.DataFrame:
        """
        KPI 1: Identify customers with more than one order
        Returns: DataFrame with customer_name and number_of_orders
        """
        try:
            if self.orders is None or self.customers is None:
                logger.warning("No data available for repeat customers calculation")
                return pd.DataFrame()
                
            # Count unique orders per customer
            customer_order_counts = self.orders.groupby('mobile_number').agg({
                'order_id': 'nunique'
            }).reset_index()
            
            customer_order_counts.columns = ['mobile_number', 'number_of_orders']
            
            # Filter for repeat customers (more than 1 order)
            repeat_customers = customer_order_counts[customer_order_counts['number_of_orders'] > 1]
            
            # Enrich with customer names
            if len(repeat_customers) > 0:
                repeat_customers = repeat_customers.merge(
                    self.customers[['mobile_number', 'customer_name']],
                    on='mobile_number',
                    how='left'
                )
                
                # Select and order columns as per requirements
                repeat_customers = repeat_customers[['customer_name', 'number_of_orders']]
                repeat_customers = repeat_customers.sort_values('number_of_orders', ascending=False)
            else:
                repeat_customers = pd.DataFrame(columns=['customer_name', 'number_of_orders'])
            
            logger.debug(f"Found {len(repeat_customers)} repeat customers")
            return repeat_customers.reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"Repeat customers calculation failed: {str(e)}")
            return pd.DataFrame()
            
    def calculate_monthly_trends(self) -> pd.DataFrame:
        """
        KPI 2: Aggregate orders by month to observe trends
        Returns: DataFrame with month, total_orders, total_revenue
        """
        try:
            if self.orders is None:
                logger.warning("No order data available for monthly trends calculation")
                return pd.DataFrame()
                
            # Ensure we have the required datetime column
            if 'order_date_time' not in self.orders.columns:
                logger.error("order_date_time column missing for monthly trends")
                return pd.DataFrame()
                
            # Extract month from order date
            monthly_data = self.orders.copy()
            monthly_data['month'] = monthly_data['order_date_time'].dt.to_period('M')
            
            # Aggregate by month
            monthly_trends = monthly_data.groupby('month').agg({
                'order_id': 'nunique',
                'total_amount': 'sum'
            }).reset_index()
            
            monthly_trends.columns = ['month', 'total_orders', 'total_revenue']
            monthly_trends['month'] = monthly_trends['month'].astype(str)
            monthly_trends = monthly_trends.sort_values('month')
            
            logger.debug(f"Calculated monthly trends for {len(monthly_trends)} months")
            return monthly_trends.reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"Monthly trends calculation failed: {str(e)}")
            return pd.DataFrame()
            
    def calculate_regional_revenue(self) -> pd.DataFrame:
        """
        KPI 3: Sum of total_amount grouped by region
        Returns: DataFrame with region and regional_revenue
        """
        try:
            if self.orders is None:
                logger.warning("No order data available for regional revenue calculation")
                return pd.DataFrame()
                
            # Check if region information is available in orders
            if 'region' not in self.orders.columns:
                # Try to get region from customers data
                if self.customers is not None and 'region' in self.customers.columns:
                    orders_with_region = self.orders.merge(
                        self.customers[['mobile_number', 'region']],
                        on='mobile_number',
                        how='left'
                    )
                else:
                    logger.error("Region information not available")
                    return pd.DataFrame()
            else:
                orders_with_region = self.orders
                
            # Aggregate revenue by region
            regional_revenue = orders_with_region.groupby('region').agg({
                'total_amount': 'sum'
            }).reset_index()
            
            regional_revenue.columns = ['region', 'regional_revenue']
            regional_revenue = regional_revenue.sort_values('regional_revenue', ascending=False)
            
            logger.debug(f"Calculated regional revenue for {len(regional_revenue)} regions")
            return regional_revenue.reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"Regional revenue calculation failed: {str(e)}")
            return pd.DataFrame()
            
    def calculate_top_customers_last_30_days(self) -> pd.DataFrame:
        """
        KPI 4: Rank customers by total spend in the last 30 days
        Returns: DataFrame with customer_name and recent_spend
        """
        try:
            if self.orders is None or self.customers is None:
                logger.warning("No data available for top customers calculation")
                return pd.DataFrame()
                
            # Calculate cutoff date (last 30 days)
            if self.orders.empty:
                return pd.DataFrame()
                
            latest_order_date = self.orders['order_date_time'].max()
            cutoff_date = latest_order_date - timedelta(days=30)
            
            # Filter orders from last 30 days
            recent_orders = self.orders[self.orders['order_date_time'] >= cutoff_date]
            
            if recent_orders.empty:
                logger.info("No orders found in the last 30 days")
                return pd.DataFrame()
                
            # Aggregate spend by customer
            customer_spend = recent_orders.groupby('mobile_number').agg({
                'total_amount': 'sum'
            }).reset_index()
            
            customer_spend.columns = ['mobile_number', 'recent_spend']
            
            # Get top 10 customers by spend
            top_customers = customer_spend.nlargest(10, 'recent_spend')
            
            # Enrich with customer names
            top_customers = top_customers.merge(
                self.customers[['mobile_number', 'customer_name']],
                on='mobile_number',
                how='left'
            )
            
            # Select and order columns
            top_customers = top_customers[['customer_name', 'recent_spend']]
            top_customers = top_customers.sort_values('recent_spend', ascending=False)
            
            logger.debug(f"Found {len(top_customers)} top customers in last 30 days")
            return top_customers.reset_index(drop=True)
            
        except Exception as e:
            logger.error(f"Top customers calculation failed: {str(e)}")
            return pd.DataFrame()
            
    def get_kpi_summary(self) -> dict:
        """
        Get summary of all KPI calculations
        """
        summary = {
            'timestamp': datetime.utcnow(),
            'kpis_calculated': len(self.kpi_results) - 1,  # Exclude metadata
            'details': {}
        }
        
        for kpi_name, kpi_data in self.kpi_results.items():
            if kpi_name != 'metadata':
                summary['details'][kpi_name] = {
                    'records_count': len(kpi_data) if hasattr(kpi_data, '__len__') else 0,
                    'data_available': not kpi_data.empty if hasattr(kpi_data, 'empty') else True
                }
                
        return summary