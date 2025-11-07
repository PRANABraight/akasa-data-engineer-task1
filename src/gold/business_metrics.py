
import pandas as pd
import logging
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class BusinessMetrics:
    """
    Gold Layer: Additional business metrics beyond required KPIs
    Provides enhanced analytics and insights
    """
    
    def __init__(self, silver_data: dict):
        self.customers = silver_data.get('customers')
        self.orders = silver_data.get('orders')
        self.additional_metrics = {}
        
    def calculate_customer_segmentation(self) -> pd.DataFrame:
        """
        Segment customers based on spending behavior
        """
        try:
            if self.orders is None or self.customers is None:
                return pd.DataFrame()
                
            # Calculate customer lifetime value
            customer_metrics = self.orders.groupby('mobile_number').agg({
                'total_amount': ['sum', 'mean', 'count'],
                'order_date_time': ['min', 'max']
            }).reset_index()
            
            # Flatten column names
            customer_metrics.columns = [
                'mobile_number', 'total_spent', 'avg_order_value', 
                'order_count', 'first_order', 'last_order'
            ]
            
            # Calculate additional metrics
            customer_metrics['customer_lifetime_days'] = (
                customer_metrics['last_order'] - customer_metrics['first_order']
            ).dt.days
            
            # Segment customers
            conditions = [
                customer_metrics['total_spent'] >= customer_metrics['total_spent'].quantile(0.8),
                customer_metrics['total_spent'] >= customer_metrics['total_spent'].quantile(0.5),
                customer_metrics['total_spent'] >= customer_metrics['total_spent'].quantile(0.2)
            ]
            choices = ['VIP', 'Regular', 'Occasional']
            customer_metrics['segment'] = np.select(conditions, choices, default='New')
            
            # Merge with customer data
            customer_segmentation = customer_metrics.merge(
                self.customers[['mobile_number', 'customer_name', 'region']],
                on='mobile_number',
                how='left'
            )
            
            self.additional_metrics['customer_segmentation'] = customer_segmentation
            return customer_segmentation
            
        except Exception as e:
            logger.error(f"Customer segmentation failed: {e}")
            return pd.DataFrame()
            
    def calculate_product_analysis(self) -> pd.DataFrame:
        """
        Analyze product (SKU) performance
        """
        try:
            if self.orders is None:
                return pd.DataFrame()
                
            product_analysis = self.orders.groupby('sku_id').agg({
                'order_id': 'count',
                'sku_count': 'sum',
                'total_amount': 'sum'
            }).reset_index()
            
            product_analysis.columns = ['sku_id', 'orders_count', 'total_units_sold', 'total_revenue']
            product_analysis['avg_revenue_per_unit'] = product_analysis['total_revenue'] / product_analysis['total_units_sold']
            product_analysis = product_analysis.sort_values('total_revenue', ascending=False)
            
            self.additional_metrics['product_analysis'] = product_analysis
            return product_analysis
            
        except Exception as e:
            logger.error(f"Product analysis failed: {e}")
            return pd.DataFrame()
            
    def calculate_seasonal_trends(self) -> pd.DataFrame:
        """
        Analyze seasonal patterns in orders
        """
        try:
            if self.orders is None:
                return pd.DataFrame()
                
            seasonal_data = self.orders.copy()
            seasonal_data['month'] = seasonal_data['order_date_time'].dt.month
            seasonal_data['day_of_week'] = seasonal_data['order_date_time'].dt.day_name()
            seasonal_data['hour_of_day'] = seasonal_data['order_date_time'].dt.hour
            
            # Monthly trends
            monthly_trends = seasonal_data.groupby('month').agg({
                'order_id': 'count',
                'total_amount': 'sum'
            }).reset_index()
            
            # Day of week trends
            dow_trends = seasonal_data.groupby('day_of_week').agg({
                'order_id': 'count',
                'total_amount': 'sum'
            }).reset_index()
            
            # Hourly trends
            hourly_trends = seasonal_data.groupby('hour_of_day').agg({
                'order_id': 'count',
                'total_amount': 'sum'
            }).reset_index()
            
            seasonal_analysis = {
                'monthly_trends': monthly_trends,
                'dow_trends': dow_trends,
                'hourly_trends': hourly_trends
            }
            
            self.additional_metrics['seasonal_analysis'] = seasonal_analysis
            return seasonal_analysis
            
        except Exception as e:
            logger.error(f"Seasonal analysis failed: {e}")
            return {}
            
    def get_all_additional_metrics(self) -> dict:
        """
        Calculate all additional business metrics
        """
        try:
            logger.info("Calculating additional business metrics")
            
            self.additional_metrics = {
                'customer_segmentation': self.calculate_customer_segmentation(),
                'product_analysis': self.calculate_product_analysis(),
                'seasonal_analysis': self.calculate_seasonal_trends()
            }
            
            # Add metadata
            self.additional_metrics['metadata'] = {
                'calculation_timestamp': datetime.utcnow(),
                'metrics_calculated': list(self.additional_metrics.keys())
            }
            
            return self.additional_metrics
            
        except Exception as e:
            logger.error(f"Additional metrics calculation failed: {e}")
            return {}
