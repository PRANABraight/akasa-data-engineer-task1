import pandas as pd
from datetime import datetime, timedelta
import logging


logger = logging.getLogger(__name__)

class InMemoryProcessor:
    """
    Process data using Pandas (in computer memory)
    """
    
    def __init__(self, customers_df, orders_df):
        """Initialize with data"""
        self.customers = customers_df
        self.orders = orders_df
        
        # Join customers with orders (like SQL JOIN)
        self.enriched_orders = self.orders.merge(
            self.customers,
            on='mobile_number',
            how='left'
        )
        
        logger.info(" Data loaded into memory")
    
    def get_repeat_customers(self):
        """KPI 1: Find customers with more than one order"""
        
        # 1. Repeat Customers (customers with more than 1 order)
        # Group by customer and count their unique order IDs
        order_counts = self.enriched_orders.groupby('customer_name')['order_id'].nunique()
        
        # Filter for customers with more than 1 unique order
        repeat_customers_series = order_counts[order_counts > 1]

        repeat_df = pd.DataFrame({
            'customer_name': repeat_customers_series.index,
            'number_of_orders': repeat_customers_series.values
        })
        
        
        repeat_df = repeat_df.sort_values('number_of_orders', ascending=False)
        
        logger.info(f"Found {len(repeat_df)} repeat customers")
        return repeat_df


    def get_monthly_trends(self):
        """KPI 2: Get order trends by month"""
        
        # Step 1: Extract month from date
        self.enriched_orders['month'] = self.enriched_orders['order_date_time'].dt.to_period('M')
        
        # Step 2: Group by month and aggregate
        monthly_sales = self.enriched_orders.groupby('month').agg({
            'order_id': 'count',
            'total_amount': 'sum'
        }).reset_index()
        
        # Step 3: Rename columns and format
        monthly_sales.columns = ['month', 'total_orders', 'total_revenue']
        monthly_sales['month'] = monthly_sales['month'].astype(str)
        
        logger.info(f"Calculated trends for {len(monthly_sales)} months")
        return monthly_sales

    def get_regional_revenue(self):
        """KPI 3: Get revenue by region"""
        
        # Step 1: CORRECTLY collapse line items to unique orders by SUMMING
        unique_orders = self.enriched_orders.groupby('order_id').agg({
            'total_amount': 'sum',  # <-- This is the fix (was 'first')
            'region': 'first'
        }).reset_index()
        
        # Step 2: Group unique orders by region
        regional_summary = unique_orders.groupby('region').agg({
            'order_id': 'count',
            'total_amount': 'sum'
        }).reset_index()
        
        regional_summary.columns = ['region', 'order_count', 'total_revenue']
        regional_summary = regional_summary.sort_values('total_revenue', ascending=False)
        
        logger.info(f"Calculated revenue for {len(regional_summary)} regions")
        return regional_summary
    def get_top_customers_last_30_days(self):
        """Return top spending customers in the last 30 days (simple + correct)."""

        # 1) Cutoff date (use latest order timestamp as reference)
        as_of = self.enriched_orders["order_date_time"].max()
        cutoff = as_of - timedelta(days=30)

        # 2) Collapse line-items → one row per order
        orders = (
            self.enriched_orders
            .groupby("order_id", as_index=False)
            .agg(
                order_date_time=("order_date_time", "min"),
                total_amount=("total_amount", "sum"),
                customer_id=("customer_id", "first"),
                customer_name=("customer_name", "first"),
                region=("region", "first"),
            )
        )

        # 3) Keep only recent orders
        recent = orders[orders["order_date_time"] >= cutoff]
        if recent.empty:
            logger.warning("No orders found in the last 30 days")
            return pd.DataFrame()

        # 4) Aggregate per customer
        top = (
            recent.groupby(["customer_id", "customer_name", "region"], as_index=False)
            .agg(
                order_count=("order_id", "nunique"),
                total_spend=("total_amount", "sum"),
            )
            .sort_values("total_spend", ascending=False)
            .head(10)
            .reset_index(drop=True)
        )

        logger.info(f"Found top {len(top)} customers in the last 30 days")
        return top