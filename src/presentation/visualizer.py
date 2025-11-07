
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import logging
import numpy as np
from pathlib import Path

logger = logging.getLogger(__name__)

class BusinessVisualizer:
    """
    Gold Layer: Business visualization and dashboard generation
    Creates professional business intelligence charts and reports
    """
    
    def __init__(self, output_dir="assets/analytics_dashboards"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Professional styling
        plt.style.use('seaborn-v0_8')
        self.colors = ['#E31B23', '#2E86AB', '#A23B72', '#F18F01', '#1B998B', '#6A4C93']
        self.palette = sns.color_palette("husl", 8)
        
        # Set professional font settings
        plt.rcParams['font.size'] = 10
        plt.rcParams['font.family'] = 'DejaVu Sans'
        
    def create_comprehensive_dashboard(self, gold_data: dict, title_suffix: str = "") -> str:
        """
        Create a comprehensive 6-panel business dashboard
        """
        try:
            logger.info("Creating comprehensive business dashboard")
            
            # Create figure with subplots
            fig = plt.figure(figsize=(20, 15))
            fig.suptitle(f'Akasa Air Business Analytics Dashboard {title_suffix}', 
                        fontsize=20, fontweight='bold', y=0.98)
            
            # Define subplot grid
            gs = plt.GridSpec(3, 3, figure=fig)
            
            # Plot 1: Revenue Trends (Top-left)
            ax1 = fig.add_subplot(gs[0, 0])
            self._plot_revenue_trends(gold_data, ax1)
            
            # Plot 2: Regional Performance (Top-middle)
            ax2 = fig.add_subplot(gs[0, 1])
            self._plot_regional_performance(gold_data, ax2)
            
            # Plot 3: Customer Segmentation (Top-right)
            ax3 = fig.add_subplot(gs[0, 2])
            self._plot_customer_segmentation(gold_data, ax3)
            
            # Plot 4: Repeat Customers (Middle-left)
            ax4 = fig.add_subplot(gs[1, 0])
            self._plot_repeat_customers(gold_data, ax4)
            
            # Plot 5: Top Customers (Middle-middle)
            ax5 = fig.add_subplot(gs[1, 1])
            self._plot_top_customers(gold_data, ax5)
            
            # Plot 6: Order Patterns (Middle-right)
            ax6 = fig.add_subplot(gs[1, 2])
            self._plot_order_patterns(gold_data, ax6)
            
            # Plot 7: KPI Summary (Bottom span)
            ax7 = fig.add_subplot(gs[2, :])
            self._plot_kpi_summary(gold_data, ax7)
            
            plt.tight_layout(rect=[0, 0.03, 1, 0.96])
            
            # Save dashboard
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dashboard_path = self.output_dir / f"comprehensive_dashboard_{timestamp}.png"
            plt.savefig(dashboard_path, dpi=300, bbox_inches='tight', facecolor='white')
            plt.close()
            
            # logger.info(f"Comprehensive dashboard created: {dashboard_path}")
            return str(dashboard_path)
            
        except Exception as e:
            logger.error(f"Dashboard creation failed: {e}")
            return ""
    
    def _plot_revenue_trends(self, gold_data: dict, ax: plt.Axes):
        """Plot monthly revenue and order trends"""
        try:
            monthly_data = gold_data['required_kpis'].get('monthly_trends')
            if monthly_data is None or monthly_data.empty:
                self._placeholder_plot(ax, "Revenue Trends\n(Data Unavailable)")
                return
                
            months = monthly_data['month']
            revenue = monthly_data['total_revenue']
            orders = monthly_data['total_orders']
            
            # Create dual-axis plot
            ax1 = ax
            ax2 = ax1.twinx()
            
            # Plot revenue (bars)
            bars = ax1.bar(months, revenue, alpha=0.7, color=self.colors[0], 
                          label='Revenue', edgecolor='black', linewidth=0.5)
            ax1.set_ylabel('Revenue (₹)', fontweight='bold', color=self.colors[0])
            ax1.tick_params(axis='y', labelcolor=self.colors[0])
            
            # Plot orders (line)
            line = ax2.plot(months, orders, marker='o', linewidth=3, 
                           color=self.colors[1], label='Orders', markersize=8)
            ax2.set_ylabel('Number of Orders', fontweight='bold', color=self.colors[1])
            ax2.tick_params(axis='y', labelcolor=self.colors[1])
            
            # Customize plot
            ax.set_title('Monthly Revenue & Order Trends', fontweight='bold', fontsize=12)
            ax.set_xlabel('Month', fontweight='bold')
            ax.tick_params(axis='x', rotation=45)
            
            # Add value annotations
            for i, (bar, rev) in enumerate(zip(bars, revenue)):
                ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
                        f'₹{rev:,.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
            
            # Combine legends
            lines1, labels1 = ax1.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
            
            ax.grid(True, alpha=0.3, axis='both')
            
        except Exception as e:
            logger.error(f"Revenue trends plot failed: {e}")
            self._placeholder_plot(ax, "Revenue Trends")
    
    def _plot_regional_performance(self, gold_data: dict, ax: plt.Axes):
        """Plot regional revenue distribution"""
        try:
            regional_data = gold_data['required_kpis'].get('regional_revenue')
            if regional_data is None or regional_data.empty:
                self._placeholder_plot(ax, "Regional Performance\n(Data Unavailable)")
                return
                
            regions = regional_data['region']
            revenue = regional_data['regional_revenue']
            
            # Create horizontal bar chart
            bars = ax.barh(regions, revenue, color=self.palette[:len(regions)], 
                          alpha=0.8, edgecolor='black', linewidth=0.5)
            
            ax.set_title('Regional Revenue Distribution', fontweight='bold', fontsize=12)
            ax.set_xlabel('Revenue (₹)', fontweight='bold')
            ax.set_ylabel('Region', fontweight='bold')
            
            # Add value annotations
            for bar in bars:
                width = bar.get_width()
                ax.text(width + 100, bar.get_y() + bar.get_height()/2,
                       f'₹{width:,.0f}', ha='left', va='center', fontweight='bold')
            
            ax.grid(True, alpha=0.3, axis='x')
            
        except Exception as e:
            logger.error(f"Regional performance plot failed: {e}")
            self._placeholder_plot(ax, "Regional Performance")
    
    def _plot_customer_segmentation(self, gold_data: dict, ax: plt.Axes):
        """Plot customer segmentation by orders"""
        try:
            # Customer distribution across different regions
            region_counts = customers['region'].value_counts()
            plt.figure(figsize=(10, 6))
            plt.pie(region_counts.values, labels=region_counts.index, autopct='%1.1f%%', startangle=90)
            plt.title('Customer Distribution by Region')
            plt.axis('equal')

        except Exception as e:
            logger.error(f"Customer segmentation plot failed: {e}")
            self._placeholder_plot(ax, "Customer Segmentation")
    
    def _plot_repeat_customers(self, gold_data: dict, ax: plt.Axes):
        """Plot top repeat customers"""
        try:
            # Orders and revenue by region
            regional_performance = orders.groupby('region').agg({
                'order_id': 'count',
                'total_amount': 'sum'
            }).reset_index()

            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

            # Orders by region
            ax1.bar(regional_performance['region'], regional_performance['order_id'])
            ax1.set_title('Number of Orders by Region')
            ax1.set_ylabel('Order Count')

            # Revenue by region
            ax2.bar(regional_performance['region'], regional_performance['total_amount'])
            ax2.set_title('Revenue by Region')
            ax2.set_ylabel('Total Revenue')
            
        except Exception as e:
            logger.error(f"Repeat customers plot failed: {e}")
            self._placeholder_plot(ax, "Repeat Customers")
    
    def _plot_top_customers(self, gold_data: dict, ax: plt.Axes):
        """Plot top customers by spending"""
        try:
           # Segment customers by spending behavior
            customer_spend = orders.groupby('mobile_number').agg({
                'total_amount': 'sum',
                'order_id': 'count'
            }).reset_index()

            # Merge with customer data
            customer_spend = customer_spend.merge(
                customers[['mobile_number', 'customer_name', 'region']], 
                on='mobile_number', 
                how='left'
            )

            # Create segments
            conditions = [
                customer_spend['total_amount'] >= customer_spend['total_amount'].quantile(0.8),
                customer_spend['total_amount'] >= customer_spend['total_amount'].quantile(0.5),
                customer_spend['total_amount'] >= customer_spend['total_amount'].quantile(0.2)
            ]
            choices = ['VIP', 'Regular', 'Occasional']
            customer_spend['segment'] = np.select(conditions, choices, default='New')

            # Plot segmentation
            segment_counts = customer_spend['segment'].value_counts()
            plt.figure(figsize=(8, 6))
            plt.pie(segment_counts.values, labels=segment_counts.index, autopct='%1.1f%%', startangle=90)
            plt.title('Customer Value Segmentation')
            plt.axis('equal')
        except Exception as e:
            logger.error(f"Top customers plot failed: {e}")
            self._placeholder_plot(ax, "Top Customers")
    
    def _plot_order_patterns(self, gold_data: dict, ax: plt.Axes):
        """Plot order patterns and seasonality"""
        try:
            monthly_data = gold_data['required_kpis'].get('monthly_trends')
            if monthly_data is None or monthly_data.empty:
                self._placeholder_plot(ax, "Order Patterns\n(Data Unavailable)")
                return
                
            months = monthly_data['month']
            orders = monthly_data['total_orders']
            
            # Create line plot with area fill
            ax.fill_between(months, orders, alpha=0.3, color=self.colors[2])
            ax.plot(months, orders, marker='s', linewidth=2, color=self.colors[2], markersize=6)
            
            ax.set_title('Order Volume Trends', fontweight='bold', fontsize=12)
            ax.set_xlabel('Month', fontweight='bold')
            ax.set_ylabel('Number of Orders', fontweight='bold')
            ax.tick_params(axis='x', rotation=45)
            
            # Add value annotations
            for i, (month, order_count) in enumerate(zip(months, orders)):
                ax.annotate(f'{order_count}', (month, order_count),
                           textcoords="offset points", xytext=(0,10), 
                           ha='center', fontsize=9, fontweight='bold')
            
            ax.grid(True, alpha=0.3)
            
        except Exception as e:
            logger.error(f"Order patterns plot failed: {e}")
            self._placeholder_plot(ax, "Order Patterns")
    
    def _plot_kpi_summary(self, gold_data: dict, ax: plt.Axes):
        """Plot KPI summary metrics"""
        try:
            # Calculate summary metrics
            kpis = gold_data['required_kpis']
            metrics = []
            values = []
            
            # Total Revenue
            if 'monthly_trends' in kpis and not kpis['monthly_trends'].empty:
                total_revenue = kpis['monthly_trends']['total_revenue'].sum()
                metrics.append('Total Revenue')
                values.append(total_revenue)
            
            # Total Orders
            if 'monthly_trends' in kpis and not kpis['monthly_trends'].empty:
                total_orders = kpis['monthly_trends']['total_orders'].sum()
                metrics.append('Total Orders')
                values.append(total_orders)
            
            # Repeat Customers
            if 'repeat_customers' in kpis:
                repeat_count = len(kpis['repeat_customers'])
                metrics.append('Repeat Customers')
                values.append(repeat_count)
            
            # Regions Covered
            if 'regional_revenue' in kpis:
                region_count = len(kpis['regional_revenue'])
                metrics.append('Regions Covered')
                values.append(region_count)
            
            if not metrics:
                self._placeholder_plot(ax, "KPI Summary\n(Data Unavailable)")
                return
            
            # Create metric boxes
            ax.axis('off')
            ax.set_title('Business Performance Summary', fontweight='bold', fontsize=14, pad=20)
            
            # Create metric boxes
            n_metrics = len(metrics)
            box_width = 0.8 / n_metrics
            
            for i, (metric, value) in enumerate(zip(metrics, values)):
                x_center = (i + 0.5) * box_width
                
                # Draw metric box
                rect = plt.Rectangle((x_center - box_width/2, 0.3), box_width, 0.4,
                                   fill=True, color=self.palette[i], alpha=0.8,
                                   edgecolor='black', linewidth=1)
                ax.add_patch(rect)
                
                # Add metric text
                if 'Revenue' in metric:
                    value_text = f'₹{value:,.0f}'
                else:
                    value_text = f'{value}'
                
                ax.text(x_center, 0.7, value_text, ha='center', va='center',
                       fontweight='bold', fontsize=16, color='white')
                ax.text(x_center, 0.2, metric, ha='center', va='center',
                       fontweight='bold', fontsize=10, color='black')
            
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            
        except Exception as e:
            logger.error(f"KPI summary plot failed: {e}")
            self._placeholder_plot(ax, "KPI Summary")
    
    def _placeholder_plot(self, ax: plt.Axes, title: str):
        """Create placeholder plot when data is unavailable"""
        ax.text(0.5, 0.5, 'Data Unavailable', ha='center', va='center',
               transform=ax.transAxes, fontsize=12, style='italic', color='gray')
        ax.set_title(title, fontweight='bold', fontsize=12)
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)
    
    def create_individual_charts(self, gold_data: dict) -> dict:
        """
        Create individual charts for each KPI
        Returns paths to all generated charts
        """
        chart_paths = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Revenue Trends Chart
            fig, ax = plt.subplots(figsize=(10, 6))
            self._plot_revenue_trends(gold_data, ax)
            plt.tight_layout()
            revenue_path = self.output_dir / f"revenue_trends_{timestamp}.png"
            plt.savefig(revenue_path, dpi=300, bbox_inches='tight')
            plt.close()
            chart_paths['revenue_trends'] = str(revenue_path)
            
            # Regional Performance Chart
            fig, ax = plt.subplots(figsize=(10, 6))
            self._plot_regional_performance(gold_data, ax)
            plt.tight_layout()
            regional_path = self.output_dir / f"regional_performance_{timestamp}.png"
            plt.savefig(regional_path, dpi=300, bbox_inches='tight')
            plt.close()
            chart_paths['regional_performance'] = str(regional_path)
            
            # Customer Segmentation Chart
            fig, ax = plt.subplots(figsize=(8, 8))
            self._plot_customer_segmentation(gold_data, ax)
            plt.tight_layout()
            segmentation_path = self.output_dir / f"customer_segmentation_{timestamp}.png"
            plt.savefig(segmentation_path, dpi=300, bbox_inches='tight')
            plt.close()
            chart_paths['customer_segmentation'] = str(segmentation_path)
            
            logger.info(f"Created {len(chart_paths)} individual charts")
            return chart_paths
            
        except Exception as e:
            logger.error(f"Individual chart creation failed: {e}")
            return {}
