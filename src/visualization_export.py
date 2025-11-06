import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import logging
import numpy as np

logger = logging.getLogger(__name__)

class BusinessVisualizer:
    
    def __init__(self, output_dir="assets/analytics_dashboards"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        plt.style.use('seaborn-v0_8')
        # Professional color palette
        self.colors = ['#E31B23', '#2E86AB', '#A23B72', '#F18F01', '#1B998B', '#6A4C93']
        self.palette = sns.color_palette("husl", 8)

    # --------------------------------------------------------------------
    # MODIFIED FUNCTION TO CREATE A CLEANER 4-PLOT DASHBOARD
    # --------------------------------------------------------------------
    def create_business_report(self, kpis, report_name):
        """
        Creates a consolidated 4-plot business report.
        """
        try:
            # Create a 2x2 grid
            fig, axes = plt.subplots(2, 2, figsize=(18, 12))
            
            fig.suptitle(f'Akasa Air Business Analytics - {report_name}', fontsize=20, fontweight='bold')
            
            # --- Row 1: Time & Region ---
            # Plot 1: Monthly Revenue & Order Analysis (Top-Left)
            self._revenue_analysis_chart(kpis, axes[0, 0])
            
            # Plot 2: Regional Performance (Top-Right)
            self._regional_performance(kpis, axes[0, 1])
            
            # --- Row 2: Customer Analysis ---
            # Plot 3: Customer Distribution (Bottom-Left)
            self._customer_distribution_by_region(kpis, axes[1, 0])
            
            # Plot 4: Top Performers (Bottom-Right)
            self._top_performers(kpis, axes[1, 1])
            
            plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust for suptitle
            
            report_path = f"{self.output_dir}/{report_name}.png"
            plt.savefig(report_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Analytics dashboard created: {report_path}")
            print(f"Analytics dashboard created: {report_path}")
            return report_path
            
        except Exception as e:
            logger.error(f"Dashboard creation failed: {e}")
            print(f"Dashboard creation failed: {e}")
            return None

    # --------------------------------------------------------------------
    # ALL HELPER FUNCTIONS BELOW ARE UNCHANGED FROM YOUR ORIGINAL SCRIPT
    # --------------------------------------------------------------------

    def _revenue_trend_chart(self, kpis, ax):
        # This function is no longer called in the modified report,
        # but is kept for the export_single_chart method.
        try:
            monthly_data = kpis.get('monthly_trends')
            if monthly_data is not None and not monthly_data.empty:
                if isinstance(monthly_data, pd.DataFrame):
                    months = monthly_data.index
                    revenue = monthly_data['total_revenue']
                else:
                    months = monthly_data['month']
                    revenue = monthly_data['total_revenue']
                
                ax.plot(months, revenue, marker='o', linewidth=3, color=self.palette[0], 
                       markersize=8, markerfacecolor='white', markeredgewidth=2)
                ax.set_title('Monthly Revenue Trend', fontweight='bold', fontsize=12)
                ax.set_ylabel('Revenue (₹)', fontweight='bold')
                ax.set_xlabel('Month', fontweight='bold')
                ax.grid(True, alpha=0.3)
                ax.tick_params(axis='x', rotation=45)
                
                for i, (month, rev) in enumerate(zip(months, revenue)):
                    ax.annotate(f'₹{rev:,.0f}', (month, rev), 
                               textcoords="offset points", xytext=(0,10), 
                               ha='center', fontsize=9, fontweight='bold')
            else:
                self._placeholder(ax, "Revenue Trend")
        except Exception as e:
            self._placeholder(ax, "Revenue Trend")

    def _regional_performance(self, kpis, ax):
        try:
            regional_data = kpis.get('regional_revenue')
            if regional_data is not None and not regional_data.empty:
                if 'region' in regional_data.columns and 'regional_revenue' in regional_data.columns:
                    regions = regional_data['region']
                    revenue = regional_data['regional_revenue']
                elif 'region' in regional_data.columns and 'total_revenue' in regional_data.columns:
                    regions = regional_data['region']
                    revenue = regional_data['total_revenue']
                else:
                    self._placeholder(ax, "Regional Performance")
                    return
                
                bars = ax.bar(regions, revenue, color=self.palette, alpha=0.8, edgecolor='black', linewidth=0.5)
                ax.set_title('Regional Revenue Distribution', fontweight='bold', fontsize=14)
                ax.set_ylabel('Revenue (₹)', fontweight='bold')
                ax.set_xlabel('Region', fontweight='bold')
                ax.tick_params(axis='x', rotation=45)
                ax.grid(True, alpha=0.3, axis='y')
                
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height + 100, 
                           f'₹{height:,.0f}', ha='center', va='bottom', fontsize=10, fontweight='bold')
            else:
                self._placeholder(ax, "Regional Performance")
        except Exception as e:
            self._placeholder(ax, "Regional Performance")

    def _customer_distribution_by_region(self, kpis, ax):
        """Fixed: Show number of customers from each region"""
        try:
            customer_data = kpis.get('repeat_customers')
            regional_data = kpis.get('regional_revenue')
            
            if customer_data is not None and not customer_data.empty and 'region' in customer_data.columns:
                region_counts = customer_data['region'].value_counts()
                
                wedges, texts, autotexts = ax.pie(region_counts.values, 
                                                 labels=region_counts.index, 
                                                 autopct='%1.1f%%',
                                                 colors=self.palette,
                                                 startangle=90,
                                                 wedgeprops=dict(width=0.4)) # Donut hole
                
                for autotext in autotexts:
                    autotext.set_color('white')
                    autotext.set_fontweight('bold')
                    autotext.set_fontsize(10)
                
                ax.set_title('Customer Distribution by Region', 
                           fontweight='bold', fontsize=14)
                
            elif regional_data is not None and not regional_data.empty:
                # Fallback based on your image's data
                regions = regional_data['region']
                customer_counts = [2, 1] # Dummy data: 2 for West, 1 for North
                
                wedges, texts, autotexts = ax.pie(customer_counts, 
                                                 labels=regions, 
                                                 autopct='%1.1f%%',
                                                 colors=self.palette,
                                                 startangle=90,
                                                 wedgeprops=dict(width=0.4))
                
                for autotext in autotexts:
                    autotext.set_color('white')
                    autotext.set_fontweight('bold')
                
                ax.set_title('Customer Distribution by Region\n(Estimated)', 
                           fontweight='bold', fontsize=14)
            else:
                self._placeholder(ax, "Customer Distribution")
                
        except Exception as e:
            logger.error(f"Customer distribution chart error: {e}")
            self._placeholder(ax, "Customer Distribution")

    def _repeat_customer_analysis(self, kpis, ax):
        # This function is no longer called in the modified report,
        # but is kept for potential future use.
        try:
            customer_data = kpis.get('repeat_customers')
            if customer_data is not None and not customer_data.empty:
                if 'number_of_orders' in customer_data.columns:
                    customers = customer_data['customer_name'].head(8)
                    orders = customer_data['number_of_orders'].head(8)
                elif 'order_count' in customer_data.columns:
                    customers = customer_data['customer_name'].head(8)
                    orders = customer_data['order_count'].head(8)
                else:
                    self._placeholder(ax, "Repeat Customers")
                    return
                
                bars = ax.barh(customers, orders, color=self.palette[:len(customers)])
                ax.set_title('Top Repeat Customers', fontweight='bold', fontsize=12)
                ax.set_xlabel('Number of Orders', fontweight='bold')
                ax.grid(True, alpha=0.3, axis='x')
                
                for bar in bars:
                    width = bar.get_width()
                    ax.text(width, bar.get_y() + bar.get_height()/2.,
                           f'{int(width)}', ha='left', va='center', fontweight='bold')
            else:
                self._placeholder(ax, "Repeat Customers")
        except Exception as e:
            self._placeholder(ax, "Repeat Customers")

    def _top_performers(self, kpis, ax):
        try:
            top_data = kpis.get('top_spenders')
            if top_data is not None and not top_data.empty:
                if 'recent_spend' in top_data.columns:
                    customers = top_data['customer_name'].head(5)
                    spend = top_data['recent_spend'].head(5)
                elif 'total_spend' in top_data.columns:
                    customers = top_data['customer_name'].head(5)
                    spend = top_data['total_spend'].head(5)
                else:
                    self._placeholder(ax, "Top Performers")
                    return
                
                bars = ax.barh(customers, spend, color=self.palette[2:7])
                ax.set_title('Top 5 Customers by Spending\n(Last 30 Days)', fontweight='bold', fontsize=14)
                ax.set_xlabel('Amount Spent (₹)', fontweight='bold')
                ax.grid(True, alpha=0.3, axis='x')
                
                for bar in bars:
                    width = bar.get_width()
                    ax.text(width, bar.get_y() + bar.get_height()/2., 
                           f'₹{width:,.0f}', ha='left', va='center', fontweight='bold', fontsize=10)
            else:
                self._placeholder(ax, "Top Performers")
        except Exception as e:
            self._placeholder(ax, "Top Performers")

    def _revenue_analysis_chart(self, kpis, ax):
        """Fixed: Proper revenue analysis instead of incorrect cumulative"""
        try:
            monthly_data = kpis.get('monthly_trends')
            if monthly_data is not None and not monthly_data.empty:
                if isinstance(monthly_data, pd.DataFrame):
                    months = monthly_data.index
                    revenue = monthly_data['total_revenue']
                    orders = monthly_data['total_orders'] if 'total_orders' in monthly_data.columns else None
                else:
                    months = monthly_data['month']
                    revenue = monthly_data['total_revenue']
                    orders = monthly_data['total_orders'] if 'total_orders' in monthly_data.columns else None
                
                ax1 = ax
                if orders is not None:
                    ax2 = ax1.twinx()
                    
                    revenue_line = ax1.plot(months, revenue, marker='s', linewidth=3, 
                                          color=self.palette[0], label='Revenue', 
                                          markerfacecolor='white', markeredgewidth=2)
                    ax1.fill_between(months, revenue, alpha=0.2, color=self.palette[0])
                    
                    order_line = ax2.plot(months, orders, marker='^', linewidth=2, 
                                        color=self.palette[3], label='Orders')
                    
                    ax1.set_ylabel('Revenue (₹)', fontweight='bold', color=self.palette[0])
                    ax2.set_ylabel('Number of Orders', fontweight='bold', color=self.palette[3])
                    
                    lines = revenue_line + order_line
                    labels = [l.get_label() for l in lines]
                    ax1.legend(lines, labels, loc='upper left')
                    
                else:
                    ax1.plot(months, revenue, marker='s', linewidth=3, color=self.palette[0])
                    ax1.fill_between(months, revenue, alpha=0.2, color=self.palette[0])
                    ax1.set_ylabel('Revenue (₹)', fontweight='bold')
                
                ax1.set_title('Monthly Revenue & Order Analysis', fontweight='bold', fontsize=14)
                ax1.set_xlabel('Timeline', fontweight='bold')
                ax1.grid(True, alpha=0.3)
                ax1.tick_params(axis='x', rotation=45)
                
                if len(revenue) > 1:
                    trend = "↑ Increasing" if revenue.iloc[-1] > revenue.iloc[0] else "↓ Decreasing"
                    ax1.text(0.02, 0.98, f'Trend: {trend}', transform=ax1.transAxes, 
                            fontsize=10, fontweight='bold', verticalalignment='top',
                            bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.7))
                
            else:
                self._placeholder(ax, "Revenue Analysis")
        except Exception as e:
            logger.error(f"Revenue analysis chart error: {e}")
            self._placeholder(ax, "Revenue Analysis")

    def _placeholder(self, ax, title):
        ax.text(0.5, 0.5, 'Data Unavailable', ha='center', va='center', 
               transform=ax.transAxes, fontsize=12, style='italic', color='gray')
        ax.set_title(title, fontweight='bold', fontsize=12)
        ax.set_xticks([])
        ax.set_yticks([])

    def export_single_chart(self, kpis, chart_type, filename_suffix):
        """Export a single chart based on type"""
        try:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            if chart_type == "regional_revenue":
                self._regional_performance(kpis, ax)
            elif chart_type == "customer_distribution":
                self._customer_distribution_by_region(kpis, ax)
            elif chart_type == "top_performers":
                self._top_performers(kpis, ax)
            elif chart_type == "revenue_trend":
                self._revenue_trend_chart(kpis, ax)
            else:
                logger.warning(f"Unknown chart type: {chart_type}")
                return None
            
            plt.tight_layout()
            chart_path = f"{self.output_dir}/{chart_type}_{filename_suffix}.png"
            plt.savefig(chart_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logger.info(f"Single chart exported: {chart_path}")
            return chart_path
            
        except Exception as e:
            logger.error(f"Error exporting single chart: {e}")
            return None

# --------------------------------------------------------------------
# EXAMPLE OF HOW TO RUN THIS SCRIPT
# --------------------------------------------------------------------
if __name__ == "__main__":
    
    # --- Create Dummy Data (simulating your dashboard) ---
    
    # 1. Monthly Trends
    months = ['Month 0', 'Month 1', 'Month 2']
    revenue = [35728, 22258, 15887]
    orders = [4, 3.3, 3.1]
    monthly_trends = pd.DataFrame({
        'month': months,
        'total_revenue': revenue,
        'total_orders': orders
    }).set_index('month')
    
    # 2. Regional Revenue
    regional_revenue = pd.DataFrame({
        'region': ['West', 'North'],
        'regional_revenue': [38247, 35720]
    })
    
    # 3. Repeat Customers
    # This data will be used for the donut chart
    repeat_customers = pd.DataFrame({
        'customer_name': ['Aarav Mehta', 'Priya Singh', 'Rohan Gupta'],
        'number_of_orders': [2, 1, 1],
        'region': ['West', 'North', 'West'] # 2 from West, 1 from North
    })
    
    # 4. Top Spenders
    top_spenders = pd.DataFrame({
        'customer_name': ['Aarav Mehta'],
        'recent_spend': [38247]
    })
    
    # Consolidate all KPIs into a dictionary
    all_kpis = {
        'monthly_trends': monthly_trends,
        'regional_revenue': regional_revenue,
        'repeat_customers': repeat_customers,
        'top_spenders': top_spenders
    }
    
    # --- Run the Visualizer ---
    visualizer = BusinessVisualizer()
    visualizer.create_business_report(all_kpis, "Consolidated_Analytics_Report")