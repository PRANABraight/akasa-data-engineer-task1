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
    Gold Layer: Simple and clear business visualization
    Creates clean, professional charts with visible data
    """
    
    def __init__(self, output_dir="assets/analytics_dashboards"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Clean styling for better visibility
        plt.style.use('default')
        self.colors = ['#2E86AB', '#A23B72', '#F18F01', '#1B998B', '#6A4C93', '#E31B23']
        
        # Set clear font settings
        plt.rcParams['font.size'] = 12
        plt.rcParams['font.family'] = 'DejaVu Sans'
        
    def create_comprehensive_dashboard(self, gold_data: dict, title_suffix: str = "") -> str:
        """
        Create a clean 4-panel business dashboard with visible graphs
        """
        try:
            # Create figure with subplots
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle(f'Akasa Air Business Dashboard {title_suffix}', 
                           fontsize=16, fontweight='bold', y=0.95)
            
            # Flatten axes for easy iteration
            axes = axes.flatten()
            
            # Plot 1: Revenue Trends
            self._plot_revenue_trends(gold_data, axes[0])
            
            # Plot 2: Regional Revenue
            self._plot_regional_revenue(gold_data, axes[1])
            
            # Plot 3: Customer Distribution by Region
            self._plot_customer_regions(gold_data, axes[2])
            
            # Plot 4: Top Customers
            self._plot_top_customers(gold_data, axes[3])
            
            plt.tight_layout(rect=[0, 0, 1, 0.95])
            
            # Save dashboard
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dashboard_path = self.output_dir / f"dashboard_{timestamp}.png"
            plt.savefig(dashboard_path, dpi=150, bbox_inches='tight', facecolor='white')
            plt.close()
            
            return str(dashboard_path)
            
        except Exception as e:
            print(f"Dashboard creation failed: {e}")
            return ""
    
    def _plot_revenue_trends(self, gold_data: dict, ax: plt.Axes):
        """Plot simple monthly revenue trends"""
        try:
            monthly_data = gold_data['required_kpis'].get('monthly_trends')
            if monthly_data is None or monthly_data.empty:
                self._simple_placeholder(ax, "Revenue Trends\nNo Data")
                return
                
            months = monthly_data['month'].astype(str)
            revenue = monthly_data['total_revenue']
            
            # Simple bar chart
            bars = ax.bar(months, revenue, color=self.colors[0], alpha=0.8, edgecolor='black')
            ax.set_title('Monthly Revenue', fontweight='bold', fontsize=14)
            ax.set_xlabel('Month')
            ax.set_ylabel('Revenue (₹)')
            ax.tick_params(axis='x', rotation=45)
            
            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 100,
                       f'₹{height:,.0f}', ha='center', va='bottom', fontsize=10)
            
            ax.grid(True, alpha=0.3)
            
        except Exception as e:
            print(f"Revenue plot error: {e}")
            self._simple_placeholder(ax, "Revenue Trends")
    
    def _plot_regional_revenue(self, gold_data: dict, ax: plt.Axes):
        """Plot regional revenue distribution"""
        try:
            regional_data = gold_data['required_kpis'].get('regional_revenue')
            if regional_data is None or regional_data.empty:
                self._simple_placeholder(ax, "Regional Revenue\nNo Data")
                return
                
            regions = regional_data['region']
            revenue = regional_data['regional_revenue']
            
            # Horizontal bar chart for better readability
            bars = ax.barh(regions, revenue, color=self.colors[1], alpha=0.8, edgecolor='black')
            ax.set_title('Revenue by Region', fontweight='bold', fontsize=14)
            ax.set_xlabel('Revenue (₹)')
            
            # Add value labels
            for bar in bars:
                width = bar.get_width()
                ax.text(width + 100, bar.get_y() + bar.get_height()/2,
                       f'₹{width:,.0f}', ha='left', va='center', fontsize=10)
            
            ax.grid(True, alpha=0.3, axis='x')
            
        except Exception as e:
            print(f"Regional revenue plot error: {e}")
            self._simple_placeholder(ax, "Regional Revenue")
    
    # --------------------------------------------------------------------
    # FIXED FUNCTION
    # --------------------------------------------------------------------
    def _plot_customer_regions(self, gold_data: dict, ax: plt.Axes):
        """Plot customer distribution across regions"""
        try:
            # FIX: Use the 'top_customers_30d' KPI as the data source, 
            # as it contains customer and region information.
            # This keeps the 'Gold Layer' visualization pure.
            kpi_data = gold_data['required_kpis'].get('top_customers_30d')
            
            # Check if data exists and has the 'region' column
            if kpi_data is None or kpi_data.empty or 'region' not in kpi_data.columns:
                self._simple_placeholder(ax, "Customer Regions\nNo Data")
                return
                
            # Count customers per region *from the KPI data*
            region_counts = kpi_data['region'].value_counts()
            
            # Simple pie chart
            wedges, texts, autotexts = ax.pie(
                region_counts.values, 
                labels=region_counts.index, 
                autopct='%1.1f%%',
                startangle=90,
                colors=self.colors[:len(region_counts)],
                textprops={'fontsize': 10}
            )
            
            # Make percentages bold
            for autotext in autotexts:
                autotext.set_fontweight('bold')
                
            ax.set_title('Customer Distribution by Region', fontweight='bold', fontsize=14)
            
        except Exception as e:
            print(f"Customer regions plot error: {e}")
            self._simple_placeholder(ax, "Customer Regions")
    # --------------------------------------------------------------------
    # END OF FIX
    # --------------------------------------------------------------------

    def _plot_top_customers(self, gold_data: dict, ax: plt.Axes):
        """Plot top customers by spending"""
        try:
            top_customers_data = gold_data['required_kpis'].get('top_customers_30d')
            if top_customers_data is None or top_customers_data.empty:
                self._simple_placeholder(ax, "Top Customers\nNo Data")
                return
                
            # Limit to top 5 for clarity
            top_data = top_customers_data.head(5)
            customers = top_data['customer_name']
            spending = top_data['recent_spend']
            
            # Simple bar chart
            bars = ax.bar(customers, spending, color=self.colors[3], alpha=0.8, edgecolor='black')
            ax.set_title('Top Customers (Last 30 Days)', fontweight='bold', fontsize=14)
            ax.set_xlabel('Customer')
            ax.set_ylabel('Spending (₹)')
            ax.tick_params(axis='x', rotation=45)
            
            # Add value labels
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 100,
                       f'₹{height:,.0f}', ha='center', va='bottom', fontsize=10)
            
            ax.grid(True, alpha=0.3)
            
        except Exception as e:
            print(f"Top customers plot error: {e}")
            self._simple_placeholder(ax, "Top Customers")
    
    def _simple_placeholder(self, ax: plt.Axes, title: str):
        """Simple placeholder for missing data"""
        ax.text(0.5, 0.5, 'No Data Available', 
               ha='center', va='center', fontsize=12, 
               transform=ax.transAxes, style='italic')
        ax.set_title(title, fontweight='bold')
        ax.set_xticks([])
        ax.set_yticks([])
    
    def create_individual_charts(self, gold_data: dict) -> dict:
        """
        Create individual charts - simplified version
        """
        chart_paths = {}
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Revenue Trends
            fig, ax = plt.subplots(figsize=(10, 6))
            self._plot_revenue_trends(gold_data, ax)
            plt.tight_layout()
            revenue_path = self.output_dir / f"revenue_{timestamp}.png"
            plt.savefig(revenue_path, dpi=150, bbox_inches='tight')
            plt.close()
            chart_paths['revenue'] = str(revenue_path)
            
            # Regional Revenue
            fig, ax = plt.subplots(figsize=(10, 6))
            self._plot_regional_revenue(gold_data, ax)
            plt.tight_layout()
            regional_path = self.output_dir / f"regional_{timestamp}.png"
            plt.savefig(regional_path, dpi=150, bbox_inches='tight')
            plt.close()
            chart_paths['regional'] = str(regional_path)
            
            return chart_paths
            
        except Exception as e:
            print(f"Chart creation error: {e}")
            return {}

    def create_simple_dashboard(self, kpi_data: dict) -> str:
        """
        Ultra-simple dashboard with just 2 key charts
        """
        try:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
            
            # Left: Revenue trends
            self._plot_revenue_trends(kpi_data, ax1)
            
            # Right: Regional performance
            self._plot_regional_revenue(kpi_data, ax2)
            
            plt.tight_layout()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dashboard_path = self.output_dir / f"simple_dashboard_{timestamp}.png"
            plt.savefig(dashboard_path, dpi=150, bbox_inches='tight')
            plt.close()
            
            return str(dashboard_path)
            
        except Exception as e:
            print(f"Simple dashboard error: {e}")
            return ""

# --------------------------------------------------------------------
# EXAMPLE OF HOW TO RUN THIS SCRIPT
# --------------------------------------------------------------------
if __name__ == "__main__":
    
    # --- Create Dummy Data (simulating your dashboard) ---
    
    # 1. Monthly Trends
    monthly_trends = pd.DataFrame({
        'month': ['2025-09', '2025-10', '2025-11'],
        'total_revenue': [35720, 22350, 15897]
    })
    
    # 2. Regional Revenue
    regional_revenue = pd.DataFrame({
        'region': ['West', 'North'],
        'regional_revenue': [38247, 35720]
    }).sort_values('regional_revenue', ascending=False)
    
    # 3. Top Customers (This data will also fix the Pie Chart)
    top_customers = pd.DataFrame({
        'customer_name': ['Aarav Mehta', 'Priya Singh'],
        'recent_spend': [38247, 35720],
        'region': ['West', 'North'] # This 'region' column fixes the pie chart
    })

    # 4. Silver data (dummy, as the fixed code doesn't use it)
    customers_df = pd.DataFrame({
        'customer_name': ['Aarav Mehta', 'Priya Singh'],
        'region': ['West', 'North']
    })
    
    # Consolidate all data into the nested structure the class expects
    all_data = {
        'required_kpis': {
            'monthly_trends': monthly_trends,
            'regional_revenue': regional_revenue,
            'top_customers_30d': top_customers
        },
        'silver_data': {
            'customers': customers_df
        }
    }
    
    # --- Run the Visualizer ---
    visualizer = BusinessVisualizer()
    report_path = visualizer.create_comprehensive_dashboard(
        all_data, 
        title_suffix="- Gold Layer Analysis (FIXED)"
    )
    print(f"Dashboard created: {report_path}")