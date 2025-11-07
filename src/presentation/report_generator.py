import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
import json

logger = logging.getLogger(__name__)

class ReportGenerator:
    """
    Gold Layer: Simple and correct business report generation
    """
    
    def __init__(self, output_dir="assets/reports"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def generate_business_report(self, gold_data: dict) -> str:
        """
        Generate simple business report with key insights
        """
        try:
            logger.info("Generating business report")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = self.output_dir / f"business_report_{timestamp}.txt"
            
            with open(report_path, 'w', encoding='utf-8') as f:
                self._write_header(f)
                self._write_summary(f, gold_data)
                self._write_kpis(f, gold_data)
                self._write_footer(f)
            
            logger.info(f"Business report generated: {report_path}")
            return str(report_path)
            
        except Exception as e:
            logger.error(f"Business report generation failed: {e}")
            return ""
    
    def _write_header(self, file):
        """Write report header"""
        file.write("=" * 60 + "\n")
        file.write("AKASA AIR - BUSINESS REPORT\n")
        file.write("=" * 60 + "\n")
        file.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    
    def _write_summary(self, file, gold_data: dict):
        """Write executive summary"""
        file.write("EXECUTIVE SUMMARY\n")
        file.write("-" * 30 + "\n")
        
        kpis = gold_data['required_kpis']
        metrics = self._calculate_metrics(kpis)
        
        file.write(f"Total Revenue: ₹{metrics['total_revenue']:,.0f}\n")
        file.write(f"Total Orders: {metrics['total_orders']}\n")
        file.write(f"Repeat Customers: {metrics['repeat_customers']}\n")
        file.write(f"Regions: {metrics['regions_covered']}\n\n")
    
    def _write_kpis(self, file, gold_data: dict):
        """Write KPI details"""
        kpis = gold_data['required_kpis']
        
        # Repeat Customers
        file.write("REPEAT CUSTOMERS\n")
        file.write("-" * 30 + "\n")
        if 'repeat_customers' in kpis and not kpis['repeat_customers'].empty:
            for _, row in kpis['repeat_customers'].iterrows():
                file.write(f"- {row['customer_name']}: {row['number_of_orders']} orders\n")
        else:
            file.write("No repeat customers\n")
        file.write("\n")
        
        # Monthly Trends
        file.write("MONTHLY TRENDS\n")
        file.write("-" * 30 + "\n")
        if 'monthly_trends' in kpis and not kpis['monthly_trends'].empty:
            for _, row in kpis['monthly_trends'].iterrows():
                file.write(f"- {row['month']}: {row['total_orders']} orders, ₹{row['total_revenue']:,.0f}\n")
        file.write("\n")
        
        # Regional Revenue
        file.write("REGIONAL PERFORMANCE\n")
        file.write("-" * 30 + "\n")
        if 'regional_revenue' in kpis and not kpis['regional_revenue'].empty:
            for _, row in kpis['regional_revenue'].iterrows():
                file.write(f"- {row['region']}: ₹{row['regional_revenue']:,.0f}\n")
        file.write("\n")
        
        # Top Customers
        file.write("TOP CUSTOMERS (30 DAYS)\n")
        file.write("-" * 30 + "\n")
        if 'top_customers_30d' in kpis and not kpis['top_customers_30d'].empty:
            for _, row in kpis['top_customers_30d'].iterrows():
                file.write(f"- {row['customer_name']}: ₹{row['recent_spend']:,.0f}\n")
        else:
            file.write("No recent customer activity\n")
        file.write("\n")
    
    def _write_footer(self, file):
        """Write report footer"""
        file.write("=" * 60 + "\n")
        file.write("End of Report\n")
        file.write("=" * 60 + "\n")
    
    def _calculate_metrics(self, kpis: dict) -> dict:
        """Calculate summary metrics"""
        metrics = {
            'total_revenue': 0,
            'total_orders': 0,
            'repeat_customers': 0,
            'regions_covered': 0
        }
        
        if 'monthly_trends' in kpis and not kpis['monthly_trends'].empty:
            metrics['total_revenue'] = kpis['monthly_trends']['total_revenue'].sum()
            metrics['total_orders'] = kpis['monthly_trends']['total_orders'].sum()
        
        if 'repeat_customers' in kpis:
            metrics['repeat_customers'] = len(kpis['repeat_customers'])
        
        if 'regional_revenue' in kpis:
            metrics['regions_covered'] = len(kpis['regional_revenue'])
        
        return metrics
    
    def generate_json_report(self, gold_data: dict) -> str:
        """
        Generate simple JSON report
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = self.output_dir / f"business_report_{timestamp}.json"
            
            kpis = gold_data['required_kpis']
            report_data = {
                "report_date": datetime.now().isoformat(),
                "summary": self._calculate_metrics(kpis),
                "kpis": {}
            }
            
            # Add KPI data
            for kpi_name, kpi_data in kpis.items():
                if kpi_name != 'metadata' and hasattr(kpi_data, 'to_dict'):
                    report_data["kpis"][kpi_name] = kpi_data.to_dict('records')
            
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"JSON report generated: {report_path}")
            return str(report_path)
            
        except Exception as e:
            logger.error(f"JSON report generation failed: {e}")
            return ""
