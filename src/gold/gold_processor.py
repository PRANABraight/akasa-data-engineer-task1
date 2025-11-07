import pandas as pd
import logging
from datetime import datetime
import os
from .kpi_calculator import KPICalculator
from .business_metrics import BusinessMetrics
from src.presentation.visualizer import BusinessVisualizer
from src.presentation.report_generator import ReportGenerator

logger = logging.getLogger(__name__)

class GoldProcessor:
    """
    Gold Layer: Main processor coordinating KPI and business metrics calculation
    Now includes comprehensive visualization and reporting
    """
    
    def __init__(self):
        self.kpi_calculator = None
        self.business_metrics = None
        self.visualizer = BusinessVisualizer()
        self.report_generator = ReportGenerator()
        self.gold_data = {}
        self.processing_stats = {}
        
    def process_to_gold(self, silver_data: dict, calculate_additional_metrics: bool = False) -> dict:
        """
        Process silver data through gold layer to generate business KPIs
        Now includes visualization and reporting
        """
        try:
            logger.info("Processing gold layer")
            start_time = datetime.utcnow()
            
            # Step 1: Calculate required KPIs
            self.kpi_calculator = KPICalculator(silver_data)
            kpi_results = self.kpi_calculator.calculate_all_kpis()
            
            # Step 2: Calculate additional business metrics (optional)
            additional_metrics = {}
            if calculate_additional_metrics:
                logger.info("Calculating additional business metrics")
                self.business_metrics = BusinessMetrics(silver_data)
                additional_metrics = self.business_metrics.get_all_additional_metrics()
            
            # Step 3: Generate visualizations and reports
            visualization_results = self._generate_visualizations(kpi_results)
            report_results = self._generate_reports(kpi_results)
            
            # Compile gold data
            self.gold_data = {
                'required_kpis': kpi_results,
                'additional_metrics': additional_metrics,
                'visualizations': visualization_results,
                'reports': report_results,
                'metadata': {
                    'processing_timestamp': datetime.utcnow(),
                    'processing_duration_seconds': (datetime.utcnow() - start_time).total_seconds(),
                    'kpis_calculated': len(kpi_results) - 1,  # Exclude metadata
                    'additional_metrics_calculated': len(additional_metrics) - 1 if additional_metrics else 0,
                    'visualizations_generated': len(visualization_results),
                    'reports_generated': len(report_results)
                }
            }
            
            logger.info("Gold layer processing completed")
            return self.gold_data
            
        except Exception as e:
            logger.error(f"Gold layer processing failed: {str(e)}")
            raise
            
    def _generate_visualizations(self, kpi_results: dict) -> dict:
        """Generate comprehensive visualizations"""
        try:
            logger.info("Generating business visualizations")
            
            visualization_data = {
                'required_kpis': kpi_results
            }
            
            # Generate comprehensive dashboard
            dashboard_path = self.visualizer.create_comprehensive_dashboard(
                visualization_data, 
                "- Gold Layer Analysis"
            )
            
            # Generate individual charts
            individual_charts = self.visualizer.create_individual_charts(visualization_data)
            
            results = {
                'comprehensive_dashboard': dashboard_path,
                'individual_charts': individual_charts
            }
            
            logger.info(f"Generated {len(individual_charts) + 1} visualizations")
            return results
            
        except Exception as e:
            logger.error(f"Visualization generation failed: {str(e)}")
            return {}
            
    def _generate_reports(self, kpi_results: dict) -> dict:
        """Generate comprehensive business reports"""
        try:
            logger.info("Generating business reports")
            
            report_data = {
                'required_kpis': kpi_results
            }
            
            # Generate text report
            text_report_path = self.report_generator.generate_business_report(report_data)
            
            # Generate JSON report
            json_report_path = self.report_generator.generate_json_report(report_data)
            
            results = {
                'text_report': text_report_path,
                'json_report': json_report_path
            }
            
            logger.info(f"Generated {len(results)} reports")
            return results
            
        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}")
            return {}
        
    def get_kpi_summary(self) -> dict:
        """Get summary of KPI calculations"""
        if self.kpi_calculator:
            return self.kpi_calculator.get_kpi_summary()
        return {}
        
    def save_gold_data(self, output_dir: str = "data/gold") -> dict:
        """
        Save gold data to persistent storage
        """
        try:
            if not self.gold_data:
                raise ValueError("No gold data available. Run process_to_gold first.")
                
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            
            saved_paths = {}
            
            # Save required KPIs
            kpis_data = self.gold_data['required_kpis']
            for kpi_name, kpi_df in kpis_data.items():
                if kpi_name != 'metadata' and isinstance(kpi_df, pd.DataFrame):
                    kpi_path = f"{output_dir}/{kpi_name}_{timestamp}.parquet"
                    kpi_df.to_parquet(kpi_path, index=False)
                    saved_paths[f"{kpi_name}_path"] = kpi_path
            
            # Save additional metrics
            additional_data = self.gold_data['additional_metrics']
            for metric_name, metric_data in additional_data.items():
                if metric_name != 'metadata':
                    if isinstance(metric_data, pd.DataFrame):  # Single DataFrame
                        metric_path = f"{output_dir}/{metric_name}_{timestamp}.parquet"
                        metric_data.to_parquet(metric_path, index=False)
                        saved_paths[f"{metric_name}_path"] = metric_path
                    elif isinstance(metric_data, dict):  # Nested structure
                        for sub_metric, sub_data in metric_data.items():
                            if isinstance(sub_data, pd.DataFrame):
                                sub_path = f"{output_dir}/{metric_name}_{sub_metric}_{timestamp}.parquet"
                                sub_data.to_parquet(sub_path, index=False)
                                saved_paths[f"{metric_name}_{sub_metric}_path"] = sub_path
            
            # Save metadata
            metadata_path = f"{output_dir}/gold_metadata_{timestamp}.json"
            import json
            with open(metadata_path, 'w') as f:
                json.dump(self.gold_data['metadata'], f, indent=2, default=str)
            saved_paths['metadata_path'] = metadata_path
            
            logger.info(f"Gold data saved to {output_dir}")
            return saved_paths
            
        except Exception as e:
            logger.error(f"Failed to save gold data: {str(e)}")
            raise
            
    def display_kpi_results(self):
        """
        Display KPI results in a formatted way with visualization info
        """
        if not self.gold_data:
            print("No gold data available. Run process_to_gold first.")
            return
            
        kpis = self.gold_data['required_kpis']
        visualizations = self.gold_data.get('visualizations', {})
        reports = self.gold_data.get('reports', {})
        
        print("\n" + "="*70)
        print("BUSINESS KPI RESULTS")
        print("="*70)
        
        for kpi_name, kpi_data in kpis.items():
            if kpi_name != 'metadata' and isinstance(kpi_data, pd.DataFrame):
                print(f"\n{kpi_name.upper().replace('_', ' ')}:")
                print("-" * 50)
                if not kpi_data.empty:
                    print(kpi_data.head(10).to_string(index=False))
                else:
                    print("No data available")
        
        # Display visualization information
        if visualizations:
            print(f"\nVISUALIZATIONS GENERATED:")
            print("-" * 50)
            dashboard = visualizations.get('comprehensive_dashboard')
            if dashboard:
                print(f"• Comprehensive Dashboard: {dashboard}")
            
            individual_charts = visualizations.get('individual_charts', {})
            for chart_name, chart_path in individual_charts.items():
                print(f"• {chart_name.replace('_', ' ').title()}: {chart_path}")
        
        # Display report information
        if reports:
            print(f"\nREPORTS GENERATED:")
            print("-" * 50)
            for report_type, report_path in reports.items():
                if report_path:  # Only show if path exists
                    print(f"• {report_type.replace('_', ' ').title()}: {report_path}")
                    
        print("\n" + "="*70)
        print("KPI CALCULATION & VISUALIZATION COMPLETE")
        print("="*70)