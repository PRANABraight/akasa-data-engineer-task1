import logging
import logging.config
from datetime import datetime
import sys
import os
import traceback

# Add src to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.config.config_manager import ConfigManager
from src.bronze.data_loader import DataLoader
from src.silver.silver_processor import SilverProcessor
from src.gold.gold_processor import GoldProcessor
from src.utils.database import DatabaseManager

class MedallionArchitecturePipeline:
    """
    Main Medallion Architecture Pipeline
    Implements Bronze → Silver → Gold layers with both in-memory and database approaches
    """
    
    def __init__(self):
        self.config = ConfigManager()
        self.setup_logging()
        
        self.bronze_loader = None
        self.silver_processor = None
        self.gold_processor = None
        self.db_manager = None
        
        self.pipeline_results = {}
        
    def setup_logging(self):
        """Setup professional logging"""
        try:
            logging_config = self.get_professional_logging_config()  # Use new config
            logging.config.dictConfig(logging_config)
            self.logger = logging.getLogger(__name__)
            self.logger.info("Pipeline initialized")
        except Exception as e:
            print(f"Logging setup failed: {e}")
            logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
            self.logger = logging.getLogger(__name__)
        
    def run_bronze_layer(self) -> dict:
        """
        Bronze Layer Execution: Raw Data Ingestion
        """
        self.logger.info("="*70)
        self.logger.info("BRONZE LAYER: Raw Data Ingestion")
        self.logger.info("="*70)
        
        try:
            # Validate file paths
            self.config.validate_paths()
            paths = self.config.file_paths
            
            # Initialize bronze loader
            self.bronze_loader = DataLoader(
                str(paths['customers_csv']),
                str(paths['orders_xml'])
            )
            
            # Load all bronze data
            bronze_data = self.bronze_loader.load_all_bronze_data()
            
            # Save to persistent storage
            saved_paths = self.bronze_loader.save_all_bronze_data(str(paths['bronze_data']))
            
            self.pipeline_results['bronze'] = {
                'data': bronze_data,
                'saved_paths': saved_paths,
                'timestamp': datetime.utcnow()
            }
            
            self.logger.info(f"Bronze layer completed: {len(bronze_data['customers'])} customers, {len(bronze_data['orders'])} orders")
            return bronze_data
            
        except Exception as e:
            self.logger.error(f"Bronze layer failed: {e}")
            self.logger.error(traceback.format_exc())
            raise
            
    def run_silver_layer(self, bronze_data: dict) -> dict:
        """
        Silver Layer Execution: Data Cleaning & Validation
        
        """
        self.logger.info("="*70)
        self.logger.info("SILVER LAYER: Data Cleaning & Validation")
        self.logger.info("="*70)
        
        try:
            # Initialize silver processor
            self.silver_processor = SilverProcessor()
            
            # Process to silver layer
            silver_data = self.silver_processor.process_to_silver(bronze_data)
            
            # Save to persistent storage
            paths = self.config.file_paths
            saved_paths = self.silver_processor.save_silver_data(str(paths['silver_data']))
            
            # Optional: Save to database
            self.db_manager = DatabaseManager()
            if self.db_manager.engine:
                try:
                    self.db_manager.save_to_database(silver_data['customers'], 'customers', 'silver')
                    self.db_manager.save_to_database(silver_data['orders'], 'orders', 'silver')
                    self.db_manager.create_indexes()
                    self.logger.info("Silver data saved to database")
                except Exception as db_error:
                    self.logger.warning(f"Database save failed but continuing: {db_error}")
            
            self.pipeline_results['silver'] = {
                'data': silver_data,
                'saved_paths': saved_paths,
                'timestamp': datetime.utcnow()
            }
            
            self.logger.info("Silver layer completed: Data cleaned, validated, and enriched")
            return silver_data
            
        except Exception as e:
            self.logger.error(f"Silver layer failed: {e}")
            self.logger.error(f"Detailed error: {traceback.format_exc()}")
            
            # Try to provide more context about the error
            if "region_group" in str(e):
                self.logger.error("The error seems to be related to region_group column. Checking data structure...")
                if bronze_data and 'customers' in bronze_data:
                    self.logger.info(f"Customers columns: {list(bronze_data['customers'].columns)}")
                if bronze_data and 'orders' in bronze_data:
                    self.logger.info(f"Orders columns: {list(bronze_data['orders'].columns)}")
            
            raise
            
    def run_gold_layer(self, silver_data: dict, calculate_additional_metrics: bool = False) -> dict:
        """
        Gold Layer Execution: Business KPI Calculation
        """
        self.logger.info("="*70)
        self.logger.info("GOLD LAYER: Business KPI Calculation")
        self.logger.info("="*70)
        
        try:
            # Initialize gold processor
            self.gold_processor = GoldProcessor()
            
            # Process to gold layer
            gold_data = self.gold_processor.process_to_gold(silver_data, calculate_additional_metrics)
            
            # Save to persistent storage
            paths = self.config.file_paths
            saved_paths = self.gold_processor.save_gold_data(str(paths['gold_data']))
            
            # Optional: Calculate KPIs using SQL (Table-Based Approach)
            sql_kpis = {}
            if self.db_manager and self.db_manager.engine:
                try:
                    sql_kpis = self.db_manager.calculate_kpis_sql()
                    self.logger.info("KPIs calculated using SQL (Table-Based Approach)")
                except Exception as sql_error:
                    self.logger.warning(f"SQL KPI calculation failed but continuing: {sql_error}")
            
            self.pipeline_results['gold'] = {
                'data': gold_data,
                'saved_paths': saved_paths,
                'sql_kpis': sql_kpis,
                'timestamp': datetime.utcnow()
            }
            
            # Display results
            self.gold_processor.display_kpi_results()
            
            self.logger.info("Gold layer completed: All business KPIs calculated")
            return gold_data
            
        except Exception as e:
            self.logger.error(f"Gold layer failed: {e}")
            self.logger.error(traceback.format_exc())
            raise
            
    def run_table_based_approach(self):
        """
        Table-Based Approach: Using SQL database for KPI calculation
        """
        self.logger.info("="*70)
        self.logger.info("TABLE-BASED APPROACH: SQL Database Processing")
        self.logger.info("="*70)
        
        if not self.db_manager or not self.db_manager.engine:
            self.logger.warning("No database connection available for table-based approach")
            return
            
        try:
            # Calculate KPIs using SQL
            sql_kpis = self.db_manager.calculate_kpis_sql()
            
            # Display SQL results
            print("\n" + "="*70)
            print("SQL DATABASE KPI RESULTS")
            print("="*70)
            
            for kpi_name, kpi_data in sql_kpis.items():
                if not kpi_data.empty:
                    print(f"\n{kpi_name.upper().replace('_', ' ')} (SQL):")
                    print("-" * 50)
                    print(kpi_data.to_string(index=False))
                    
            self.logger.info("Table-based approach completed successfully")
            
        except Exception as e:
            self.logger.error(f"Table-based approach failed: {e}")
            
    def run_complete_pipeline(self, calculate_additional_metrics: bool = False):
        """
        Run complete Medallion Architecture pipeline
       """
        try:
            start_time = datetime.utcnow()
            self.logger.info("Starting Medallion Architecture Pipeline")
            
            # Execute all layers
            bronze_data = self.run_bronze_layer()
            silver_data = self.run_silver_layer(bronze_data)
            gold_data = self.run_gold_layer(silver_data, calculate_additional_metrics)
            
            # Run table-based approach if database available
            self.run_table_based_approach()
            
            # Calculate pipeline statistics
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            self.logger.info("="*70)
            self.logger.info("PIPELINE EXECUTION SUMMARY")
            self.logger.info("="*70)
            self.logger.info(f"Total duration: {duration:.2f} seconds")
            self.logger.info(f"Bronze records: {len(bronze_data['customers'])} customers, {len(bronze_data['orders'])} orders")
            self.logger.info(f"Silver records: {len(silver_data['customers'])} customers, {len(silver_data['orders'])} orders")
            self.logger.info(f"KPIs calculated: {len(gold_data['required_kpis']) - 1}")
            
            
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            self.logger.error(f"Full error traceback: {traceback.format_exc()}")
            print(f"\nPipeline execution failed: {e}")
            print(f"Check logs/akasaair_processing.log for detailed error information")
            sys.exit(1)

def main():
    """
    Main execution function
    """
    print("\n" + "="*70)
    print("MEDALLION ARCHITECTURE DATA PIPELINE")
    print("="*70)
    print("Implementing Bronze → Silver → Gold Layers")
    print("With Both In-Memory and Table-Based Approaches")
    print("="*70)
    
    try:
        # Initialize and run pipeline
        pipeline = MedallionArchitecturePipeline()
        
        # Run complete pipeline
        pipeline.run_complete_pipeline(calculate_additional_metrics=True)
        
    except KeyboardInterrupt:
        print("\nPipeline execution interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nFatal error: {e}")
        print(f"Detailed error information in logs/akasaair_processing.log")
        sys.exit(1)

if __name__ == "__main__":
    main()