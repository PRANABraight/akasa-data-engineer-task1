import pandas as pd
import logging
from datetime import datetime
from .data_cleaner import DataCleaner
from .data_validator import DataValidator
from .data_enricher import DataEnricher

logger = logging.getLogger(__name__)

class SilverProcessor:
    """
    Silver Layer: Main processor coordinating cleaning, validation, and enrichment
    """
    
    def __init__(self):
        self.cleaner = DataCleaner()
        self.validator = DataValidator()
        self.enricher = DataEnricher()
        self.silver_data = {}
        
    def process_to_silver(self, bronze_data: dict) -> dict:
        """
        Process bronze data through silver layer transformations
        """
        try:
            logger.info("Processing silver layer")
            start_time = datetime.utcnow()
            
            # Extract bronze data
            raw_customers = bronze_data['customers']
            raw_orders = bronze_data['orders']
            
            # Step 1: Data Cleaning
            cleaned_customers = self.cleaner.clean_customers_data(raw_customers)
            cleaned_orders = self.cleaner.clean_orders_data(raw_orders)
            
            # Step 2: Data Validation
            customer_validation = self.validator.validate_customers_data(cleaned_customers)
            order_validation = self.validator.validate_orders_data(cleaned_orders)
            
            # Step 3: Data Enrichment
            enriched_customers = self.enricher.enrich_customers_data(cleaned_customers)
            enriched_orders = self.enricher.enrich_orders_data(cleaned_orders, enriched_customers)
            
            # Compile silver data
            self.silver_data = {
                'customers': enriched_customers,
                'orders': enriched_orders,
                'metadata': {
                    'processing_timestamp': datetime.utcnow(),
                    'processing_duration_seconds': (datetime.utcnow() - start_time).total_seconds(),
                    'cleaning_stats': self.cleaner.get_cleaning_stats(),
                    'validation_summary': self.validator.get_validation_summary(),
                    'enrichment_stats': self.enricher.get_enrichment_stats()
                }
            }
            
            logger.info(f"Silver layer completed: {len(enriched_customers)} customers, {len(enriched_orders)} orders")
            return self.silver_data
            
        except Exception as e:
            logger.error(f"Silver layer processing failed: {str(e)}")
            raise
            
    def save_silver_data(self, output_dir: str = "data/silver") -> dict:
        """
        Save silver data to persistent storage
        """
        try:
            if not self.silver_data:
                raise ValueError("No silver data available. Run process_to_silver first.")
                
            import os
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            
            # Save customers data
            customers_path = f"{output_dir}/customers_silver_{timestamp}.parquet"
            self.silver_data['customers'].to_parquet(customers_path, index=False)
            
            # Save orders data
            orders_path = f"{output_dir}/orders_silver_{timestamp}.parquet"
            self.silver_data['orders'].to_parquet(orders_path, index=False)
            
            saved_paths = {
                'customers_silver_path': customers_path,
                'orders_silver_path': orders_path
            }
            
            logger.info(f"Silver data saved to {output_dir}")
            return saved_paths
            
        except Exception as e:
            logger.error(f"Failed to save silver data: {str(e)}")
            raise